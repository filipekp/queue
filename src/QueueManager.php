<?php
  
  namespace filipekp\queue;
  
  use filipekp\queue\db\Database;
  use PF\helpers\MyArray;
  use PF\helpers\MyString;
  use PF\helpers\SqlFilter;
  use PF\helpers\SqlTable;
  use PF\helpers\Verifier;

  /**
   * Class QueueProcessor checks db, call workers and process all stuff
   *
   * @author    Pavel Filípek <pavel@filipek-czech.cz>
   * @copyright © 2019, Proclient s.r.o.
   * @created   28.03.2019
   */
  class QueueManager
  {
    const PROCESSOR_GENERAL = 1;
    
    /** @var Database */
    protected static $db;
    
    protected $dbPrefix = '';
    protected $intervalToDelete = '7 days';
    
    /** @var QueueManager */
    protected static $instance = NULL;
    /** @var Queue */
    protected static $queue = NULL;
    /** @var QueueProcessorChecker */
    protected static $queueChecker = NULL;
    
    protected static $isSetTimeZone = FALSE;
    
    
    /**
     * QueueManager constructor.
     *
     * @param Database $database
     */
    private function __construct(Database $database) {
      self::$db = $database;
      self::setTimeZone();
    }
  
    /**
     * Nastaví výchozí timezonu.
     *
     * @param string $timeZone
     */
    public static function setTimeZone($timeZone = 'Europe/Prague') {
      if (!self::$isSetTimeZone) {
        date_default_timezone_set($timeZone);
        self::$isSetTimeZone = TRUE;
      }
    }
    
    /**
     * Vypíše MSG na obrazovku.
     *
     * @param        $type
     * @param        $msg
     * @param string $worker
     */
    public static function printMsg($type, $msg, $worker = '') {
      self::setTimeZone();
      
      $status = date("Y-m-d H:i:s") . ' [' . $type . '] ';
      
      if ($worker != '') {
        $status .= $worker . ': ';
      }
      $status .= $msg . PHP_EOL;
      
      print $status;
    }
    
    /**
     * Nastaví prefix tabulek v DB.
     *
     * @param $dbPrefix
     *
     * @return $this
     */
    public function setDbPrefix($dbPrefix) {
      $this->dbPrefix = $dbPrefix;
      SqlTable::setPrefix($this->dbPrefix);
      
      return $this;
    }
  
  
    /**
     * @param int $processor
     *
     * @return Queue
     * @throws \Exception
     */
    public static function getQueueInstance($processor) {
      if (!self::$db) { throw new \Exception('Database not initialized.'); }
      
      return new Queue(self::$db, $processor);
    }
  
    /**
     * @return QueueProcessorChecker
     * @throws \Exception
     */
    public static function getQueueCheckerInstance() {
      if (!self::$db) { throw new \Exception('Database not initialized.'); }
      
      if (is_null(self::$queueChecker)) {
        self::$queueChecker = new QueueProcessorChecker(self::$db);
      }
      
      return self::$queueChecker;
    }
  
    /**
     * Promaže starou frontu
     */
    public function deleteOldQueueItems() {
      $table = SqlTable::create('queue');
      $filter = SqlFilter::create()
        ->compare('date_added', '<', date('Y-m-d H:i:s', strtotime('-' . $this->intervalToDelete)));
      
      self::$db->query("DELETE FROM {$table} WHERE {$filter}");
    }
  
    /**
     * Zapíše požadavek do fronty.
     *
     * @param        $url
     * @param null   $webhookUrl
     * @param array  $data
     * @param int    $processor
     * @param null   $groupId
     * @param null   $parentGroupId
     * @param string $type     *
     * @param int    $countTry
     * @param int    $delay
     *
     * @return int last inserted ID
     */
    public function writeRequest($url, $webhookUrl = NULL, $data = [], $processor = self::PROCESSOR_GENERAL, $groupId = NULL, $parentGroupId = NULL, $type = Queue::TYPE_SYNC, $countTry = 1, $delay = 0) {
      $url = urldecode(str_replace('&amp;', '&', $url));
      
      if (!in_array($type, [Queue::TYPE_SYNC, Queue::TYPE_ASYNC])) {
        throw new \InvalidArgumentException(vsprintf('Bad type: `%s` for queue.', [$type]), 500);
      }
      
      $table = SqlTable::create('queue');
      $sql   = "INSERT INTO {$table} (id, group_id, parent_group_id, queue_processor_id, `process_type`, webhook_url, url, `data`, state, retry, delay) VALUES
              (NULL, " . ((is_null($groupId)) ? 'NULL' : $groupId) . ", " . ((is_null($parentGroupId)) ? 'NULL' : $parentGroupId) . ",
              {$processor}, '{$type}', " . ((is_null($webhookUrl)) ? 'NULL' : "'" . self::$db->escape($webhookUrl) . "'") . ", '" . self::$db->escape($url) . "',
              '" . self::$db->escape(json_encode($data, JSON_UNESCAPED_UNICODE)) . "', '" . Queue::STATE_NEW . "', {$countTry},
              {$delay})";
  
      self::$db->query($sql);
      
      return self::$db->getLastId();
    }
  
    /**
     * Update state from webHook async call.
     *
     * @param $hashIdentification
     * @param $responseData
     *
     * @return int
     * @throws \Exception
     */
    public function webHookAsyncResult($hashIdentification, $responseData) {
      $decodedArray = json_decode(Verifier::decode($hashIdentification), TRUE);
      $decodedArr = MyArray::init($decodedArray);
      
      if (!($queueId = $decodedArr->item('queue_id'))) {
        throw new \InvalidArgumentException(vsprintf('Hashcode `%s` has bad form.', [$hashIdentification]));
      }
      
      $dataArr = MyArray::init($responseData);
      
      $exceptionMsg = 'Response has not require attribute `%s`.';
  
      $http_code = NULL;
      $result = NULL;
      $datetime = NULL;
      
      foreach (['http_code', 'result', 'datetime'] as $param) {
        if (is_null((${$param} = $dataArr->item($param)))) {
          throw new \InvalidArgumentException(vsprintf($exceptionMsg, [$param]));
        }
      }
      
      $validDatetimeFormat = 'Y-m-d\TH:i:s.uP';
      if ($datetime && !($dateTimeObj = \DateTime::createFromFormat($validDatetimeFormat, $datetime))) {
        throw new \InvalidArgumentException(vsprintf('Attribute datetime: `%s` has not valid format. Valid format is `%s`.', [$datetime, $validDatetimeFormat]));
      }
      $datetime = $dateTimeObj;
  
  
      $table = SqlTable::create('queue', 'q');
  
      if (is_null($datetime)) { $datetime = new \DateTime('now'); }
      $datetime->setTimezone((new \DateTimeZone('Europe/Prague')));
  
      switch ($http_code) {
        case $http_code >= 500:
          $state = Queue::STATE_ERROR;
          break;
        default:
          $state = Queue::STATE_DONE;
      }
  
      $filterCurrentItem = SqlFilter::create()
        ->compare('id', '=', (string)$queueId);
      self::$db->query("
        UPDATE {$table->getFullName()}
          SET state='" . $state . "', state_code='" . $http_code . "',
          message='" . self::$db->escape(((is_array($result)) ? json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) : (string)$result)) . "',
          date_end='" . $datetime->format('Y-m-d H:i:s') . "'
        WHERE {$filterCurrentItem};
      ");
      
      self::$db->query("
        INSERT INTO queue_response
          (queue_id, code, response_data, datetime)
        VALUES ({$queueId}, {$http_code}, '" . self::$db->escape(((is_array($result)) ? json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) : (string)$result)) . "');
      ");
      
      return self::$db->countAffected();
    }
  
    /**
     * @param $id
     *
     * @return string
     */
    public static function getWebhookHash($id) {
      return Verifier::encode(json_encode([
        'queue_id' => $id
      ]));
    }
  
    /**
     * Vrátí maximální číslo skupiny.
     *
     * @return int
     */
    public function getNextGroupId() {
      $table = SqlTable::create('queue');
      
      return (self::$db->query("SELECT MAX(group_id) as 'max' FROM {$table}")->row['max'] + 2);
    }
    
    /**
     * @param        $hostname
     * @param        $username
     * @param        $password
     * @param        $database
     * @param null   $port
     * @param string $adaptor
     *
     * @return QueueManager
     * @throws \Exception
     */
    public static function getInstance($hostname, $username, $password, $database, $port = NULL, $adaptor = 'mysqli') {
      if (is_null(self::$instance)) {
        self::$instance = new self((new Database($adaptor, $hostname, $username, $password, $database, $port)));
      }
      
      return self::$instance;
    }
  
    /**
     * Instalace tabulek
     */
    public function install() {
      $table1 = SqlTable::create('queue');
      $table2 = SqlTable::create('queue_processor');
      
      $createTable1 = "
        CREATE TABLE `{$table1->getFullName()}` (
          `id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
          `group_id` INT(11) UNSIGNED NULL DEFAULT NULL,
          `parent_group_id` INT(11) UNSIGNED NULL DEFAULT NULL,
          `queue_processor_id` INT(11) NOT NULL,
	        `process_type` ENUM('sync','async') NOT NULL DEFAULT 'sync' COLLATE 'utf8_czech_ci',
	        `webhook_url` TEXT NULL COLLATE 'utf8_czech_ci',
          `url` TEXT NOT NULL COLLATE 'utf8_czech_ci',
          `data` LONGTEXT NOT NULL COLLATE 'utf8_czech_ci',
          `state` ENUM('new','process','wait','error','done') NOT NULL DEFAULT 'new' COLLATE 'utf8_czech_ci',
          `state_code` INT(6) NULL DEFAULT NULL,
          `processing_PID` VARCHAR(32) NULL DEFAULT NULL COLLATE 'utf8_czech_ci',
          `message` TEXT NULL DEFAULT NULL COLLATE 'utf8_czech_ci',
          `delay` INT(11) NOT NULL DEFAULT '0',
          `retry_counter` INT(11) NOT NULL DEFAULT '3',
          `retry` INT(11) NOT NULL DEFAULT '3',
          `date_added` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          `date_start` DATETIME NULL DEFAULT NULL,
          `date_end` DATETIME NULL DEFAULT NULL,
          PRIMARY KEY (`id`),
          INDEX `FK_{$table1->getFullName()}_{$table2->getFullName()}` (`queue_processor_id`),
          INDEX `group` (`group_id`),
          INDEX `FK_{$table1->getFullName()}_{$table1->getFullName()}` (`parent_group_id`),
          CONSTRAINT `FK_{$table1->getFullName()}_{$table1->getFullName()}` FOREIGN KEY (`parent_group_id`) REFERENCES `{$table1->getFullName()}` (`group_id`) ON DELETE CASCADE,
          CONSTRAINT `FK_{$table1->getFullName()}_{$table2->getFullName()}` FOREIGN KEY (`queue_processor_id`) REFERENCES `{$table2->getFullName()}` (`id`)
        )
        COLLATE='utf8_czech_ci'
        ENGINE=InnoDB
        AUTO_INCREMENT=1;
      ";
      
      $createTable2 = "
        CREATE TABLE `{$table2->getFullName()}` (
          `id` INT(11) NOT NULL AUTO_INCREMENT,
          `name` VARCHAR(32) NOT NULL COLLATE 'utf8_czech_ci',
          `state` ENUM('up','down') NOT NULL DEFAULT 'down' COLLATE 'utf8_czech_ci',
          `updated` DATETIME NOT NULL,
          PRIMARY KEY (`id`)
        )
        COLLATE='utf8_czech_ci'
        ENGINE=InnoDB
        AUTO_INCREMENT=1;
      ";
      
      // request table
      $table3 = SqlTable::create('queue_request');
      $createTable3 = "
        CREATE TABLE `{$table3->getFullName()}` (
          `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
          `queue_id` INT(10) UNSIGNED NOT NULL,
          `endpoint` VARCHAR(512) NOT NULL COLLATE 'utf8_czech_ci',
          `datetime` TIMESTAMP NOT NULL DEFAULT current_timestamp(),
          PRIMARY KEY (`id`),
          INDEX `FK_{$table3->getFullName()}2{$table1->getFullName()}` (`queue_id`),
          CONSTRAINT `FK_{$table3->getFullName()}2{$table1->getFullName()}` FOREIGN KEY (`queue_id`) REFERENCES `{$table1->getFullName()}` (`id`) ON UPDATE CASCADE ON DELETE CASCADE
        )
        COLLATE='utf8_czech_ci'
        ENGINE=InnoDB;
      ";
      
      // response table
      $table4 = SqlTable::create('queue_response');
      $createTable4 = "
        CREATE TABLE `{$table4->getFullName()}` (
          `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
          `queue_id` INT(10) UNSIGNED NOT NULL,
          `code` INT(11) NOT NULL,
          `response_data` LONGTEXT NOT NULL DEFAULT '' COLLATE 'utf8_czech_ci',
          `datetime` TIMESTAMP NOT NULL DEFAULT current_timestamp(),
          PRIMARY KEY (`id`),
          INDEX `FK_{$table4->getFullName()}{$table1->getFullName()}` (`queue_id`),
          CONSTRAINT `fk_{$table4->getFullName()}2{$table1->getFullName()}` FOREIGN KEY (`queue_id`) REFERENCES `{$table1->getFullName()}` (`id`) ON UPDATE CASCADE ON DELETE CASCADE
        )
        COLLATE='utf8_czech_ci'
        ENGINE=InnoDB
        ROW_FORMAT=DYNAMIC;
      ";
      
      
      
      $t2R = self::$db->query($createTable2);
      self::printMsg((($t2R) ? 'OK' : 'ERROR'), "Table `{$table2->getFullName()}` is" . (($t2R) ? '' : ' not') . " created.");
      $t1R = self::$db->query($createTable1);
      self::printMsg((($t1R) ? 'OK' : 'ERROR'), "Table `{$table1->getFullName()}` is" . (($t1R) ? '' : ' not') . " created.");
      $t2R = self::$db->query($createTable3);
      self::printMsg((($t2R) ? 'OK' : 'ERROR'), "Table `{$table3->getFullName()}` is" . (($t2R) ? '' : ' not') . " created.");
      $t2R = self::$db->query($createTable2);
      self::printMsg((($t2R) ? 'OK' : 'ERROR'), "Table `{$table4->getFullName()}` is" . (($t2R) ? '' : ' not') . " created.");
    }
    
    public function changeLogDB() {
      // ver. 2.0
      "ALTER TABLE `queue`
          ADD COLUMN `process_type` ENUM('sync','async') NOT NULL DEFAULT 'sync' AFTER `queue_processor_id`,
          ADD COLUMN `webhook_url` TEXT NULL AFTER `process_type`,
          ADD COLUMN `delay` INT(11) NOT NULL DEFAULT '0' AFTER `message`,
          ADD COLUMN `retry_counter` INT(11) NOT NULL DEFAULT '0' AFTER `delay`,
          ADD COLUMN `retry` INT(11) NOT NULL DEFAULT '0' AFTER `retry_counter`,
          CHANGE COLUMN `processing_PID` `processing_pid` VARCHAR(32) NULL DEFAULT NULL COLLATE 'utf8_czech_ci' AFTER `state_code`;";
    }
  
    /**
     * @param string $intervalToDelete by datetime specifications
     */
    public function setIntervalToDelete(string $intervalToDelete = '7 days') {
      $this->intervalToDelete = $intervalToDelete;
    }
  }