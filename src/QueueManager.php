<?php
  
  namespace filipekp\queue;
  
  use filipekp\queue\db\Database;
  use PF\helpers\SqlFilter;
  use PF\helpers\SqlTable;
  
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
    
    /** @var QueueManager */
    protected static $instance = NULL;
    /** @var Queue */
    protected static $queue = NULL;
    /** @var QueueProcessorChecker */
    protected static $queueChecker = NULL;
    
    /**
     * QueueManager constructor.
     *
     * @param Database $database
     */
    private function __construct(Database $database) {
      self::$db = $database;
  
      date_default_timezone_set('Europe/Prague');
    }
    
    /**
     * Vypíše MSG na obrazovku.
     *
     * @param        $type
     * @param        $msg
     * @param string $worker
     */
    public static function printMsg($type, $msg, $worker = '') {
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
        ->compare($table->column('date_added'), '<', date('Y-m-d H:i:s', strtotime('-7 days')));
      
      self::$db->query("DELETE FROM {$table} WHERE {$filter}");
    }
  
    /**
     * Zapíše požadavek do fronty.
     *
     * @param       $url
     * @param array $data
     * @param int   $processor
     * @param null  $groupId
     * @param null  $parentGroupId
     *
     * @return int
     */
    public function writeRequest($url, $data = [], $processor = self::PROCESSOR_GENERAL, $groupId = NULL, $parentGroupId = NULL) {
      $url = urldecode(str_replace('&amp;', '&', $url));
      
      $table = SqlTable::create('queue');
      $sql   = "INSERT INTO {$table} (id, group_id, parent_group_id, queue_processor_id, url, `data`, state, message, date_added, date_start, date_end) VALUES
              (NULL, " . ((is_null($groupId)) ? 'NULL' : $groupId) . ", " . ((is_null($parentGroupId)) ? 'NULL' : $parentGroupId) . ", {$processor}, '{$url}', '" . json_encode($data) . "', 'new', NULL, NULL, NULL, NULL)";
  
      self::$db->query($sql);
      
      return self::$db->countAffected();
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
  
    public function install() {
      $table1 = SqlTable::create('queue');
      $createTable1 = "
        CREATE TABLE `{$table1->getFullName()}` (
          `id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
          `group_id` INT(11) UNSIGNED NULL DEFAULT NULL,
          `parent_group_id` INT(11) UNSIGNED NULL DEFAULT NULL,
          `queue_processor_id` INT(11) NOT NULL,
          `url` TEXT NOT NULL COLLATE 'utf8_czech_ci',
          `data` LONGTEXT NOT NULL COLLATE 'utf8_czech_ci',
          `state` ENUM('new','process','wait','error','done') NOT NULL DEFAULT 'new' COLLATE 'utf8_czech_ci',
          `message` TEXT NULL COLLATE 'utf8_czech_ci',
          `date_added` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          `date_start` DATETIME NULL DEFAULT NULL,
          `date_end` DATETIME NULL DEFAULT NULL,
          PRIMARY KEY (`id`),
          INDEX `FK_oc_queue_oc_queue_processor` (`queue_processor_id`),
          INDEX `group` (`group_id`),
          INDEX `FK_oc_queue_oc_queue` (`parent_group_id`),
          CONSTRAINT `FK_oc_queue_oc_queue` FOREIGN KEY (`parent_group_id`) REFERENCES `oc_queue` (`group_id`),
          CONSTRAINT `FK_oc_queue_oc_queue_processor` FOREIGN KEY (`queue_processor_id`) REFERENCES `oc_queue_processor` (`id`)
        )
        COLLATE='utf8_czech_ci'
        ENGINE=InnoDB
        AUTO_INCREMENT=1;
      ";
      
      $table2 = SqlTable::create('queue_processor');
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
      
      $t1R = self::$db->query($createTable1);
      self::printMsg((($t1R) ? 'OK' : 'ERROR'), "Table `{$table1->getFullName()}` is" . (($t1R) ? '' : ' not') . " created.");
      $t2R = self::$db->query($createTable2);
      self::printMsg((($t2R) ? 'OK' : 'ERROR'), "Table `{$table2->getFullName()}` is" . (($t2R) ? '' : ' not') . " created.");
    }
  }