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
  }