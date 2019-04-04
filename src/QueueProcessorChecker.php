<?php
  namespace filipekp\queue;
  
  use filipekp\queue\db\Database;
  use PF\helpers\mail\Sender;
  use PF\helpers\SqlFilter;
  use PF\helpers\SqlTable;
  
  /**
   * Třída QueueChecker.
   *
   * @author    Pavel Filípek <pavel@filipek-czech.cz>
   * @copyright © 2019, Proclient s.r.o.
   * @created   28.03.2019
   */
  class QueueProcessorChecker {
    /** @var Database|null */
    protected static $db = NULL;
    
    protected $notifyToMail = FALSE;
    protected $serverName = NULL;
    protected $from = NULL;
    protected $to = NULL;
    
    public function __construct(Database $database, $notifyToMail = FALSE, $to = NULL, $serverName = NULL, $from = NULL) {
      self::$db = $database;
  
      $this->notifyToMail = $notifyToMail;
      if (is_null($to)) {
        $this->notifyToMail = FALSE;
      }
      $this->to           = $to;
      $this->serverName   = ((is_null($serverName)) ? $_SERVER['SERVER_NAME'] : $serverName);
      $this->from         = ((is_null($from)) ? 'queuechecker@' . $_SERVER['SERVER_NAME'] : $from);
    }
  
    /**
     * Otestuje frontu zda běží.
     */
    public function testQueueProcessor() {
      $table = SqlTable::create('queue_processor');
      $result = self::$db->query("SELECT * FROM {$table}");
  
      if ($result->num_rows > 0) {
        foreach ($result->rows as $queueProcessor) {
          $currentState = $queueProcessor['state'];
          if (exec('ps a | grep php\ queue.php\ ' . $queueProcessor['id'] . '$ | wc -l') == 0) {
            $newState = 'down';
          } else {
            $newState = 'up';
          }
  
          self::$db->query("UPDATE {$table->getFullName()} SET state = '" . $newState . "', updated = '" . date('Y-m-d H:i:s') . "' WHERE id = " . $queueProcessor['id']);
          if ($this->notifyToMail && $currentState != $newState) {
            $sender = new Sender();
            $sender->sendMail(
              $this->serverName . ' - Fronta `' . $queueProcessor['name'] . '` [' . date('d.m.Y H:i:s') . '] - ' . $newState,
              'Zpracování fronty procesoru `' . $queueProcessor['name'] . '` ve screenu bylo ' . (($newState == 'down') ? 'zastaveno' : 'obnoveno') . '.',
              [$this->from],
              [$this->to],
              [],
              [],
              Sender::MAIL_TYPE_HTML
            );
          }
        }
      }
    }
  
    /**
     * Overeni zda procesor existuje.
     *
     * @param $processor
     */
    public static function processorExists($processor) {
      $table = SqlTable::create('queue_processor', 'qp');
      $filter = SqlFilter::create()
        ->compare($table->column('id'), '=', $processor);
      
      $result = self::$db->query("SELECT * FROM {$table} WHERE {$filter} LIMIT 0,1");
    
      if ($result->num_rows == 0) {
        QueueManager::printMsg('ERROR', 'Queue processor ' . $processor . ' not exists.');
        exit(-1);
      }
    }
  }