<?php
  
  namespace filipekp\queue\db;
  
  use filipekp\queue\db\adaptors\IAdaptor;

  /**
   * Třída DB.
   *
   * @author    Pavel Filípek <pavel@filipek-czech.cz>
   * @copyright © 2019, Proclient s.r.o.
   * @created   03.04.2019
   */
  class Database
  {
    /** @var IAdaptor */
    private $adaptor;
    private $log;
    private $result;
    
    public static $timerSum = 0;
  
    /**
     * DB constructor.
     *
     * @param      $adaptor
     * @param      $hostname
     * @param      $username
     * @param      $password
     * @param      $database
     * @param null $port
     *
     * @throws DatabaseException
     */
    public function __construct($adaptor, $hostname, $username, $password, $database, $port = NULL) {
      $adaptor = ucfirst($adaptor);
      $adaptorClass = 'filipekp\queue\db\adaptors\\' . $adaptor;
      
      if (isset($_SESSION['_tracy']['sql_log'])) {
        unset($_SESSION['_tracy']['sql_log']);
      }
      $this->log                     = [];
      $this->log['page_url']         = (isset($_SERVER['QUERY_STRING']) && $_SERVER['QUERY_STRING']) ? $_SERVER['QUERY_STRING'] : 'index.php';
      $this->log['query_total_time'] = 0;
      
      if (class_exists($adaptorClass)) {
        $this->adaptor = new $adaptorClass($hostname, $username, $password, $database, $port);
      } else {
        throw new DatabaseException('Error: Could not load database adaptor `' . $adaptor . '`!');
      }
    }
  
    /**
     * Nastartuje transakci
     *
     * @param null $name
     *
     * @return bool
     * @throws DatabaseException
     */
    public function beginTransaction($name = NULL) {
      if (method_exists($this->adaptor, 'beginTransaction')) {
        return $this->adaptor->beginTransaction($name);
      } else {
        throw new DatabaseException('Method beginTransaction() not implemented yet in adaptor ' . get_class($this->adaptor));
      }
    }
  
    /**
     * Komitne transakci
     *
     * @param null $name
     *
     * @return bool
     * @throws DatabaseException
     */
    public function commit($name = NULL) {
      if (method_exists($this->adaptor, 'commit')) {
        return $this->adaptor->commit($name);
      } else {
        throw new DatabaseException('Method commit() not implemented yet in adaptor ' . get_class($this->adaptor));
      }
    }
  
    /**
     * Rollback transakce.
     *
     * @param null $name
     *
     * @return bool
     * @throws DatabaseException
     */
    public function rollback($name = NULL) {
      if (method_exists($this->adaptor, 'rollback')) {
        return $this->adaptor->rollback($name);
      } else {
        throw new DatabaseException('Method rollback() not implemented yet in adaptor ' . get_class($this->adaptor));
      }
    }
    
    public function query($sql, $params = []) {
      $this->log['query_total_time'] = 0;
      $trace                         = debug_backtrace();
      $filename                      = (isset($trace[0]['file'])) ? $trace[0]['file'] : '---';
//      $cmsPath                       = str_replace('upload/system/', '', DIR_SYSTEM);
//      $cmsPath                       = str_replace('public/system/', '', $cmsPath);
//      $pureFile                      = str_replace($cmsPath, '', $filename);
      $query_time                    = (microtime(TRUE));
      
      $this->adaptor->tryQuery = 10;
      $this->result            = $this->adaptor->query($sql, $params);
      
      $exec_time = (microtime(TRUE));
      $exec_time = round(($exec_time - $query_time) * 1000, 4);
      if (!isset($this->log['query_total_time'])) {
        $this->log['query_total_time'] = 0;
      }
      $this->log['query_total_time']   = (float)$this->log['query_total_time'] + (float)$exec_time;
//      $this->log['file']               = $pureFile;
      $this->log['time']               = $exec_time;
      $this->log['query']              = $sql;
      $_SESSION['_tracy']['sql_log'][] = $this->log;
      
      return $this->result;
    }
    
    /**
     * Vrati vysledek v asociativnim poli.
     *
     * @param string $columnName Název sloupce, který má být použitý jako klíč
     *
     * @return array
     */
    public function array_assoc($columnName) {
      $rows = $this->result->rows;
      
      return array_combine(array_map(function ($item) use ($columnName) {
        return $item[$columnName];
      }, $rows), $rows);
    }
  
    /**
     * @param $value
     *
     * @return string
     */
    public function escape($value) {
      return $this->adaptor->escape($value);
    }
  
    /**
     * @return int
     */
    public function countAffected() {
      return $this->adaptor->countAffected();
    }
  
    /**
     * @return mixed
     */
    public function getLastId() {
      return $this->adaptor->getLastId();
    }
  
    /**
     * @return bool
     */
    public function connected() {
      return $this->adaptor->isConnected();
    }
  
    /**
     * @return bool
     */
    public function connect() {
      return $this->adaptor->connect();
    }
  
    /**
     * @return bool
     */
    public function reconnect() {
      return $this->adaptor->reconnect();
    }
  
    /**
     * @return bool
     */
    public function closeConnection() {
      return $this->adaptor->closeConnection();
    }
  
    /**
     * @param null $name
     *
     * @return bool
     * @throws DatabaseException
     */
    public function inTransaction($name = NULL) {
      if (method_exists($this->adaptor, 'inTransaction')) {
        return $this->adaptor->inTransaction($name);
      } else {
        throw new DatabaseException('Method inTransaction() not implemented yet in adaptor ' . get_class($this->adaptor));
      }
    }
  }