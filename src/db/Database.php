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
     * @throws \Exception
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
        throw new \Exception('Error: Could not load database adaptor `' . $adaptor . '`!');
      }
    }
  
    /**
     * Nastartuje transakci
     *
     * @throws \Exception
     */
    public function beginTransaction() {
      if (method_exists($this->adaptor, 'beginTransaction')) {
        $this->adaptor->beginTransaction();
      } else {
        throw new \Exception('Method beginTransaction() not implemented yet in adaptor ' . get_class($this->adaptor));
      }
    }
  
    /**
     * Komitne transakci
     *
     * @return bool
     * @throws \Exception
     */
    public function commit() {
      if (method_exists($this->adaptor, 'commit')) {
        return $this->adaptor->commit();
      } else {
        throw new \Exception('Method commit() not implemented yet in adaptor ' . get_class($this->adaptor));
      }
    }
  
    /**
     * Rollback transakce.
     *
     * @throws \Exception
     */
    public function rollback() {
      if (method_exists($this->adaptor, 'rollback')) {
        $this->adaptor->rollback();
      } else {
        throw new \Exception('Method rollback() not implemented yet in adaptor ' . get_class($this->adaptor));
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
  
    public function reconnect() {
      $this->adaptor->reconnect();
    }
  
    /**
     * @return bool
     * @throws \Exception
     */
    public function inTransaction() {
      if (method_exists($this->adaptor, 'inTransaction')) {
        return $this->adaptor->inTransaction();
      } else {
        throw new \Exception('Method inTransaction() not implemented yet in adaptor ' . get_class($this->adaptor));
      }
    }
  }