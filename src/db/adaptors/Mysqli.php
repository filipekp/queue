<?php
  
  namespace filipekp\queue\db\adaptors;

  use filipekp\queue\db\DatabaseException;

  /**
   * Třída MySQLi.
   *
   * @author    Pavel Filípek <pavel@filipek-czech.cz>
   * @copyright © 2019, Proclient s.r.o.
   * @created   03.04.2019
   */
  final class Mysqli implements IAdaptor
  {
    private $hostname = NULL;
    private $username = NULL;
    private $password = NULL;
    private $database = NULL;
    private $port     = '3306';
    
    private $connection;
    private $isMultiQuery  = FALSE;
    private $countAffected = 0;
    private $inTransaction = FALSE;
    
    public         $tryQuery          = 10;
    private static $errors            = [1213, 1205];
  
    /**
     * Mysqli constructor.
     *
     * @param        $hostname
     * @param        $username
     * @param        $password
     * @param        $database
     * @param string $port
     *
     * @throws \Exception
     */
    public function __construct($hostname, $username, $password, $database, $port = '3306') {
      $this->hostname = $hostname;
      $this->username = $username;
      $this->password = $password;
      $this->database = $database;
      $this->port     = $port;
      
      $this->connection = new \mysqli();
      $this->connect();
    }
    
    /**
     * @param $sql
     *
     * @return array|bool|\mysqli_result|\stdClass
     * @throws \ErrorException
     */
    private function queryExec($sql) {
      $this->connection->multi_query($sql);
      
      if (!$this->connection->errno) {
        $this->isMultiQuery = $this->connection->more_results();
        $data               = [];
        
        if ($this->isMultiQuery) {
          do {
            if (($result = $this->connection->store_result()) instanceof \mysqli_result) {
              while ($rowArray = $result->fetch_assoc()) {
                $dataResult[] = $rowArray;
              }
              
              $resultClass           = new \stdClass();
              $resultClass->num_rows = $result->num_rows;
              $resultClass->row      = isset($dataResult[0]) ? $dataResult[0] : [];
              $resultClass->rows     = $dataResult;
              
              $data[] = $resultClass;
              
              $result->close();
            } else {
              $this->countAffected += $this->connection->affected_rows;
              $data[]              = TRUE;
            }
            
          } while ($this->connection->more_results() && $this->connection->next_result());
          
          return $data;
        } else {
          $query = $this->connection->store_result();
          
          if ($query instanceof \mysqli_result) {
            $data = [];
            
            while ($row = $query->fetch_assoc()) {
              $data[] = $row;
            }
            
            $result           = new \stdClass();
            $result->num_rows = $query->num_rows;
            $result->row      = isset($data[0]) ? $data[0] : [];
            $result->rows     = $data;
            
            $query->close();
            
            return $result;
          } else {
            $this->countAffected = $this->connection->affected_rows;
            
            return TRUE;
          }
        }
      } else {
        throw new DatabaseException($this->connection->error, $this->connection->errno);
      }
    }
  
    /**
     * @param      $sql
     * @param null $currentTry
     *
     * @return array|bool|mixed|\mysqli_result|\stdClass
     * @throws \Exception
     */
    public function query($sql, &$currentTry = NULL) {
      $this->countAffected = 0;
      $currentTry          = ((is_null($currentTry)) ? $this->tryQuery : $currentTry);
      
      try {
        try {
          $r = $this->queryExec($sql);
        } catch (\ErrorException $e) {
          if (in_array($e->getCode(), self::$errors) && $currentTry > 0) {
            $currentTry--;
            usleep(100000);
            $r = $this->query($sql, $currentTry);
          } else {
            throw new \Exception('Error: ' . $this->connection->error . '<br />Error No: ' . $this->connection->errno . '<br />' . $sql, $this->connection->errno);
          }
        }
      } catch (\Exception $exp) {
        if ($this->inTransaction) {
          $this->rollback();
        }
        
        throw new DatabaseException($exp->getMessage(), $exp->getCode());
      }
      
      return $r;
    }
    
    public function beginTransaction() {
      if (!$this->inTransaction) {
        $this->connection->autocommit($this->inTransaction);
        $this->connection->begin_transaction();
        
        $this->inTransaction = TRUE;
      }
    }
  
    /**
     * @return bool
     */
    public function commit() {
      $return = FALSE;
      
      if ($this->inTransaction) {
        $return = $this->connection->commit();
        $this->connection->autocommit($this->inTransaction);
        
        $this->inTransaction = FALSE;
      }
      
      return $return;
    }
  
    /**
     * @return bool|void
     */
    public function rollback() {
      if ($this->inTransaction) {
        $this->connection->rollback();
        $this->connection->autocommit($this->inTransaction);
        
        $this->inTransaction = FALSE;
      }
    }
  
    /**
     * @param string $value
     *
     * @return string
     */
    public function escape($value) {
      return $this->connection->real_escape_string($value);
    }
  
    /**
     * @return int
     */
    public function countAffected() {
      return $this->countAffected;
    }
  
    /**
     * @return mixed
     */
    public function getLastId() {
      return $this->connection->insert_id;
    }
  
    /**
     * @return bool
     */
    public function isConnected() {
      return $this->connection->ping();
    }
  
    /**
     * @return mixed|void
     * @throws DatabaseException
     */
    public function reconnect() {
      $this->closeConnection();
      return $this->connect();
    }
    
    /**
     * @return bool
     */
    public function inTransaction() {
      return $this->inTransaction;
    }
  
    /**
     * @return mixed|void
     * @throws DatabaseException
     */
    public function connect() {
      $this->connection->connect($this->hostname, $this->username, $this->password, $this->database, $this->port);
      
      if ($this->connection->connect_error) {
        throw new DatabaseException($this->connection->connect_error, $this->connection->connect_errno);
      }
  
      $this->connection->set_charset("utf8");
      $this->connection->query("SET SQL_MODE = ''");
      
      return TRUE;
    }
  
    /**
     * Close current connection.
     *
     * @return bool
     */
    public function closeConnection() {
      return $this->connection->close();
    }
  }