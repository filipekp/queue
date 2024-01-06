<?php
  
  namespace filipekp\queue\db\adaptors;

  use filipekp\queue\db\DatabaseException;
  use PF\helpers\MyArray;

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
  
    private static $transactions = [];
    
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
      $this->connection->options(MYSQLI_OPT_CONNECT_TIMEOUT, 65);
      $this->connect();
    }
  
    /**
     * @param $sql
     *
     * @return array|bool|\mysqli_result|\stdClass
     * @throws DatabaseException|\ErrorException
     */
//    private function queryExec($sql) {
//      $this->connection->multi_query($sql);
//
//      if (!$this->connection->errno) {
//        $this->isMultiQuery = $this->connection->more_results();
//        $data               = [];
//
//        if ($this->isMultiQuery) {
//          do {
//            if (($result = $this->connection->store_result()) instanceof \mysqli_result) {
//              while ($rowArray = $result->fetch_assoc()) {
//                $dataResult[] = $rowArray;
//              }
//
//              $resultClass           = new \stdClass();
//              $resultClass->num_rows = $result->num_rows;
//              $resultClass->row      = isset($dataResult[0]) ? $dataResult[0] : [];
//              $resultClass->rows     = $dataResult;
//
//              $data[] = $resultClass;
//
//              $result->close();
//            } else {
//              $this->countAffected += $this->connection->affected_rows;
//              $data[]              = TRUE;
//            }
//
//          } while ($this->connection->more_results() && $this->connection->next_result());
//
//          return $data;
//        } else {
//          $query = $this->connection->store_result();
//
//          if ($query instanceof \mysqli_result) {
//            $data = [];
//
//            while ($row = $query->fetch_assoc()) {
//              $data[] = $row;
//            }
//
//            $result           = new \stdClass();
//            $result->num_rows = $query->num_rows;
//            $result->row      = isset($data[0]) ? $data[0] : [];
//            $result->rows     = $data;
//
//            $query->close();
//
//            return $result;
//          } else {
//            $this->countAffected = $this->connection->affected_rows;
//
//            return TRUE;
//          }
//        }
//      } else {
//        throw new DatabaseException($this->connection->error, $this->connection->errno);
//      }
//    }
    
    
    
    /**
     * @param $sql
     *
     * @return array|bool|\mysqli_result|\stdClass
     * @throws \ErrorException
     */
    private function queryExec($sql) {
      $query = $this->connection->query($sql);
      
      if (!$this->connection->errno) {
        if ($query instanceof \mysqli_result) {
          $data = [];
          
          while ($row = $query->fetch_assoc()) {
            $data[] = $row;
          }
          
          $result           = new \stdClass();
          $result->num_rows = $query->num_rows;
          $result->row      = isset($data[0]) ? $data[0] : [];
          $result->rows     = $data;
          
          $query->free_result();
          
          return $result;
        } else {
          $this->countAffected = $this->connection->affected_rows;
          
          return TRUE;
        }
      } else {
        throw new \ErrorException($this->connection->error, $this->connection->errno);
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
      $currentTry = $this->tryQuery;
      $this->currentTry = 1;
      $result = FALSE;
    
      try {
        $result = $this->queryExec($sql);
      } catch (\ErrorException $e) {
        if (in_array($e->getCode(), self::$errors) && $currentTry > 0) {
          while ($currentTry && !($result = $this->queryExec($sql))) {
            $currentTry--;
            $this->currentTry++;
            sleep(1);
          }
          if (!$currentTry) {
            throw $e;
          }
        } else {
          throw new DatabaseException('⚠ Error: ' . $this->connection->error . ' => ' . $sql, (int)$this->connection->errno);
        }
      }    
      return $result;
    }
  
    /**
     * @param null $name
     *
     * @return bool
     */
    public function beginTransaction($name = NULL) {
      $name = $this->getNameOfTransaction($name);
  
      if ($this->inTransaction($name)) {
        throw new \LogicException(
          '⚠ Couldn\'t call `' . __FUNCTION__ . '()`. Instance of `' . __CLASS__ . '` in transaction `' . $name . '`.',
          10100
        );
      }
  
      if (count(self::$transactions) == 0) {
        $return = $this->connection->begin_transaction(0, $name);
      } else {
        $return = $this->connection->savepoint($name);
      }
  
      $this->setInTransaction($name, TRUE);
  
      return $return;
    }
  
    /**
     * @param null $name
     *
     * @return bool
     */
    public function commit($name = NULL) {
      $name = $this->getNameOfTransaction($name);
  
      if (!$this->inTransaction($name)) {
        throw new \LogicException(
          '⚠ Couldn\'t call `' . __FUNCTION__ . '()`. Instance of `' . __CLASS__ . '` isn\'t in transaction `' . $name . '`.',
          10110
        );
      }
  
      $this->setInTransaction($name, FALSE);
      if (count(self::$transactions) == 0) {
        $return = $this->connection->commit();
      } else {
        $return = TRUE;
      }
  
      return $return;
    }
  
    /**
     * @param null $name
     *
     * @return bool
     */
    public function rollback($name = NULL) {
      $name = $this->getNameOfTransaction($name);
  
      if (!$this->inTransaction($name)) {
        throw new \LogicException(

          '⚠ Couldn\'t call `' . __FUNCTION__ . '()`. Instance of `' . __CLASS__ . '` isn\'t in transaction `' . $name . '`.',
          10101
        );
      }
  
      $return = $this->connection->rollback(0, $name);
      $this->setInTransaction($name, FALSE);
  
      return $return;
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
      return @$this->connection->ping();
    }
  
    /**
     * @return mixed|void
     * @throws DatabaseException
     */
    public function reconnect() {
      $this->closeConnection();
      return $this->connect();
    }
    
    private function getNameOfTransaction($name = NULL) {
      return ((is_null($name)) ? 'NA___NULL' : $name);
    }
  
    /**
     * @param null $name
     * @param bool $state
     *
     * @return string|null
     */
    private function setInTransaction($name = NULL, $state = TRUE) {
      $name = $this->getNameOfTransaction($name);
    
      if ($state) {
        self::$transactions[$name] = TRUE;
      } else {
        $transactionsArr = MyArray::init(self::$transactions);
        $transactionsArr->unsetItem([$name]);
      }
    
      $this->connection->autocommit(count(self::$transactions) < 1);
    
      return $name;
    }
  
    /**
     * @param null $name
     *
     * @return bool
     */
    public function inTransaction($name = NULL) {
      $name = $this->getNameOfTransaction($name);
    
      return (bool)MyArray::init(self::$transactions)->item([$name], FALSE);
    }
  
    /**
     * @return mixed|void
     * @throws DatabaseException
     */
    public function connect() {
      if (!$this->isConnected()) {
        $this->connection->connect($this->hostname, $this->username, $this->password, $this->database, $this->port);
        
        if ($this->connection->connect_error) {
          throw new DatabaseException($this->connection->connect_error, $this->connection->connect_errno);
        }
    
        $this->connection->set_charset("utf8");
        $this->connection->query("SET SQL_MODE = ''");
      }
      
      return TRUE;
    }
  
    /**
     * Close current connection.
     *
     * @return bool
     */
    public function closeConnection() {
      if ($this->isConnected()) {
        $thread = $this->connection->thread_id;
        @$this->connection->kill($thread);
        @$this->connection->close();
        
        return TRUE;
      }
    }
  }