<?php
  
  namespace filipekp\queue\db\adaptors;
  
  /**
   * Rozhraní IAdaptor.
   *
   * @author    Pavel Filípek <pavel@filipek-czech.cz>
   * @copyright © 2019, Proclient s.r.o.
   * @created   03.04.2019
   */
  interface IAdaptor
  {
    /**
     * IAdaptor constructor.
     *
     * @param string $hostname
     * @param string $username
     * @param string $password
     * @param string $database
     * @param string $port
     */
    public function __construct($hostname, $username, $password, $database, $port = '3306');
  
    /**
     * Execute query.
     *
     * @param      $sql
     * @param null $currentTry
     *
     * @return mixed
     */
    public function query($sql, &$currentTry = NULL);
  
    /**
     * Begin transaction.
     *
     * @param null $name
     *
     * @return bool
     */
    public function beginTransaction($name = NULL);
  
    /**
     * Commit transaction.
     * @param null $name
     *
     * @return bool
     */
    public function commit($name = NULL);
  
    /**
     * Rollback transaction.
     *
     * @param null $name
     *
     * @return bool
     */
    public function rollback($name = NULL);
  
    /**
     * Escape string.
     *
     * @param string $value
     * @return string
     */
    public function escape($value);
  
    /**
     * Count affected rows.
     * @return int
     */
    public function countAffected();
  
    /**
     * Last inserted ID.
     * @return mixed
     */
    public function getLastId();
  
    /**
     * Return is connected.
     * @return bool
     */
    public function isConnected();
  
    /**
     * @param null $name
     *
     * @return bool
     */
    public function inTransaction($name = NULL);
  
    /**
     * @return bool
     */
    public function connect();
  
    /**
     * @return bool
     */
    public function reconnect();
    
    public function closeConnection();
  }