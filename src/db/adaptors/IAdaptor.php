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
     */
    public function beginTransaction();
  
    /**
     * Commit transaction.
     * @return bool
     */
    public function commit();
  
    /**
     * Rollback transaction.
     * @return bool
     */
    public function rollback();
  
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
     * @return bool
     */
    public function inTransaction();
  
    /**
     * @return mixed
     */
    public function connect();
  
    /**
     * @return mixed
     */
    public function reconnect();
  }