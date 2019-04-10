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
  class Queue {
    /** @var Database  */
    protected static $db;
    
    protected $processor = 0;
    protected $requestTimeout = 240;
    protected $processPID;
    
  
    const STATE_NEW     = 'new';
    const STATE_PROCESS = 'process';
    const STATE_WAIT    = 'wait';
    const STATE_ERROR   = 'error';
    const STATE_DONE    = 'done';
    
    public function __construct(Database $database, $processor) {
      self::$db = $database;
      $this->processor = $processor;
      
      $this->processPID = getmypid();
    }
  
    /**
     * Zavola danou URL.
     *
     * @param string $link
     * @param array  $paramsArray
     *
     * @return bool|string
     * @throws \Exception
     */
    protected function callUrl($link, $paramsArray = []) {
      $ch = curl_init();
        curl_setopt($ch, CURLOPT_HTTPHEADER, [
          'Expect:',
          'Accept-Encoding: gzip, deflate'
        ]);
        curl_setopt($ch, CURLOPT_ENCODING, "gzip");
        curl_setopt($ch, CURLOPT_URL, $link);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, TRUE);
        curl_setopt($ch, CURLOPT_TIMEOUT, $this->requestTimeout);
        curl_setopt($ch, CURLOPT_MAXREDIRS, 10);
        curl_setopt($ch, CURLOPT_AUTOREFERER, TRUE);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
        curl_setopt($ch, CURLOPT_USERAGENT, "QueueProcessor-proclient-ver. 1.0");
        curl_setopt($ch, CURLOPT_POST, count($paramsArray));
        curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query([json_encode($paramsArray)]));
    
        //execute post
        $result = curl_exec($ch);
        
        if (curl_errno($ch)) {
          throw new \Exception(curl_error($ch));
        }
    
      //close connection
      curl_close($ch);
      
      return $result;
    }
  
    /**
     * Spusti zpracovani fronty.
     *
     * @throws \Exception
     */
    public function run() {
      $QueueChecker = QueueManager::getQueueCheckerInstance();
      $QueueChecker::processorExists($this->processor);
      
      $moreTasks = FALSE;
      
      $table = SqlTable::create('queue', 'q');
      while (TRUE) {
        $filterCurrentItem = SqlFilter::create();
        
        try {
          if (!$moreTasks) {
            // blokovani uloh pro muj proces
            self::$db->beginTransaction();
              $filterReserveItems = SqlFilter::create()
                ->compare('state', '=', self::STATE_NEW)
                ->andL()->isEmpty('processing_PID')
                ->andL()->compare('queue_processor_id', '=', $this->processor);
              self::$db->query("UPDATE {$table->getFullName()}	SET	processing_PID = '" . $this->processPID . "' WHERE {$filterReserveItems} ORDER BY date_added ASC, id ASC LIMIT 5");
              $affected = self::$db->countAffected();
            self::$db->commit();
          }
          
          $filter = SqlFilter::create()
            ->compare($table->column('state'), '=', 'new')
            ->andL()->compare($table->column('queue_processor_id'), '=', $this->processor)
            ->andL()->compare($table->column('processing_PID'), '=', $this->processPID);
          $queueResult = self::$db->query("SELECT * FROM {$table} WHERE {$filter} ORDER BY {$table->date_added} ASC, {$table->id} ASC LIMIT 0,2");
          $currentItem = $queueResult->row;
          
          if ($queueResult->num_rows > 0) {
            
            $filterCurrentItem = SqlFilter::create()
              ->compare('id', '=', $currentItem['id']);
            
            self::$db->query("UPDATE {$table->getFullName()} SET state='" . self::STATE_PROCESS . "', date_start='" . date('Y-m-d H:i:s') . "' WHERE {$filterCurrentItem}");
            
            if ($queueResult->num_rows > 1) {
              $moreTasks = TRUE;
            } else {
              $moreTasks = FALSE;
            }
          
            
            if (!is_null($currentItem['parent_group_id'])) {
              self::$db->query("UPDATE {$table->getFullName()}	SET state='" . self::STATE_WAIT . "', date_start='" . date('Y-m-d H:i:s') . "' WHERE {$filterCurrentItem}");
              
              $waiting = TRUE;
              while ($waiting) {
                $filter2 = SqlFilter::create()
                  ->compare($table->column('state'), '<>', 'done')
                  ->andL()->compare($table->column('group_id'), '=', $currentItem['parent_group_id']);
                $queueParentResult = self::$db->query("SELECT * FROM {$table} WHERE {$filter2} LIMIT 0,1");
  
                $waiting = ($queueParentResult->num_rows > 0);
                sleep(10);
              }
            }
  
            
            if (isset($currentItem['url']) && ($url = $currentItem['url'])) {
              QueueManager::printMsg('OK', 'Call URL: ' . $url);
              $response = $this->callUrl($url, ((isset($currentItem['data']) && ($data = (array)json_decode($currentItem['data'], TRUE))) ? $data : []));
              $responseArr = json_decode($response, TRUE);
              
              $responseResult = '';
              if ($responseArr) {
                if (isset($responseArr['errors']) && $responseArr['errors']) {
                  $errorsString = json_encode($responseArr['errors'], JSON_UNESCAPED_UNICODE);
                  throw new \Exception($errorsString);
                } else {
                  $responseResult = $responseArr;
                }
              } elseif ($response) {
                $responseResult = $response;
              }
  
              self::$db->query("UPDATE {$table->getFullName()}	SET state='" . self::STATE_DONE . "', message='" . self::$db->escape(((is_array($responseResult)) ? json_encode($responseResult, JSON_PRETTY_PRINT|JSON_UNESCAPED_UNICODE) : (string)$responseResult)) . "', date_end='" . date('Y-m-d H:i:s') . "' WHERE {$filterCurrentItem}");
              QueueManager::printMsg('OK', "URL `{$url}` done.");
            }
          }
        } catch (\Exception $e) {
          QueueManager::printMsg('ERROR', $e->getMessage());
          self::$db->query("UPDATE {$table->getFullName()}	SET state='" . self::STATE_ERROR . "', message='" . self::$db->escape($e->getMessage()) . "', date_end='" . date('Y-m-d H:i:s') . "' WHERE {$filterCurrentItem}");
        }
  
        if (!$moreTasks) {
          sleep(10);
        }
      }
    }
  }