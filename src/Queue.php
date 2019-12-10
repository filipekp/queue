<?php
  namespace filipekp\queue;
  
  use filipekp\queue\db\Database;
  use filipekp\queue\db\DatabaseException;
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
     * @return array
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
        $headers = curl_getinfo($ch);
        
        if (curl_errno($ch)) {
          throw new \Exception(curl_error($ch));
        }
    
      //close connection
      curl_close($ch);
      
      return [
        'result' => $result,
        'headers' => $headers,
      ];
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
          $filterReserveItems = SqlFilter::create()->compare('state', '=', self::STATE_NEW)->andL()->isEmpty('processing_PID')->andL()->compare('queue_processor_id', '=', $this->processor);
          if (!$moreTasks) {
            // blokovani 2 uloh pro muj proces
            self::$db->beginTransaction();
            self::$db->query("UPDATE {$table->getFullName()}	SET	processing_PID = '" . $this->processPID . "' WHERE {$filterReserveItems} ORDER BY date_added ASC, id ASC LIMIT 2");
            self::$db->commit();
          } else {
            self::$db->beginTransaction();
            self::$db->query("UPDATE {$table->getFullName()}	SET	processing_PID = '" . $this->processPID . "' WHERE {$filterReserveItems} ORDER BY date_added ASC, id ASC LIMIT 1");
            self::$db->commit();
          }
  
          $filter      = SqlFilter::create()->compare($table->column('state'), '=', 'new')->andL()->compare($table->column('queue_processor_id'), '=', $this->processor)->andL()->compare($table->column('processing_PID'), '=', $this->processPID);
          $queueResult = self::$db->query("SELECT * FROM {$table} WHERE {$filter} ORDER BY {$table->date_added} ASC, {$table->id} ASC LIMIT 0,2");
          $currentItem = $queueResult->row;
  
          if ($queueResult->num_rows > 0) {
    
            $filterCurrentItem = SqlFilter::create()->compare('id', '=', $currentItem['id']);
    
            self::$db->query("UPDATE {$table->getFullName()} SET state='" . self::STATE_PROCESS . "', date_start='" . date('Y-m-d H:i:s') . "' WHERE {$filterCurrentItem}");
    
            if ($queueResult->num_rows > 1) {
              $moreTasks = TRUE;
            } else {
              $moreTasks = FALSE;
            }
    
    
            if (!is_null($currentItem['parent_group_id'])) {
              self::$db->query("UPDATE {$table->getFullName()}	SET state='" . self::STATE_WAIT . "', date_start='" . date('Y-m-d H:i:s') . "' WHERE {$filterCurrentItem}");
              $currItem = self::$db->query("SELECT * FROM {$table->getFullName()} WHERE {$filterCurrentItem}")->row;
  
              $filter4 = SqlFilter::create()
                ->inArray($table->column('state'), [self::STATE_NEW, self::STATE_PROCESS, self::STATE_WAIT])
                ->andL()->compare($table->column('group_id'), '=', $currentItem['parent_group_id']);
              $countChildrenResult = self::$db->query("SELECT COUNT(id) as `count` FROM {$table} WHERE {$filter4}");
              $countChildren = $countChildrenResult->row['count'];
              $maxTimeout = ($countChildren * $this->requestTimeout) + 240;
              
              $waiting = TRUE;
              while ($waiting) {
                if ((new \DateTime($currItem['date_start'])) < (new \DateTime())->sub((new \DateInterval("PT{$maxTimeout}S")))) {
                  throw new \Exception("Operation expired after {$maxTimeout} seconds while waiting for children.", 504);
                }
                
                $filter2 = SqlFilter::create()
                  ->inArray($table->column('state'), [self::STATE_NEW, self::STATE_PROCESS, self::STATE_WAIT])
                  ->andL()->compare($table->column('processing_PID'), '<>', $this->processPID)
                  ->andL()->compare($table->column('group_id'), '=', $currentItem['parent_group_id']);
                $queueParentResult = self::$db->query("SELECT id FROM {$table} WHERE {$filter2} LIMIT 0,1");
                $waiting = ($queueParentResult->num_rows > 0);
                
                if ($waiting) {
                  sleep(5);
                } else {
                  $filter3 = SqlFilter::create()
                    ->inArray($table->column('state'), [self::STATE_ERROR])
                    ->andL()->compare($table->column('group_id'), '=', $currentItem['parent_group_id']);
                  $existsErrorChildResult = self::$db->query("SELECT id FROM {$table} WHERE {$filter3} LIMIT 0,1");
                  if ($existsErrorChildResult->num_rows > 0) {
                    throw new \Exception("Some children ended with error state.", 504);
                  }
                }
              }
            }
    
    
            if (isset($currentItem['url']) && ($url = $currentItem['url'])) {
              QueueManager::printMsg('OK', 'Call URL: ' . $url);
              $result      = $this->callUrl($url, ((isset($currentItem['data']) && ($data = (array)json_decode($currentItem['data'], TRUE))) ? $data : []));
              $response    = $result['result'];
              $headers     = $result['headers'];
              $stateCode   = (int)$headers['http_code'];
              $state       = self::STATE_DONE;
              
              $responseResult = '';
              if ($stateCode == 200 && ($responseArr = json_decode($response, TRUE))) {
                if (isset($responseArr['errors']) && $responseArr['errors']) {
                  $errorsString = json_encode($responseArr['errors'], JSON_UNESCAPED_UNICODE);
                  throw new \Exception($errorsString);
                } else {
                  $responseResult = $responseArr;
                }
              } elseif ($stateCode == 200) {
                $responseResult = $response;
              } else {
                $responseResult = $response;
                $state = self::STATE_ERROR;
              }
      
              self::$db->query("UPDATE {$table->getFullName()}	SET state='" . $state . "', state_code='" . $stateCode . "', message='" . self::$db->escape(((is_array($responseResult)) ? json_encode($responseResult, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) : (string)$responseResult)) . "', date_end='" . date('Y-m-d H:i:s') . "' WHERE {$filterCurrentItem}");
              QueueManager::printMsg(strtoupper($state), "URL `{$url}` complete with stateCode: `{$stateCode}`.");
            }
          }
        } catch (DatabaseException $e) {
          QueueManager::printMsg("ERROR: {$e->getCode()}", $e->getMessage());
          
          if ($e->getCode() == 2006) {
            $countTryReconnect = 5;
            $countTryReconnectCounter = 1;
            while ($countTryReconnect >= $countTryReconnectCounter) {
              sleep(5);
              try {
                QueueManager::printMsg("INFO", "Try reconnect {$countTryReconnectCounter}/{$countTryReconnect}.");
                self::$db->reconnect();
                break;
              } catch (DatabaseException $e) {
                QueueManager::printMsg("ERROR: {$e->getCode()}", $e->getMessage());
              }
  
              $countTryReconnectCounter++;
            }
            
            if ($countTryReconnect < $countTryReconnectCounter) {
              throw new \Exception("Server has gone away and reconnect failed after {$countTryReconnect} attempts.");
            }
          }
        } catch (\Exception $e) {
          $stateCode = (($e->getCode()) ? $e->getCode() : 500);
          QueueManager::printMsg('ERROR', $e->getMessage() . ", stateCode: {$stateCode}");
          self::$db->query("UPDATE {$table->getFullName()}	SET state='" . self::STATE_ERROR . "', state_code='" . $stateCode . "', message='" . self::$db->escape($e->getMessage()) . "', date_end='" . date('Y-m-d H:i:s') . "' WHERE {$filterCurrentItem}");
        }
  
        if (!$moreTasks) {
          sleep(10);
        }
      }
    }
  
    /**
     * Nastavi timeout pro požadavek cURL.
     *
     * @param int $requestTimeout
     * @return Queue
     */
    public function setRequestTimeout(int $requestTimeout): Queue {
      $this->requestTimeout = $requestTimeout;
      
      return $this;
    }
  }