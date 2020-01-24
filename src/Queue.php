<?php
  namespace filipekp\queue;
  
  use filipekp\queue\db\Database;
  use filipekp\queue\db\DatabaseException;
  use PF\helpers\MyString;
  use PF\helpers\SqlFilter;
  use PF\helpers\SqlTable;
  use PF\helpers\Verifier;

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
  
    const STATE_NEW           = 'new';
    const STATE_PROCESS       = 'process';
    const STATE_PROCESS_ASYNC = 'process_async';
    const STATE_WAIT          = 'wait';
    const STATE_ERROR         = 'error';
    const STATE_DONE          = 'done';
  
    const TYPE_SYNC  = 'sync';
    const TYPE_ASYNC = 'async';
    
    const PARAM_WEB_HOOK_URL = 'web_hook_url';
  
    private static $VERSION = '___VERSION_N/A___';
    
    public function __construct(Database $database, $processor) {
      self::$db = $database;
      $this->processor = $processor;
      
      $this->processPID = getmypid();
      
      $t = filemtime(__FILE__);
      $major = date('y', $t);
      $minor = date('n', $t) . round((date('j', $t) / date('t', $t)) * 100);
      $release = date('G', $t).(int)date('i', $t).(int)date('s', $t);
      self::$VERSION = $major . '.' . $minor . '.' . $release;
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
        curl_setopt($ch, CURLOPT_USERAGENT, "QueueProcessor-proclient-ver." . self::getVersion());
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
        $filterCurrentItem = SqlFilter::create()->identity();
        $currentItem = NULL;
        
        try {
          if (!self::$db->connected()) {
            self::$db->connect();
          }
          
          $numRows = 0;
          $filterErr      = SqlFilter::create()
            ->compare($table->column('state'), '=', self::STATE_ERROR)
            ->andL(
              SqlFilter::create()
                ->compare($table->column('state_code'), '>=', '500')
            )
            ->andL()->compare($table->column('queue_processor_id'), '=', $this->processor)
            ->andL(
              SqlFilter::create()
                ->compare($table->column('retry'), '>', '0')
                ->andL()->compareColumns($table->column('retry_counter'), '<', $table->column('retry'))
                ->orL()->compare($table->column('retry'), '=', '-1')
            )->andL(
              SqlFilter::create()
                ->isEmpty($table->column('date_start'))
                ->orL()->compareColumns(
                  $table->column('date_start'), '<',
                  "DATE_ADD({$table->column('date_start')}, INTERVAL (CASE WHEN {$table->column('delay')} = 0 THEN 30 ELSE {$table->column('delay')} * 2 END) SECOND)")
            );
          $queueResultErr = self::$db->query("SELECT * FROM {$table} WHERE {$filterErr} ORDER BY {$table->date_added} ASC, {$table->id} ASC LIMIT 0,2");
          $numRows = $queueResultErr->num_rows;
          $currentItem = $queueResultErr->row;
  
          if (!$numRows) {
            $filterReserveItems = SqlFilter::create()->compare('state', '=', self::STATE_NEW)->andL()->isEmpty('processing_pid')->andL()->compare('queue_processor_id', '=', $this->processor);
            if (!$moreTasks) {
              // blokovani 2 uloh pro muj proces
              self::$db->beginTransaction();
              self::$db->query("UPDATE {$table->getFullName()}	SET	processing_pid = '" . $this->processPID . "' WHERE {$filterReserveItems} ORDER BY date_added ASC, id ASC LIMIT 2");
              self::$db->commit();
            } else {
              self::$db->beginTransaction();
              self::$db->query("UPDATE {$table->getFullName()}	SET	processing_pid = '" . $this->processPID . "' WHERE {$filterReserveItems} ORDER BY date_added ASC, id ASC LIMIT 1");
              self::$db->commit();
            }
          
            $filter      = SqlFilter::create()->compare($table->column('state'), '=', self::STATE_NEW)->andL()->compare($table->column('queue_processor_id'), '=', $this->processor)->andL()->compare($table->column('processing_pid'), '=', $this->processPID);
            $queueResult = self::$db->query("SELECT * FROM {$table} WHERE {$filter} ORDER BY {$table->date_added} ASC, {$table->id} ASC LIMIT 0,2");
            $numRows = $queueResult->num_rows;
            $currentItem = $queueResult->row;
          }
  
          if ($numRows > 0) {
            $filterCurrentItem = SqlFilter::create()->compare('id', '=', $currentItem['id']);
    
            self::$db->query("UPDATE {$table->getFullName()} SET state='" . self::STATE_PROCESS . "', date_start='" . date('Y-m-d H:i:s') . "', date_end = NULL, retry_counter=(retry_counter + 1), delay = (CASE WHEN delay = 0 THEN 30 ELSE delay * 2 END) WHERE {$filterCurrentItem}");
    
            if ($numRows > 1) {
              $moreTasks = TRUE;
            } else {
              $moreTasks = FALSE;
            }
    
    
            if (!is_null($currentItem['parent_group_id'])) {
              self::$db->query("UPDATE {$table->getFullName()}	SET state='" . self::STATE_WAIT . "' WHERE {$filterCurrentItem}");
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
                  ->andL()->compare($table->column('processing_pid'), '<>', $this->processPID)
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
  
                  self::$db->query("UPDATE {$table->getFullName()}	SET state='" . self::STATE_PROCESS . "', date_start='" . date('Y-m-d H:i:s') . "' WHERE {$filterCurrentItem}");
                }
              }
            }
    
    
            if (isset($currentItem['url']) && ($url = $currentItem['url'])) {
              QueueManager::printMsg('OK', 'Call URL: ' . $url);
              $data = [];
              if (isset($currentItem['data']) && ($dataFromJson = (array)json_decode($currentItem['data'], TRUE))) {
                $data = $dataFromJson;
              }
              
              if ($currentItem['process_type'] == self::TYPE_ASYNC && isset($data[self::PARAM_WEB_HOOK_URL])) {
                $data[self::PARAM_WEB_HOOK_URL] = vsprintf($data[self::PARAM_WEB_HOOK_URL], [QueueManager::getWebhookHash($currentItem['id'])]);
              }
              
              $result      = $this->callUrl($url, $data);
              $response    = $result['result'];
              $headers     = $result['headers'];
              $stateCode   = (int)$headers['http_code'];
              $state       = (($currentItem['process_type'] == self::TYPE_ASYNC) ? self::STATE_PROCESS_ASYNC : self::STATE_DONE);
              
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
      
              self::$db->query("UPDATE {$table->getFullName()}
                SET state='" . $state . "',
                state_code='" . $stateCode . "',
                message='" . self::$db->escape(((is_array($responseResult)) ? json_encode($responseResult, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) : (string)$responseResult)) . "'" .
                (($currentItem['process_type'] == self::TYPE_SYNC || $state == self::STATE_ERROR) ? ", date_end='" . date('Y-m-d H:i:s') . "'" : '') .
                "WHERE {$filterCurrentItem}");
              
//              // zaslani vysledku na webhook URL
//              if (!is_null($currentItem['webhook_url']) && $currentItem['webhook_url']) {
//                QueueManager::printMsg(strtoupper($state), "Call webHookUrl `{$currentItem['webhook_url']}` ...");
//                  $responseWebHook = $this->callUrl($currentItem['webhook_url'], (array)$responseResult);
//                  $responseWebHookStateCode = (int)((isset($responseWebHook['headers']['http_code'])) ? $responseWebHook['headers']['http_code'] : 0);
//                QueueManager::printMsg(strtoupper($state), "WebHookUrl `{$currentItem['webhook_url']}` response with state `{$responseWebHookStateCode}`.");
//              }
              
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
                if (self::$db->reconnect()) {
                  QueueManager::printMsg("INFO", "Connected.");
                }
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
          self::$db->query("UPDATE {$table->getFullName()}
            SET state='" . self::STATE_ERROR . "',
            state_code='" . $stateCode . "',
            message='" . self::$db->escape($e->getMessage()) . "',
            date_end='" . date('Y-m-d H:i:s') . "'
            WHERE {$filterCurrentItem}");
  
        }
        
        // zaslani vysledku na webhook URL
        if (!is_null($currentItem) && $currentItem) {
          $filterCurrItem = SqlFilter::create()->compare('id', '=', $currentItem['id']);
          $currItem = self::$db->query("SELECT * FROM {$table->getFullName()} WHERE {$filterCurrItem}")->row;
          if (!is_null($currItem['webhook_url']) && $currItem['webhook_url']) {
            QueueManager::printMsg('INFO', "Call webHookUrl `{$currentItem['webhook_url']}` ...");
            $msg = (($msgArr = json_decode($currItem['message'], TRUE)) ? $msgArr : $currItem['message']);
            $responseWebHook = $this->callUrl($currItem['webhook_url'], (array)$msg);
            $responseWebHookStateCode = (int)((isset($responseWebHook['headers']['http_code'])) ? $responseWebHook['headers']['http_code'] : 0);
            QueueManager::printMsg('INFO', "WebHookUrl `{$currItem['webhook_url']}` response with state `{$responseWebHookStateCode}`.");
          }
        }
  
        if (!$moreTasks) {
          self::$db->closeConnection();
          sleep(60);
        }
      }
    }
  
    /**
     * Nastavi timeout pro požadavek cURL.
     *
     * @param int $requestTimeout
     *
     * @return Queue
     */
    public function setRequestTimeout(int $requestTimeout): Queue {
      $this->requestTimeout = $requestTimeout;
      
      return $this;
    }
  
    /**
     * Vrátí aktuální verzi konektoru.
     *
     * @return string
     */
    public static function getVersion() {
      return self::$VERSION;
    }
  }