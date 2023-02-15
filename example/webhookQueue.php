<?php
  /**
   * @author    Pavel Filípek <pavel@filipek-czech.cz>
   * @copyright © 2020, Proclient s.r.o.
   * @created   23.01.2020
   */
  require_once __DIR__ . '/framework.php';
  
  use filipekp\queue\Queue;
  use filipekp\queue\QueueManager;
  
  try {
    $queueManager = QueueManager::getInstance('127.0.0.1', 'middleware_styleplus_cz', '123456789', 'middleware_styleplus_cz');
    $queueManager->webHookAsyncResult($_GET['hash'], $_POST);
    
  } catch (Exception $e) {
    QueueManager::printMsg('ERROR', $e->getMessage());
  }
