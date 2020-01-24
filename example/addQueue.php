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
    $queueManager = QueueManager::getInstance('localhost', 'root', 'root', 'database_name');
    $id = $queueManager->writeRequest('https://localhpst/callUrl.php', NULL, ['aaa' => 111], QueueManager::PROCESSOR_GENERAL, NULL, NULL, Queue::TYPE_ASYNC, 3);
    
    // ziskani webhookHASH pro vagenerovani odkazu
    echo 'https://localhost.example.loc/queue/example/webhookQueue.php?hash=' . $queueManager->getWebhookHash($id);
  } catch (Exception $e) {
    QueueManager::printMsg('ERROR', $e->getMessage());
  }
