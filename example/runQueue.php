<?php
  /**
   * @author    Pavel Filípek <pavel@filipek-czech.cz>
   * @copyright © 2019, Proclient s.r.o.
   * @created   03.04.2019
   */
  require_once __DIR__ . '/framework.php';
  
  use filipekp\queue\QueueManager;
  
  $queueProcessorId = (int)$argv[1];
  
  if (!is_numeric($queueProcessorId)) {
    QueueManager::printMsg('ERROR', 'Unknown queueProcessorId!');
    exit(-1);
  }
  
  try {
    QueueManager::printMsg('OK', 'Starting queue for processor ' . $queueProcessorId . '...');
    $queueManager = QueueManager::getInstance('localhost', 'root', 'root', 'database_name');
    $queueManager->setDbPrefix('');
    
    $queueProcessor = $queueManager::getQueueInstance($queueProcessorId);
    QueueManager::printMsg('OK', 'Queue started.');
    
    $queueProcessor->run();
  } catch (Exception $e) {
    QueueManager::printMsg('ERROR', $e->getMessage());
  }
