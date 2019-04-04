<?php
  /**
   * @author    Pavel Filípek <pavel@filipek-czech.cz>
   * @copyright © 2019, Proclient s.r.o.
   * @created   03.04.2019
   */
  
  use filipekp\queue\QueueManager;
  
  $queueProcessorId = (int)$argv[1];
  
  if (!is_numeric($queueProcessorId)) {
    QueueManager::printMsg('ERROR', 'Unknown queueProcessorId!');
    exit(-1);
  }
  
  QueueManager::printMsg('OK', 'Starting queue for processor ' . $queueProcessorId . '...');
  $queueManager = QueueManager::getInstance('localhost', 'root', 'root', 'database_name');
  $queueManager->setDbPrefix('prefix_');
  
  $queueProcessor = $queueManager::getQueueInstance($queueProcessorId);
  QueueManager::printMsg('OK', 'Queue started.');
  
  $queueProcessor->run();