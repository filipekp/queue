<?php
  /**
   * @author    Pavel Filípek <pavel@filipek-czech.cz>
   * @copyright © 2019, Proclient s.r.o.
   * @created   03.04.2019
   */
  
  use filipekp\queue\QueueManager;
  
  try {
    $queueManager = QueueManager::getInstance('localhost', 'root', 'root', 'database_name');
    $queueManager->setDbPrefix('prefix_');
    
    /* Promaze stare pozadavky z fronty */
    $queueManager->deleteOldQueueItems();
    
    $queueCheckerProcessor = $queueManager::getQueueCheckerInstance();
    $queueCheckerProcessor->setNameRunQueue('runQueue.php');
    $queueCheckerProcessor->testQueueProcessor();
  } catch (Exception $e) {
    QueueManager::printMsg('ERROR', $e->getMessage());
  }
  