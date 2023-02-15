<?php
  /**
   * @author    Pavel Filípek <pavel@filipek-czech.cz>
   * @copyright © 2019, Proclient s.r.o.
   * @created   03.04.2019
   */
  require_once __DIR__ . '/framework.php';
  
  use filipekp\queue\QueueManager;
  
  try {
    QueueManager::printMsg('OK', 'Begin install ...');
    $queueManager = QueueManager::getInstance('localhost', 'root', 'root', 'database_name');
    $queueManager->setDbPrefix('prefix_');
    $queueManager->install();
    
    QueueManager::printMsg('OK', 'End install.');
  } catch (Exception $e) {
    QueueManager::printMsg('ERROR', $e->getMessage());
  }