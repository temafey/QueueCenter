<?php
namespace QueueCenter\Storage;

/**
 * Class Queue
 *
 * @category   Storage
 * @package    QueueCenter
 */
class Queue
{
    /**
     * Queue storage adatepr
     * @var \QueueCenter\Storage\AdapterInterface
     */
    protected $_adapterQueue;

    /**
     * Queue to exchange storage adatepr
     * @var \QueueCenter\Storage\AdapterInterface
     */
	protected $_adapterQueueRouters;

	/**
	 * Queue config
	 * @param \stdClass
	 */
	protected $_config;
	
	/**
	 * Construct
	 *
	 * @param mixed $config
	 */
	public function __construct($config)
	{
		$this->_config = $config;
	}
	
	/**
	 * Return queue storage adapter
	 *
	 * @return \QueueCenter\Storage\AdapterInterface
	 */
	public function getAdapterQueue()
	{
		if (!$this->_adapterQueue) {
			$this->_adapterQueue = $this->_config['storageQueue'];
			if (!($this->_adapterQueue instanceof AdapterInterface)) {
				throw new \Exception("Not valid QueueCenter storage adapter!");
			}
		}
	
		return $this->_adapterQueue;
	}
	
	/**
	 * Return storage adapter
	 *
	 * @return \QueueCenter\Storage\AdapterInterface
	 */
	public function getAdapterQueueRouters()
	{
		if (!$this->_adapterQueueRouters) {
			$this->_adapterQueueRouters = $this->_config['storageQueueRouters'];
			if (!($this->_adapterQueueRouters instanceof AdapterInterface)) {
				throw new \Exception("Not valid QueueCenter storage adapter!");
			}
		}
	
		return $this->_adapterQueueRouters;
	}
	
	/**
	 * Add new queue
	 * 
	 * @param integer $userId 
	 * @param string $name
	 * @return boolean
	 */
	public function addQueue($userId, $name)
	{
		if ($this->getByName($name)) {
			return false;
		}

		return $this->_adapterQueue->add(["user_id" => $userId, "name" => $name]);
	}
	
	/**
	 * Add new queue router
	 * 
	 * @param integer $queueId
	 * @param integer $exchangeId
	 * @param string $routingKey
	 * @return boolean
	 */
	public function addQueueRouter($queueId, $exchangeId, $routingKey = '*')
	{
		if (count($this->getQueueRouters($queueId, $exchangeId, $routingKey)) != 0) {
			return false;
		}
		
		return $this->_adapterQueueRouters->add(["queue_id" => $queueId, "exchange_id" => $exchangeId, "routing_key" => $routingKey]);
	}
			
	
	/**
	 * Remove queue
	 * 
	 * @param string $name
	 * @param integer $exchangeId
	 * @param string $routingKey
	 * @return boolean
	 */
	public function removeQueue($queueId)
	{
		if (!($queue = $this->getById($queueId))) {
			return false;
		}
		$this->removeQueueRouter($queueId);		
		$this->_adapterQueue->remove($queue['id']);
		
		return true;
	}
	
	/**
	 * Remove queue routers
	 * 
	 * @param integer $queueId
	 * @param integer $exchangeId
	 * param string $routingKey
	 * @return true
	 */
	public function removeQueueRouter($queueId, $exchangeId = null, $routingKey = null)
	{
		if (!($routers = $this->getQueueRouters($queueId, $exchangeId, $routingKey))) {
			return false;
		}
		foreach ($routers as $router) {
			$this->_adapterQueueRouters->remove($router['id']);
		}
		
		return true;
	}
	
	/**
	 * Return queue
	 * 
	 * @param string $name
	 * @param integer $exchangeId
	 * @param string $routingKey
	 * @return boolean|array
	 */
	public function getQueueRouters($queueId, $exchangeId = null, $routingKey = null)
	{
		$adapter = $this->getAdapterQueueRouters();
		$params = ['queue_id' => $queueId];
		if (null !== $exchangeId) {
			$params['exchange_id'] = $exchangeId;
		}
		if (null !== $routingKey) {
			$params['routing_key'] = $routingKey;
		}
		if (!($routers = $adapter->get($params))) {
			return [];
		}
		if (!isset($routers['0'])) {
			$routers = [$routers];
		}
		
		return $routers;
	}
	
	/**
	 * Return queue by id
	 * 
	 * @param integer $id
	 * @return boolean|array
	 */
	public function getById($id)
	{
		$adapter = $this->getAdapterQueue();
		if (!($queue = $adapter->get(array("id" => $id)))) {
			return false;
		}
	
		return $queue;
	}
	
	/**
	 * Return queue by name
	 * 
	 * @param string $name
	 * @return boolean|array
	 */
	public function getByName($name)
	{
		$adapter = $this->getAdapterQueue();
		if (!($queue = $adapter->get(array("name" => $name)))) {
			return false;
		}
		
		return $queue;
	}
	
	/**
	 * Return queues by exchange id
	 * 
	 * @param integer $exchangeId
	 * @return array
	 */
	public function getExchangeQueues($exchangeId)
	{
		$adapter = $this->getAdapterQueueRouters();
		$queues = [];
		if (!($routers = $adapter->get(array('exchange_id' => $exchangeId)))) {
			return $queues;
		}
		if (!isset($routers[0])) {
			$routers = [$routers];
		}
		foreach ($routers as $router) {
			if (!isset($queues[$router['queue_id']])) {
				$queues[$router['queue_id']] = $this->getById($router['queue_id']);
			}
		}
		
		return $queues;
	}
	
	/**
	 * Return user bind queues
	 * 
	 * @param integer $userId
	 * @return array
	 */
	public function getUserQueues($userId)
	{
		$adapter = $this->getAdapterQueue();
		if (!($queues = $adapter->get(array('user_id' => $userId)))) {
			return [];
		}
		if (!isset($queues['0'])) {
			$queues = [$queues];
		}
		
		return $queues;
	}
}