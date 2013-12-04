<?php
namespace QueueCenter\Storage;

/**
 *
 *
 *
 * @category   Storage
 * @package    QueueCenter
 */
class Exchange
{	
	/**
	 * Storage adapter
	 * @var CActiveRecord
	 */
	protected $_adapter;

	/**
	 * Queue config
	 * @param \stdClass
	 */
	protected $_config;
	
	/**
	 * Construct
	 *
	 * @param \stdClass $config
	 */
	public function __construct(\stdClass $config)
	{
		$this->_config = $config;
	}
	
	/**
	 * Return storage adapter
	 *
	 * @return CActiveRecord
	 */
	public function getAdapter()
	{
		if (!$this->_adapter) {
			$this->_adapter = $this->_config->storageExchange;
			if (!($this->_adapter instanceof AdapterInterface)) {
				throw new \Exception("Not valid QueueCenter storage adapter!");
			}
		}
	
		return $this->_adapter;
	}
	
	/**
	 * Add new exchange to queue
	 * 
	 * @param string $name
	 * @return boolean
	 */
	public function add($userId, $name)
	{
		if ($this->getByName($name)) {
			return false;
		}
		$result = $this->_adapter->add(['user_id' => $userId, 'name' => $name]);

		return $result;
	}
	
	/**
	 * Remove exchange by id
	 * 
	 * @param integer $id
	 * @return boolean
	 */
	public function remove($id)
	{
		if (!$this->getById($id)) {
			return false;
		}
		$this->_adapter->remove(['id' => $id]);
		
		return true;
	}
	
	/**
	 * Return exchange by name
	 * 
	 * @param string $name
	 * @return boolean|array
	 */
	public function getByName($name)
	{
		$adapter = $this->getAdapter();
		if (!($exchange = $adapter->get(["name" => $name]))) {
			return false;
		}
		
		return $exchange;
	}
	
	/**
	 * Return exchange by id
	 * 
	 * @param integer $id
	 * @return boolean|array
	 */
	public function getById($id)
	{
		$adapter = $this->getAdapter();
		if (!($exchange = $adapter->get(["id" => $id]))) {
			return false;
		}
	
		return $exchange;
	}
	
	/**
	 * Return all exchange queues by id
	 * 
	 * @return boolean|array
	 */
	public function getQueues($id)
	{
		$queueStorage = new Queue($this->_config);
		return $queueStorage->getQueuesByExchange($id);
	}
	
	/**
	 * Return user bind exchanges
	 * 
	 * @param integer $userId
	 * @return array
	 */
	public function getUserExchanges($userId)
	{
		$adapter = $this->getAdapter();
		if (!($exchanges = $adapter->get(['user_id' => $userId]))) {
			return [];
		}
		
		return $exchanges;
	}
}