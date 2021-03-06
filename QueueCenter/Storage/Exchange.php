<?php
namespace QueueCenter\Storage;

/**
 * Class Exchange
 *
 * @category   Storage
 * @package    QueueCenter
 */
class Exchange
{
    /**
     * Exchange storage adatepr
     * @var \QueueCenter\Storage\AdapterInterface
     */
	protected $_adapter;

    /**
     * Queue name prefix
     * @var string
     */
    protected $_prefix = null;


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

        if (isset($this->_config['exchangePrefix'])) {
            $this->_prefix = $this->_config['exchangePrefix'];
        }
	}

    /**
     * Return exchange name
     *
     * @param string $name
     * @return string
     */
    public function getFullName($name)
    {
        if ($this->_prefix && strpos($name, $this->_prefix) !== 0) {
            $name = $this->_prefix."_".$name;
        }

        return $name;
    }
	
	/**
	 * Return storage adapter
	 *
	 * @return \QueueCenter\Storage\AdapterInterface
	 */
	public function getAdapter()
	{
		if (!$this->_adapter) {
			$this->_adapter = $this->_config['storageExchange'];
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

        $name = $this->getFullName($name);
		$result = $this->_adapter->add(array('user_id' => $userId, 'name' => $name));

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
		$this->_adapter->remove(array('id' => $id));
		
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
        $name = $this->getFullName($name);
		$adapter = $this->getAdapter();
		if (!($exchange = $adapter->get(array("name" => $name)))) {
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
		if (!($exchange = $adapter->get(array("id" => $id)))) {
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
		return $queueStorage->getExchangeQueues($id);
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
		if (!($exchanges = $adapter->get(array('user_id' => $userId)))) {
			return [];
		}
		
		return $exchanges;
	}
}