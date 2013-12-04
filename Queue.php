<?php
namespace QueueCenter;

use \QueueCenter\Storage;

/**
 *
 *
 *
 * @category   QueueCenter
 * @package    QueueCenter
 */
class Queue
{	
	/**
	 * Queue name
	 * @var string
	 */
	protected $_name;
	
	/**
	 * Queue adapter
	 * @var \QueueCenter\Adapter\RabbitMQ
	 */
	private $_adapter;
	
	/**
	 * Queue config
	 * @param \stdClass
	 */
	protected $_config;
	
	/**
	 * Queue options
	 * @param array
	 */
	protected $_options = [
		'name' => '',
		'passive' => false,
		'durable' => true,
		'exclusive' => false,
		'auto_delete' => false,
		'nowait' => false,
		'arguments' => [
	        "x-dead-letter-exchange" => array("S", "exchange-die"),
	        /*"x-message-ttl" => array("I", 10000), //milliseconds
	        "x-expires" => array("I", 16000)*/
	    ],
		'ticket' => null
	];
	
	/**
	 * Construct
	 * 
	 * @param string $name
	 * @param \stdClass $config
	 */
	public function __construct($name, \stdClass $config)
	{
		$this->_name = $name;
		$this->_config = $config;
	}
	
	/**
	 * Return queue adapter
	 * 
	 * @return \QueueCenter\Adapter\RabbitMQ
	 */
	public function getAdapter()
	{
		if (!$this->_adapter) {
			switch ($this->_config->adapter) {
				case 'rabbit':
					$connection = array(
						'type' => $this->_config->type,
						'host' => $this->_config->host,
						'port' => $this->_config->port,
						'username' => $this->_config->username,
						'password' => $this->_config->password,
						'vhost' => $this->_config->vhost
					);
					$this->_adapter = new \QueueCenter\Adapter\RabbitMQ($connection);
					break;
			}
			
			$this->_declare();
		}
		
		return $this->_adapter;
	}
	
	/**
	 * Declare queue
	 */
	private function _declare()
	{
		$this->getAdapter()->queueDeclare($this->_name, $this->_options['passive'], $this->_options['durable'], $this->_options['exclusive'], $this->_options['auto_delete'], $this->_options['nowait'],  $this->_options['arguments'], $this->_options['ticket']);
	}
	
	/**
	 * Bind exchange to queue
	 * 
	 * @param string $exchange
	 * @param string $routingKey
	 * @return \QueueCenter\Queue
	 */
	public function bind($exchange, $routingKey = "")
	{
		$this->getAdapter()->queueBind($this->_name, $exchange, $routingKey, $this->_options['nowait'], $this->_options['arguments'], $this->_options['ticket']);
		
		return $this;
	}
	
	/**
	 * Un\bBind exchange from queue
	 *
	 * @param string $exchange
	 * @param string $routingKey
	 * @return \QueueCenter\Queue
	 */
	public function unbind($exchange, $routingKey = "")
	{
		$this->getAdapter()->queueUnBind($this->_name, $exchange, $routingKey, $this->_options['arguments'], $this->_options['ticket']);
		
		return $this;
	}

	private $_consumed = 0;
	
	private $_target = 0;
	
	protected $_callback;
	
	/**
	 * Consume messages
	 * 
	 * @param $msgAmount
	 * @return \QueueCenter\Queue
	 */
	public function consume($msgAmount)
	{
		$this->_target = $msgAmount;
		$this->getAdapter()->queueConsume($this->_name, $this->_getConsumerTag(), false, false, $this->_options['exclusive'], $this->_options['nowait'], array($this, 'processMessage'), $this->_options['ticket']);	
		while (count($this->_adapter->getChannel()->callbacks)) {
			$this->_adapter->getChannel()->wait();
		}
		
		return $this;
	}
	
	/**
	 * Consumer callback method 
	 */
	final public function processMessage($message)
	{
		try {
			call_user_func($this->_callback, $message);
			$message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
			$this->_consumed++;
			$this->_maybeStopConsumer($message);
		} catch (\Exception $e) {
			throw $e;
		}
	}
	
	/**
	 * Generate consumer tag
	 * 
	 * @return string
	 */
	protected function _getConsumerTag()
	{
		return "PHPPROCESS_".getmypid();
	}
	
	/**
	 * If consumed message eq target stop consume
	 */
	protected function _maybeStopConsumer($message)
	{
		if ($this->_consumed == $this->_target) {
			$message->delivery_info['channel']->basic_cancel($message->delivery_info['consumer_tag']);
		}
	}
	
	/**
	 * Set consumer callback
	 * 
	 * @param \Closure $callback
	 * @return \QueueCenter\Queue
	 */
	public function setCallback(\Closure $callback)
	{
		$this->_callback = $callback;
	}
	
	/**
	 * Get messages
	 */
	public function get()
	{
		return $this->getAdapter()->queueGet($this->_name);
	}
	
	/**
	 * End message
	 */
	public function ack($deliveryTag, $multiple = false)
	{
		return $this->getAdapter()->queueAck($deliveryTag, $multiple);
	}
	
	/**
	 * Set queue options
	 * 
	 * @param array $options
	 * @return \QueueCenter\Queue
	 */
	public function setOptions(array $options)
	{
		$this->_options = array_merge($this->_options, $options);
	
		return $this;
	}
	
	/**
	 * Add new user queue
	 *
	 * @param \stdClass $config
	 * @param integer $userId
	 * @param string $name
	 * @return boolean
	 */
	public static function addUserQueue(\stdClass $config, $userId, $name)
	{
		$storage = new Storage\Queue($config);
		$fullName = self::generateUserQueueName($userId, $name);
		if (!$storage->addQueue($userId, $fullName)) {
			return false;
		} 
		$queue = new self($fullName, $config);
		$queue->getAdapter();
	
		return true;
	}
	
	/**
	 * Bind exchange to user queue
	 * 
	 * @param \stdClass $config
	 * @param integer $userId
	 * @param string $name
	 * @param integer $exchangeId
	 * @param string $routingKey
	 * @return boolean
	 */
	public static function bindUserQueue(\stdClass $config, $userId, $name, $exchangeId, $routingKey = "*") 
	{
		$storage = new Storage\Queue($config);
		$fullName = self::generateUserQueueName($userId, $name);
		if(!($queue = $storage->getByName($fullName))) {
			if (!self::addUserQueue($config, $userId, $name)) {
				return false;
			}
			$queue = $storage->getByName($fullName);
		}
		if (!$storage->addQueueRouter($queue['id'], $exchangeId, $routingKey)) {
			if (!$storage->getQueueRouters($queue['id'], $exchangeId, $routingKey)) {
				return false;
			}
		}
		$storage = new Storage\Exchange($config);
		if (!($exchange = $storage->getById($exchangeId))) {
			return false;
		};		
		$queue = new self($fullName, $config);
		$queue->bind($exchange['name'], $routingKey);
		
		return true;
	}
	
	/**
	 * Unbind exchange from user queue
	 *
	 * @param \stdClass $config
	 * @param integer $userId
	 * @param string $name
	 * @param integer $exchangeId
	 * @param string $routingKey
	 * @return boolean
	 */
	public static function unbindUserQueue(\stdClass $config, $userId, $name, $exchangeId, $routingKey = "*")
	{
		$storage = new Storage\Queue($config);
		$fullName = self::generateUserQueueName($userId, $name);
		if(!($queue = $storage->getByName($fullName))) {
			return false;
		}
		if (!$storage->removeQueueRouter($queue['id'], $exchangeId, $routingKey)) {
			return false;
		}
		$storage = new Storage\Exchange($config);
		if (!($exchange = $storage->getById($exchangeId))) {
			return false;
		};
		$queue = new self($fullName, $config);
		$queue->unbind($exchange['name'], $routingKey);
	
		return true;
	}
	
	/**
	 * Return queue name
	 *
	 * @param integer $userId
	 * @param string $name
	 * @return string
	 */
	public static function generateUserQueueName($userId, $name)
	{
		return "user_".$userId."_".$name;
	}
}