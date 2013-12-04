<?php
namespace QueueCenter;

/**
 *
 *
 *
 * @category   QueueCenter
 * @package    QueueCenter
 */
class Exchange
{
	
	/**
	 * Exchange channel name
	 * @var string
	 */
	protected $_name;

	/**
	 * Queue adapter
	 * @var \QueueCenter\Adapter\RabbitMQ
	 */
	private $_adapter;
	
	/**
	 * Dependency injection queue adapter key
	 * @param string
	 */
	protected $_source = 'queue_adapter';
	
	/**
	 * Exchange type
	 * @param string
	 */
	protected $_type = 'topic';
	
	/**
	 * Queue options
	 * @param array
	 */
	protected $_options = [
		'passive' => false,
		'durable' => true,
		'auto_delete' => false,
		'internal' => false,
		'nowait' => false,
		'arguments' => null,
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
	 * Declare exchange
	 */
	private function _declare()
	{
		$this->getAdapter()->exchangeDeclare($this->_name, $this->_type, $this->_options['passive'], $this->_options['durable'], $this->_options['auto_delete'], $this->_options['internal'], $this->_options['nowait'], $this->_options['arguments'], $this->_options['ticket']);
	}
	
	/**
	 * Publish message to exchange
	 * 
	 * @param string|array|object $message
	 * @param string $routingKey
	 * @return \QueueCenter\Exchange
	 */
	public function publish($message, $routingKey = "*")
	{
		$message = $this->_filterMessage($message);
		$this->getAdapter()->exchangePublish($message, $this->_name, $routingKey, $mandatory = false, $immediate = false, $this->_options['ticket']);
				
		return $this;
	}
	
	/**
	 * Filter exchange message, if message not string serialize it
	 * 
	 * @param string|array|object $message
	 * @return string
	 */
	protected function _filterMessage($message)
	{
		if (is_string($message)) {
			return $message;
		}
		return serialize($message);
	}
	
	/**
	 * Add new user exchange
	 * 
	 * @param \stdClass $config
	 * @param integer $userId
	 * @param string $name
	 * @return boolean
	 */
	public static function addUserExchange(\stdClass $config, $userId, $name)
	{
		$storage = new Storage\Exchange($config);
		$fullName = self::generateUserExchangeName($userId, $name);
		if (!$storage->add($userId, $fullName)) {
			return false;
		}
		$exchange = new self($fullName, $config);
		$exchange->getAdapter();
		
		return true;
	}
	
	/**
	 * Publish message to user exchange by router
	 * 
	 * @param \stdClass $config
	 * @param integer $userId
	 * @param string $name
	 * @param string|array|object $message
	 * @param string $handler
	 * @param string $routingKey
	 * @return boolean
	 */
	public static function publishToUserExchange(\stdClass $config, $userId, $name, $message, $handler, $routingKey = "*")
	{
		$storage = new Storage\Exchange($config);
		$fullName = self::generateUserExchangeName($userId, $name);
		if (!$storage->getByName($fullName)) {
			if (!self::addUserExchange($config, $userId, $name)) {
				return false;
			}
		}
		$exchange = new self($fullName, $config);		
		$exchange->publish(['publish' => time(), 'handler' => $handler, 'message' => $message], $routingKey);

		return true;
	}
	
	/**
	 * Return exchange name
	 * 
	 * @param integer $userId
	 * @param string $name
	 * @return string
	 */
	public static function generateUserExchangeName($userId, $name)
	{
		return "user_".$userId."_".$name;
	}
}