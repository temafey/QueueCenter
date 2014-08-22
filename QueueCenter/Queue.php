<?php
/**
 * @namespace
 */
namespace QueueCenter;

use \QueueCenter\Storage;

/**
 * Class Queue
 *
 * @package QueueCenter
 */
class Queue
{	
	/**
	 * Queue name
	 * @var string
	 */
	protected $_name;

    /**
     * Queue name prefix
     * @var string
     */
    protected $_prefix = null;
	
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
	protected $_options = array(
		'name' => '',
		'passive' => false,
		'durable' => true,
		'exclusive' => false,
		'auto_delete' => false,
		'nowait' => false,
		'arguments' => array(
	        "x-dead-letter-exchange" => array("S", "exchange-die"),
	        /*"x-message-ttl" => array("I", 10000), //milliseconds
	        "x-expires" => array("I", 16000)*/
        ),
		'ticket' => null
    );
	
	/**
	 * Construct
	 * 
	 * @param string $name
	 * @param mixed $config
	 */
	public function __construct($name, $config)
	{
        $this->_name = $name;
		$this->_config = $config;

        if (isset($this->_config['queuePrefix'])) {
            $this->_prefix = $this->_config['queuePrefix'];
        }
	}
	
	/**
	 * Return queue adapter
	 * 
	 * @return \QueueCenter\Adapter\RabbitMQ
	 */
	public function getAdapter()
	{
		if (!$this->_adapter) {
			switch ($this->_config['adapter']) {
				case 'rabbit':
					$connection = array(
						'type' => $this->_config['type'],
						'host' => $this->_config['host'],
						'port' => $this->_config['port'],
						'username' => $this->_config['username'],
						'password' => $this->_config['password'],
						'vhost' => $this->_config['vhost']
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
        $queueName = $this->getName();
		return $this->getAdapter()->queueDeclare($queueName, $this->_options['passive'], $this->_options['durable'], $this->_options['exclusive'], $this->_options['auto_delete'], $this->_options['nowait'],  $this->_options['arguments'], $this->_options['ticket']);
	}

    /**
     * Return queue name
     *
     * @return string
     */
    public function getName()
    {
        return ($this->_prefix) ? $this->_prefix."_".$this->_name : $this->_name;
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
        $queueName = $this->getName();
		return $this->getAdapter()->queueBind($queueName, $exchange, $routingKey, $this->_options['nowait'], $this->_options['arguments'], $this->_options['ticket']);
	}
	
	/**
	 * Unbind exchange from queue
	 *
	 * @param string $exchange
	 * @param string $routingKey
	 * @return \QueueCenter\Queue
	 */
	public function unbind($exchange, $routingKey = "")
	{
        $queueName = $this->getName();
		return $this->getAdapter()->queueUnBind($queueName, $exchange, $routingKey, $this->_options['arguments'], $this->_options['ticket']);
	}

    /**
     * Delete queue
     *
     * @param string $exchange
     * @param string $routingKey
     * @return \QueueCenter\Queue
     */
    public function delete()
    {
        $queueName = $this->getName();
        return $this->getAdapter()->queueDelete($queueName);
    }

    /**
     * @var int
     */
    private $_consumed = 0;

    /**
     * @var int
     */
    private $_target = 0;

    /**
     * @var closure
     */
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
        $queueName = $this->getName();
		$this->getAdapter()->queueConsume($queueName, $this->_getConsumerTag(), false, false, $this->_options['exclusive'], $this->_options['nowait'], array($this, 'processMessage'), $this->_options['ticket']);	
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
     *
     * @return \PhpAmqpLib\Wire\GenericContent\AMQPMessage
	 */
	public function get()
	{
        $queueName = $this->getName();
		return $this->getAdapter()->queueGet($queueName);
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
	 * @param mixed $config
	 * @param integer $userId
	 * @param string $name
	 * @return boolean
	 */
	public static function addUserQueue($config, $userId, $name)
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
	 * @param mixed $config
	 * @param integer $userId
	 * @param string $name
	 * @param integer $exchangeId
	 * @param string $routingKey
	 * @return boolean
	 */
	public static function bindUserQueue($config, $userId, $name, $exchangeId, $routingKey = "*")
	{
		$storage = new Storage\Queue($config);
		$fullName = self::generateUserQueueName($userId, $name);
		if (!($queue = $storage->getByName($fullName))) {
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
	 * @param mixed $config
	 * @param integer $userId
	 * @param string $name
	 * @param integer $exchangeId
	 * @param string $routingKey
	 * @return boolean
	 */
	public static function unbindUserQueue($config, $userId, $name, $exchangeId, $routingKey = "*")
	{
		$storage = new Storage\Queue($config);
		$fullName = self::generateUserQueueName($userId, $name);
		if (!($queue = $storage->getByName($fullName))) {
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
     * Delete user queue
     *
     * @param mixed $config
     * @param integer $userId
     * @param string $name
     * @param integer $exchangeId
     * @param string $routingKey
     * @return boolean
     */
    public static function deleteUserQueue($config, $userId, $name)
    {
        $storage = new Storage\Queue($config);
        $fullName = self::generateUserQueueName($userId, $name);
        $queue = $storage->getByName($fullName);
        $userQueueExchangeRouters = $storage->getQueueRouters($queue['id']);
        foreach ($userQueueExchangeRouters as $queueExchangeRouter) {
            if (!static::unbindUserQueue($config, $userId, $name, $queueExchangeRouter['exchange_id'], $queueExchangeRouter['router_key'])) {
                //return false;
            }
        }

        $queue = new self($fullName, $config);
        if (!$queue->delete()) {
            return false;
        }

        return $storage->removeQueue($queue['id']);
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