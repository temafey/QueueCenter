<?php
namespace QueueCenter;

/**
 * Class Exchange
 *
 * @package QueueCenter
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
     * Exchange name prefix
     * @var string
     */
    protected $_prefix = null;
	
	/**
	 * Queue options
	 * @param array
	 */
	protected $_options = array(
		'passive' => false,
		'durable' => true,
		'auto_delete' => false,
		'internal' => false,
		'nowait' => false,
		'arguments' => null,
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
        
        if (isset($this->_config['exchangePrefix'])) {
            $this->_prefix = $this->_config['exchangePrefix'];
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
            if (isset($this->_config['exchangeType'])) {
                $this->_type = $this->_config['exchangeType'];
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
        $exchangeName = $this->getName();
		$this->getAdapter()->exchangeDeclare($exchangeName, $this->_type, $this->_options['passive'], $this->_options['durable'], $this->_options['auto_delete'], $this->_options['internal'], $this->_options['nowait'], $this->_options['arguments'], $this->_options['ticket']);
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
        $exchangeName = $this->getName();
		$this->getAdapter()->exchangePublish($message, $exchangeName, $routingKey, $mandatory = false, $immediate = false, $this->_options['ticket']);
				
		return $this;
	}

    /**
     * Return exchange name
     *
     * @return string
     */
    public function getName()
    {
        return ($this->_prefix) ? $this->_prefix."_".$this->_name : $this->_name;
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
	 * @param mixed $config
	 * @param integer $userId
	 * @param string $name
	 * @return boolean
	 */
	public static function addUserExchange($config, $userId, $name)
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
	 * @param mixed $config
	 * @param integer $userId
	 * @param string $name
	 * @param string|array|object $message
	 * @param string $handler
	 * @param string $routingKey
	 * @return boolean
	 */
	public static function publishToUserExchange($config, $userId, $name, $message, $handler, $routingKey = "*")
	{
		$storage = new Storage\Exchange($config);
		$fullName = self::generateUserExchangeName($userId, $name);
		if (!$storage->getByName($fullName)) {
			if (!self::addUserExchange($config, $userId, $name)) {
				return false;
			}
		}
		$exchange = new self($fullName, $config);		
		$exchange->publish(array('publish' => time(), 'handler' => $handler, 'message' => $message), $routingKey);

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