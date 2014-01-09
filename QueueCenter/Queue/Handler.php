<?php
namespace QueueCenter\Queue;

use \QueueCenter\Queue,
	\QueueCenter\Storage\Queue as Storage;

/**
 * Class Handler
 *
 * @category Queue
 * @package QueueCenter
 */
class Handler
{	
	CONST HANDLER_CALLBACK_PREFIX = 'handler';

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
	 * Return data handler
	 *
	 * @param string $type
	 * @return \QueueCenter\Queue\HandlerCallbackInterface
	 */
	public function getHandler($type)
	{
		$handler = $this->_config[self::HANDLER_CALLBACK_PREFIX.$this->_normalizeHandlerType(ucfirst($type))];
		if (!($handler instanceof HandlerCallbackInterface)) {
			throw new \Exception("Not valid QueueCenter message handler!");
		}
	
		return $handler;
	}
	
	/**
	 * Add handler callback
	 * 
	 * @param string $type
	 * @param \QueueCenter\Queue\HandlerCallbackInterface $handler
	 * @return \QueueCenter\Queue\Handler
	 */
	public function addHandler($type, \QueueCenter\Queue\HandlerCallbackInterface $handler)
	{
		$this->_config[self::HANDLER_CALLBACK_PREFIX.$this->_normalizeHandlerType(ucfirst($type))] = $handler;
		return $this;
	}
	
	/** 
	 * Normailize handler type
	 * 
	 * @param string $type
	 * @return string
	 */
	protected function _normalizeHandlerType($type) 
	{
		$words = explode('-', $type);
		$normalizeType = '';
		foreach ($words as $word) {
			$normalizeType .= ucfirst($word);
		}
		
		return  $normalizeType;
	}
	
	/**
	 * Handle queues
	 * 
	 * @param integer $userId
	 * @param integer $queueId
	 * @param integer $exchangeId
	 * @return boolean
	 */
	public function handle($userId = null, $queueId = null, $exchangeId = null)
	{
		$storage = new Storage($this->_config);
		if (null !== $queueId) {
			$queues = [$storage->getById($userId)];			
		} elseif (null !== $userId) {
			$queues = $storage->getUserQueues($userId);
		} elseif (null !== $exchangeId) {
			$queues = $storage->getExchangeQueues($exchangeId);
		} else {
            throw new \Exception('Not set any params for handle queues');
        }
		foreach ($queues as $queue) {
			$this->_consumeQueueMessages($queue);
		}
		
		return true;
	}
	
	/**
	 * Consume queue messages
	 * 
	 * @param array $queue
	 * @return void
	 */
	protected function _consumeQueueMessages($queue)
	{
		$queueAdapter = new Queue($queue['name'], $this->_config);
		while(($message = $queueAdapter->get())) {
			if ($this->_handle($queue['user_id'], $queue['id'], $message)) {
				$queueAdapter->ack($message->delivery_info['delivery_tag']);
			}
		}
	}
	
	/**
	 * Handle message
	 * 
	 * @param integer $userId
	 * @param integer $queueId
	 * @param \PhpAmqpLib\Wire\GenericContent\AMQPMessage $message
	 * @return boolean
	 */
	protected function _handle($userId, $queueId, $message)
	{
		$data = [];
		$data = unserialize($message->body);
		$data['user_id'] = $userId;
		$data['queue_id'] = $queueId;
		$data['exchange'] = $message->delivery_info['exchange'];
		$data['routing_key'] = $message->delivery_info['routing_key'];
		$handler = $this->getHandler($data['handler']);
		$result = $handler->handle($data);
		
		if ($result) {
			return true;
		}
		
		return false;
	}
	
}