<?php
namespace QueueCenter\Adapter;

use PhpAmqpLib\Connection as Connection,
	PhpAmqpLib\Message as Message;

/**
 * Class RabbitMQ
 *
 * @category   Adapter
 * @package    QueueCenter
 */
class RabbitMQ implements QueueInterface
{
	/**
	 * AMQP connection
	 * @var \PhpAmqpLib\Connection\AMQPConnection
	 */
	private $_connection;
	
	/**
	 * AMQP connection channel
	 * @var \PhpAmqpLib\Channel\AMQPChannel
	 */
	private $_channel;
	
	/**
	 * 
	 * @param array|\PhpAmqpLib\Connection\AbstractConnection $connection
	 */
	public function __construct($connection)
	{
		if ((array) $connection == $connection) {
			if (!isset($connection['type'])) {
				$connection['type']  = 'lazy';
			}
			$type = $connection['type'];
			unset($connection['type']);
			switch ($type) {
				case 'lazy':					
				default:
					$reflection = new \ReflectionClass('\PhpAmqpLib\Connection\AMQPLazyConnection');
					$this->_connection = $reflection->newInstanceArgs($connection);
					break;
			}
		} elseif ($connection instanceof \PhpAmqpLib\Connection\AbstractConnection) {
			$this->_connection = $connection;
		}
		
		$this->_channel = $this->_connection->channel();
	}

	/**
	 * Return queue connection 
	 * 
	 * @return \PhpAmqpLib\Connection\AMQPConnection
	 */
	public function getConnection()
	{
		return $this->_connection;
	}
	
	/**
	 * Return queue connection channel
	 * 
	 * @return \PhpAmqpLib\Channel\AMQPChannel
	 */
	public function getChannel()
	{
		return $this->_channel;
	}
	
	/**
	 * Declare new exchange
	 * 
	 * @param string $exchange
	 * @param string $type
	 * @param string $passive
	 * @param string $durable
	 * @param string $auto_delete
	 * @param string $internal
	 * @param string $nowait
	 * @param string $arguments
	 * @param string $ticket
	 */
	public function exchangeDeclare(
        $exchange,
        $type,
        $passive = false,
        $durable = false,
        $auto_delete = true,
        $internal = false,
        $nowait = false,
        $arguments = null,
        $ticket = null
    ) {
		return $this->_channel->exchange_declare($exchange, $type, $passive, $durable, $auto_delete, $internal, $nowait, $arguments, $ticket);
	}
	
	/**
	 * Publish new message
	 * 
	 * @param string $msg
	 * @param string $exchange
	 * @param string $routingKey
	 * @param string $mandatory
	 * @param string $immediate
	 * @param string $ticket
	 */
	public function exchangePublish(
        $message,
        $exchange,
        $routingKey = "*",
        $mandatory = false,
        $immediate = false,
        $ticket = null
    ) {
		$msg = new Message\AMQPMessage($message, ['content_type' => 'text/plain', 'delivery_mode' => 2]);
		return $this->_channel->basic_publish($msg, $exchange, $routingKey, $mandatory, $immediate,  $ticket);
	}
	
	/**
	 * Remove user exchange
     *
	 * @param string $exchange
	 */
	public function exchangeDelete($exchange) 
	{
		return $this->_channel->exchange_delete($exchange);
	}
	
	/**
	 * Bind user exchange
     *
	 * @param string $destination
	 * @param string $source
	 * @param string $routingKey
	 * @param string $nowait
	 * @param string $arguments
	 * @param string $ticket
	 */
	public function exchangeBind(
        $destination,
        $source,
        $routingKey = "*",
        $nowait = false,
        $arguments = null,
        $ticket = null
    ) {
		return $this->_channel->exchange_bind($destination, $source, $routingKey, $nowait, $arguments, $ticket);
	}
	
	/**
	 * Unbind user exchange
     *
	 * @param string $source
	 * @param string $destination
	 * @param string $routingKey
	 * @param string $arguments
	 * @param string $ticket
	 */
	public function exchangeUnbind(
        $source,
        $destination,
        $routingKey = "*",
        $arguments = null,
        $ticket = null
    ) {
		return $this->_channel->exchange_unbind($source, $destination, $routingKey, $arguments, $ticket);
	}

    /**
     * Declare queue
     *
     * @param string $queue
     * @param bool $passive
     * @param bool $durable
     * @param bool $exclusive
     * @param bool $auto_delete
     * @param bool $nowait
     * @param null $arguments
     * @param null $ticket
     * @return mixed
     */
    public function queueDeclare(
        $queue,
        $passive = false,
        $durable = false,
        $exclusive = false,
        $auto_delete = true,
        $nowait = false,
        $arguments = null,
        $ticket = null
    ) {
		return $this->_channel->queue_declare($queue, $passive, $durable, $exclusive, $auto_delete, $nowait,  $arguments, $ticket);
	}
	
	/**
	 * Bind queue to user exchange
     *
	 * @param string $queue
	 * @param string $exchange
	 * @param string $routingKey
	 * @param string $nowait
	 * @param string $arguments
	 * @param string $ticket
	 */
	public function queueBind(
        $queue,
        $exchange,
        $routingKey = "*",
        $nowait = false,
        $arguments = null,
        $ticket = null
    ) {
		return $this->_channel->queue_bind($queue, $exchange, $routingKey, $nowait, $arguments, $ticket);
	}
	
	/**
	 * Unbind queue from user exchange
     *
	 * @param string $queue
	 * @param string $exchange
	 * @param string $routingKey
	 * @param string $arguments
	 * @param string $ticket
	 */
	public function queueUnBind(
        $queue,
        $exchange,
        $routingKey = "*",
        $arguments = null,
        $ticket = null
    ) {
		return $this->_channel->queue_unbind($queue, $exchange, $routingKey, $arguments, $ticket);
	}
	
	/**
	 * Delete queue
     *
	 * @param string $queue
	 * @param string $if_unused
	 * @param string $if_empty
	 * @param string $nowait
	 * @param string $ticket
	 */
	public function queueDelete(
        $queue,
        $if_unused = false,
        $if_empty = false,
        $nowait = false,
        $ticket = null
    ) {
		return $this->_channel->queue_delete($queue, $if_unused, $if_empty, $nowait, $ticket);
	}
	
	/**
	 * Consume queue
     *
	 * @param string $queue
	 * @param string $consumer_tag
	 * @param string $no_local
	 * @param string $no_ack
	 * @param string $exclusive
	 * @param string $nowait
	 * @param string $callback
	 * @param string $ticket
	 */
	public function queueConsume(
        $queue,
        $consumer_tag = "",
        $no_local = false,
        $no_ack = false,
        $exclusive = false,
        $nowait = false,
        $callback = null,
        $ticket = null
    ) {
		return $this->_channel->basic_consume($queue, $consumer_tag, $no_local, $no_ack, $exclusive, $nowait, $callback, $ticket);
	}
	
	/**
	 * Get message from queue
     *
	 * @param string $queue
	 * @param string $no_ack
	 * @param string $ticket
     * @return
	 */
	public function queueGet($queue, $no_ack = false, $ticket = null)
	{
		return $this->_channel->basic_get($queue, $no_ack, $ticket);
	}
	
	/**
	 * 
	 * @param string $delivery_tag
	 * @param string $multiple
	 */
	public function queueAck($delivery_tag, $multiple = false)
	{
		return $this->_channel->basic_ack($delivery_tag, $multiple);
	}

	/**
	 * Purge queue
     *
	 * @param string $queue
	 * @param string $nowait
	 * @param string $ticket
	 */
	public function queuePurge($queue, $nowait = false, $ticket = null)
	{
		return $this->_channel->queue_purge($queue, $nowait, $ticket);
	}
	
	/**
	 * Close active channel and connection
	 */
	public function __destruct()
	{
		if ($this->_channel) {
			$this->_channel->close();
		}
		if ($this->_connection) {
			$this->_connection->close();
		}
	}
}