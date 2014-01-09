<?php
namespace QueueCenter\Queue;

/**
 * Interface HandlerCallbackInterface
 *
 * @category Queue
 * @package QueueCenter
 */
interface HandlerCallbackInterface
{
	/**
	 * Handle data by params
     * $data[
     *  publish - timestamp publish new message to exchange
     *  handler - queue message handler name
     *  message - exchange message
     *  queue_id - queue identifier key in database
     *  user_id - user identifier key in database
     *  exchange - exchange name
     *  routing_key - message routing key
     * ]
	 * 
	 * @param array $data
	 * @return boolean
	 */
	public function handle(array $data);
}