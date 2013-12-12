<?php
namespace QueueCenter\Queue;

interface HandlerCallbackInterface
{
	/**
	 * Handle data by params
	 * 
	 * @param array $params
	 * @return array
	 */
	public function handle(array $data);
}