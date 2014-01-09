<?php
namespace QueueCenter\Storage;

/**
 * Interface AdapterInterface
 *
 * @category Storage
 * @package QueueCenter
 */
interface AdapterInterface
{
	/**
	 * Return data from database by params
	 * @param array $params
	 * @return array
	 */
	public function get(array $params);
	
	/**
	 * Add new data to database
	 * 
	 * @param array $params
	 * @return boolean
	 */
	public function add(array $params);
	
	/**
	 * Remove data from database by id
	 * 
	 * @param integer $id
	 * @return bollean
	 */
	public function remove($id);
}