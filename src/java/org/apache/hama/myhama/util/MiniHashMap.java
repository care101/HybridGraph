package org.apache.hama.myhama.util;

/**
 * A Mini HashMap, it supports <key> = int and <value> = int, and <key> is an unique int number.
 * Key and value is positive int numbers(>=0).
 * The Mini HashMap can not adjust its capacity automatically.
 * @author 
 * @version 0.1
 */
public class MiniHashMap {
	
	private int capacity = 100;
	private int length = 0;
	private int[] values;
	private int[] checks;
	private boolean[] exists;
	
	/**
	 * Construct the {@link MiniHashMap} with the default capacity of 100.
	 */
	public MiniHashMap() {
		this.values = new int[this.capacity];
		this.checks = new int[this.capacity];
		this.exists = new boolean[this.capacity];
	}
	
	/**
	 * Construct the {@link MiniHashMap} with the given capacity.
	 * @param capacity the capacity of {@link MiniHashMap} 
	 */
	public MiniHashMap(int capacity) {
		this.capacity = capacity;
		this.values = new int[this.capacity];
		this.checks = new int[this.capacity];
		this.exists = new boolean[this.capacity];
	}
	
	/**
	 * Get the current length of the {@link MiniHashMap}
	 * @return
	 */
	public int size() {
		return this.length;
	}
	
	/**
	 * If the given key is existed in the {@link MiniHashMap},
	 * return its position, else return -1.
	 * @param key
	 * @return
	 */
	public int containsKey(int key) {
		int pos = key % this.capacity, start = pos;
		int check = key / this.capacity;
		int result = -1;
		
		while (this.exists[pos]) {
			if (this.checks[pos] == check) {
				result = pos;
				break;
			} else {
				pos = (pos + 1) % this.capacity;
				if (pos == start) {
					break;
				}
			}
		}
		
		return result;
	}
	
	/**
	 * Put the new <key-value> pair into the {@link MiniHashMap}.
	 * If insert successfully, return true, else return false.
	 * @param key
	 * @param value
	 */
	public boolean put(int key, int value) {
		int pos = key % this.capacity, start = pos;
		int check = key / this.capacity;
		boolean flag = true;
		
		while (this.exists[pos]) {
			if (this.checks[pos] == check) {
				flag = false;
				break;
			} else {
				pos = (pos + 1) % this.capacity;
				if (pos == start) {
					flag = false;
					break;
				}
			}
		}
		
		if (flag) {
			this.values[pos] = value;
			this.exists[pos] = true;
			this.length++;
		}
		
		return flag;
	}
	
	/**
	 * Search the value by the given position.
	 * @param key
	 * @return
	 */
	public int getValue(int position) {
		return this.values[position];
	}
	
	/**
	 * Set the value by the given position
	 * @param position
	 * @param value
	 */
	public void setValue(int position, int value) {
		this.values[position] = value;
	}
	
	/**
	 * Get the values array.
	 * Note, the length of array is equal with {@link MiniHashMap}'s capacity,
	 * while, the real length must be gotten by .size() function.
	 * @return
	 */
	public int[] values() {
		return this.values;
	}
	
	public boolean[] getExists() {
		return this.exists;
	}
}
