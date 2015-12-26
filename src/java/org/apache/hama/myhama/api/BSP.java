/**
 * Termite System
 * copyright 2011-2016
 */
package org.apache.hama.myhama.api;

import org.apache.hama.Constants.Opinion;
import org.apache.hama.myhama.util.GraphContextInterface;

/**
 * This class provides an abstract implementation of the BSP interface. 
 * Users can define their own computing logics by implementing 
 * or extending functions in {@link BSP}.
 * 
 * @author 
 * @version 0.1
 */
public abstract class BSP<V, W, M, I> implements BSPInterface<V, W, M, I> {
	
	@Override
	public void taskSetup(GraphContextInterface<V, W, M, I> context) {
		// TODO The default function will do nothing.
	}
	
	
	@Override
	public void superstepSetup(GraphContextInterface<V, W, M, I> context) {
		// TODO The default function will do nothing.
	}
	
	/**
	 * Does this bucket need to be processed?
	 * This function will be invoked for every Bucket in every SuperStep.
	 * The default function will return {@link Opinion.NONE} for every Bucket.
	 * That means the bucket will not be processed 
	 * if there is no messages sent for it.
	 * @param bucketId
	 * @param currentSuperStepCounter
	 * @return
	 */
	@Override
	public Opinion processThisBucket(int bucketId, 
			int currentSuperStepCounter) {
		return Opinion.MSG_DEPEND;
	}
	
	@Override
	public void superstepCleanup(GraphContextInterface<V, W, M, I> context) {
		// TODO The default function will do nothing.
	}
	
	@Override
	public void taskCleanup(GraphContextInterface<V, W, M, I> context) {
		// TODO The default function will do nothing.
	}
}

