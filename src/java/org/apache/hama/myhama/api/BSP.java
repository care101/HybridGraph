/**
 * NeuSoft Termite System
 * copyright 2011-2016
 */
package org.apache.hama.myhama.api;

import org.apache.hama.Constants.Opinion;
import org.apache.hama.bsp.BSPInterface;
import org.apache.hama.myhama.util.SuperStepContext;
import org.apache.hama.myhama.util.TaskContext;

/**
 * This class provides an abstract implementation of the BSP interface.
 * 
 * @author zhigang wang
 * @version 0.1
 */
public abstract class BSP implements BSPInterface {
	
	@Override
	public void taskSetup(TaskContext context) {
		// TODO The default function will do nothing.
	}
	
	@Override
	public void superstepSetup(SuperStepContext context) {
		// TODO The default function will do nothing.
	}
	
	/**
	 * Judge this bucket weather need to be processed.
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
	public void superstepCleanup(SuperStepContext context) {
		// TODO The default function will do nothing.
	}
	
	@Override
	public void taskCleanup(TaskContext context) {
		// TODO The default function will do nothing.
	}
}

