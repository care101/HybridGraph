/**
 * NeuSoft Termite System
 * copyright 2011-2016
 */
package org.apache.hama.bsp;

import org.apache.hama.Constants.Opinion;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.util.GraphContext;
import org.apache.hama.myhama.util.SuperStepContext;
import org.apache.hama.myhama.util.TaskContext;

/**
 * Interface BSP defines the basic operations needed to 
 * implement the BSP algorithm.
 * 
 * @author zhigang wang
 * @version 0.1
 */
public interface BSPInterface {
	/**
	 * Setup befor executing the user-defined algorithm.
	 * This function will be invoked by framework only once during the whole task.
	 * @param context
	 */
	public void taskSetup(TaskContext context);
	
	/**
	 * Setup befor executing the user-defined algorithm.
	 * This function will be invoked by framework only once during one superstep.
	 * @param job
	 */
	public void superstepSetup(SuperStepContext context);
	
	/**
	 * Judge this bucket weather need to be processed.
	 * This function will be invoked for every Bucket in every SuperStep.
	 * @param bucketId
	 * @param currentSuperStepCounter
	 * @return
	 */
	public Opinion processThisBucket(int bucketId, int currentSuperStepCounter);
	
	/**
	 * User-defined algorithm to process every Vertex.
	 * This function must be implemented by user.
	 * This function will be invoked for every {@link GraphRecord} 
	 * in every SuperStep.
	 * @throws Exception
	 */
	public void compute(GraphContext context) throws Exception;
	
	/**
	 * Cleanup after executing the user-defined algorithm.
	 * This function will be invoked by framework only once during one superstep.
	 * @param context
	 */
	public void superstepCleanup(SuperStepContext context);
	
	/**
	 * Cleanup after executing the user-defined algorithm.
	 * This function will be invoked by framework only once during the whole job.
	 * @param context
	 */
	public void taskCleanup(TaskContext context);
}
