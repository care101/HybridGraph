/**
 * copyright 2011-2016
 */
package org.apache.hama.myhama.api;

import org.apache.hama.Constants.Opinion;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.util.GraphContextInterface;

/**
 * Interface BSP defines the basic operations needed to 
 * implement the BSP algorithm.
 * 
 * @author 
 * @version 0.1
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public interface BSPInterface<V, W, M, I> {
	/**
	 * Setup befor executing the user-defined algorithm.
	 * This function will be invoked by framework only once during the whole task.
	 * @param context
	 */
	public void taskSetup(GraphContextInterface<V, W, M, I> context);
	
	/**
	 * Setup befor executing the user-defined algorithm.
	 * This function will be invoked by framework only once during one superstep.
	 * @param job
	 */
	public void superstepSetup(GraphContextInterface<V, W, M, I> context);
	
	/**
	 * Does this bucket need to be processed?
	 * This function will be invoked for every Bucket in every SuperStep.
	 * @param bucketId
	 * @param currentSuperStepCounter
	 * @return
	 */
	public Opinion processThisBucket(int bucketId, int currentSuperStepCounter);
	
	/**
	 * A user-defined function used to update the value of 
	 * an active vertex.
	 * 
	 * @throws Exception
	 */
	public void update(GraphContextInterface<V, W, M, I> context) 
			throws Exception;
	
	/**
	 * A user-defined function used to generate messages 
	 * sent from the source vertex to its neighbors. 
	 * Note: the edge collection may be varied with the value 
	 * fo context.getIteStyle().
	 * 
	 * @param context
	 * @return
	 * @throws Exception
	 */
	public MsgRecord<M>[] getMessages(GraphContextInterface<V, W, M, I> context) 
			throws Exception;
	
	/**
	 * Cleanup after executing the user-defined algorithm.
	 * This function will be invoked by framework only once during one superstep.
	 * @param context
	 */
	public void superstepCleanup(GraphContextInterface<V, W, M, I> context);
	
	/**
	 * Cleanup after executing the user-defined algorithm.
	 * This function will be invoked by framework only once during the whole job.
	 * @param context
	 */
	public void taskCleanup(GraphContextInterface<V, W, M, I> context);
}
