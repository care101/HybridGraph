/**
 * copyright 2011-2016
 */
package org.apache.hama.myhama.api;

import org.apache.hama.Constants.VBlockUpdateRule;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.util.Context;

/**
 * BSPInterface defines the basic operations needed to 
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
	 * Setup befor running this task.
	 * Do nothing as default.
	 * @param context
	 */
	public void taskSetup(Context<V, W, M, I> context);
	
	/**
	 * Setup befor starting a new superstep.
	 * Do nothing as default.
	 * @param context
	 */
	public void superstepSetup(Context<V, W, M, I> context);
	
	/**
	 * Setup before processing vertices in one VBlock.
	 * @param context
	 * @return
	 */
	public void vBlockSetup(Context<V, W, M, I> context);
	
	/**
	 * A vertex-centric function for updating a vertex value.
	 * It must be implemented by users.
	 * @throws Exception
	 */
	public void update(Context<V, W, M, I> context) 
			throws Exception;
	
	/**
	 * A vertex-centric function for generating messages 
	 * sent from one vertex to its neighbors if this vertex 
	 * is set as "respond" via context.setRespond() in update(). 
	 * This function is invoked to either respond pulling requests 
	 * if the vertex is set as "respond" at the previous superstep 
	 * when running style.Pull, or immediately generate messages 
	 * after the vertex is just updated in update() at the current superstep 
	 * when running style.Push. 
	 * This fuction must be implemented by users.
	 * @param context
	 * @return
	 * @throws Exception
	 */
	public MsgRecord<M>[] getMessages(Context<V, W, M, I> context) 
			throws Exception;
	
	/**
	 * Cleanup after processing vertices in one VBlock.
	 * Do nothing as default.
	 * @param context
	 */
	public void vBlockCleanup(Context<V, W, M, I> context);
	
	/**
	 * Cleanup after accomplishing one superstep.
	 * Do nothing as default.
	 * @param context
	 */
	public void superstepCleanup(Context<V, W, M, I> context);
	
	/**
	 * Cleanup after accomplishing this task.
	 * Do nothing as default.
	 * @param context
	 */
	public void taskCleanup(Context<V, W, M, I> context);
}
