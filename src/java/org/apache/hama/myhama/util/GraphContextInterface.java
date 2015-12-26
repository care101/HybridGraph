/**
 * Termite System
 * copyright 2012-2010
 */
package org.apache.hama.myhama.util;

import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.api.GraphRecordInterface;
import org.apache.hama.myhama.api.MsgRecordInterface;

/**
 * GraphContextInterface defines interfaces available for users.
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public interface GraphContextInterface<V, W, M, I> {
	
	/**
	 * Get a vertex which should be updated at this superstep.
	 * Read-write.
	 * @return
	 */
	public GraphRecordInterface<V, W, M, I> getGraphRecord();
	
	/**
	 * Get the message sent to this vertex.
	 * If messages are accumulated, all of the received messages 
	 * will be combined into a single one. 
	 * Otherwise, all message values will be 
	 * concatenated and packaged in the returned {@link MsgRecord}.
	 * Read-only.
	 * @return
	 */
	public MsgRecordInterface<M> getReceivedMsgRecord();
	
	/**
	 * Get the global aggregator value calculated 
	 * at the previous superstep. 
	 * Now, HybridGraph only provides a simple sum-aggregator.
	 * Read-only.
	 * @return
	 */
	public float getJobAgg();
	
	/**
	 * Get the superstep counter.
	 * Read-only.
	 * @return
	 */
	public int getIteCounter();
	
	/**
	 * Get the iteration style in the current superstep.
	 * "style" is either {@link Constants}.STYLE.Pull or Push.
	 * Read-only.
	 * @return
	 */
	public int getIteStyle();
	
	/**
	 * Get the read-only {@link BSPJob} object which 
	 * keeps configuration information and static global statics of this job.
	 * Read-only.
	 * @return
	 */
	public BSPJob getBSPJobInfo();
	
	/**
	 * Get the task id. Read-only.
	 * @return
	 */
	public int getTaskId();
	
	/**
	 * Set the sum-aggregator value based on this vertex.
	 * {@link JobInProgress} will sum up the values of all vertices 
	 * and then send the global aggregator value to all {@link BSPTask} 
	 * at the next superstep.
	 * 
	 * @param agg
	 */
	public void setVertexAgg(float agg);
	
	/**
	 * Users invoke this function to indicate that 
	 * this vertex should send messages to its neighbors.
	 */
	public void setRespond();
	
	/**
	 * Vote to halt. 
	 * If all vertices vote to halt, the iteration will 
	 * be terminated.
	 */
	public void voteToHalt();
}
