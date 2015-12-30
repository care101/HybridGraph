/**
 * Termite System
 * copyright 2012-2010
 */
package org.apache.hama.myhama.util;

import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;

/**
 * Context defines some functions for updating vertex values 
 * used in {@link BSPInterface}.update(). 
 * Context includes the following variables:
 * (1) {@link BSPJob};
 * (2) {@link GraphRecord};
 * (3) superstepCounter;
 * (4) message;
 * (5) jobAgg;
 * (6) vAgg;
 * (7) iteStyle;
 * (8) taskId;
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class Context<V, W, M, I> {
	protected int taskId;
	protected BSPJob job;
	protected GraphRecord<V, W, M, I> graph;
	protected int iteNum;
	protected MsgRecord<M> msg;
	protected float jobAgg;
	@SuppressWarnings("unused")
	protected float vAgg;
	
	@SuppressWarnings("unused")
	protected boolean actFlag;
	@SuppressWarnings("unused")
	protected boolean resFlag;
	
	protected int iteStyle;
	
	public Context(int _taskId, BSPJob _job, int _iteNum, int _iteStyle) {
		this.taskId = _taskId;
		this.job = _job;
		this.iteNum = _iteNum;
		this.iteStyle = _iteStyle;
	}
	
	/**
	 * Get a vertex which should be updated at this superstep.
	 * Read-write.
	 * @return
	 */
	public GraphRecord<V, W, M, I> getGraphRecord() {
		return graph;
	}
	
	/**
	 * Get the message sent to this vertex.
	 * If messages are accumulated, all of the received messages 
	 * will be combined into a single one. 
	 * Otherwise, all message values will be 
	 * concatenated and packaged in the returned {@link MsgRecord}.
	 * Read-only.
	 * @return
	 */
	public final MsgRecord<M> getReceivedMsgRecord() {
		return msg;
	}
	
	/**
	 * Get the global aggregator value calculated 
	 * at the previous superstep. 
	 * Now, HybridGraph only provides a simple sum-aggregator.
	 * Read-only.
	 * @return
	 */
	public final float getJobAgg() {
		return jobAgg;
	}
	
	/**
	 * Get the superstep counter.
	 * Read-only.
	 * @return
	 */
	public final int getIteCounter() {
		return iteNum;
	}
	
	/**
	 * Get the iteration style in the current superstep.
	 * "style" is either {@link Constants}.STYLE.Pull or Push.
	 * Read-only.
	 * @return
	 */
	public final int getIteStyle() {
		return iteStyle;
	}
	
	/**
	 * Get the read-only {@link BSPJob} object which 
	 * keeps configuration information and static global statics of this job.
	 * Read-only.
	 * @return
	 */
	public final BSPJob getBSPJobInfo() {
		return job;
	}
	
	/**
	 * Get the task id. Read-only.
	 * @return
	 */
	public final int getTaskId() {
		return taskId;
	}
	
	/**
	 * Users invoke this function to indicate that 
	 * this vertex should send messages to its neighbors.
	 */
	public void setRespond() {
		this.resFlag = true;
	}
	
	/**
	 * Vote to halt. 
	 * If all vertices vote to halt, the iteration will 
	 * be terminated.
	 */
	public void voteToHalt() {
		this.actFlag = false;
	}
	
	/**
	 * Set the sum-aggregator value based on this vertex.
	 * {@link JobInProgress} will sum up the values of all vertices 
	 * and then send the global aggregator value to all {@link BSPTask} 
	 * at the next superstep.
	 * 
	 * @param agg
	 */
	public void setVertexAgg(float agg) {
		this.vAgg = agg;
	}
}