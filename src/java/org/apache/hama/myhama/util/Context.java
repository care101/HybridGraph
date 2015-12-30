/**
 * Termite System
 * copyright 2012-2010
 */
package org.apache.hama.myhama.util;

import org.apache.hama.Constants.VBlockUpdateRule;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.comm.CommRouteTable;

/**
 * Context defines some functions for users to implement 
 * their own algorithms. 
 * Context includes the following variables:
 * (1) {@link BSPJob};
 * (2) {@link GraphRecord};
 * (3) superstepCounter;
 * (4) message;
 * (5) jobAgg;
 * (6) vAgg;
 * (7) iteStyle;
 * (8) taskId;
 * (9) vBlockId;
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class Context<V, W, M, I> {
	protected BSPJob job;      //task level
	protected int taskId;      //task level
	protected int iteNum;      //superstep level
	protected int iteStyle;    //superstep level
	protected int vBlkId;                    //VBlock id
	protected VBlockUpdateRule vBlkUpdRule;  //VBlock level
	protected CommRouteTable<V, W, M, I> commRT;
	
	//variables associated with each vertex
	protected GraphRecord<V, W, M, I> graph;
	protected MsgRecord<M> msg;
	protected float jobAgg;
	@SuppressWarnings("unused")
	protected float vAgg;
	@SuppressWarnings("unused")
	protected boolean actFlag;
	@SuppressWarnings("unused")
	protected boolean resFlag;
	
	public Context(int _taskId, BSPJob _job, int _iteNum, int _iteStyle, 
			final CommRouteTable<V, W, M, I> _commRT) {
		this.taskId = _taskId;
		this.job = _job;
		this.iteNum = _iteNum;
		this.iteStyle = _iteStyle;
		this.commRT = _commRT;
		this.vBlkId = -1;
		this.vBlkUpdRule = VBlockUpdateRule.MSG_DEPEND;
	}
	
	/**
	 * Get a {@link GraphRecord}.
	 * It only makes sense for {@link BSP}.update() and getMessages(). 
	 * 
	 * In addition, variables in {@link GraphRecord}, 
	 * including vertex id and edges are always read-only. 
	 * On the other hand, the vertex value is read-write 
	 * in {@link BSP}.update(), but read-only in getMessages(). 
	 * 
	 * In particular, in {@link BSP}.getMessages(), the edge set 
	 * may be different. Specifically, when running style.Pull, 
	 * the returned {@link GraphRecord} only maintains edges in one 
	 * fragment. By contrast, for style.Push, it keeps the whole edges 
	 * of this vertex.
	 * @return
	 */
	public GraphRecord<V, W, M, I> getGraphRecord() {
		return graph;
	}
	
	/**
	 * Get a read-only message sent to one vertex.
	 * If messages are accumulated, all of the received messages 
	 * will be combined into a single one. 
	 * Otherwise, all message values will be 
	 * concatenated and packaged in the returned {@link MsgRecord}.
	 * Return null if no messages are received.
	 * This function only makes sense for {@link BSP}.update().
	 * @return
	 */
	public final MsgRecord<M> getReceivedMsgRecord() {
		return msg;
	}
	
	/**
	 * Get the read-only global aggregator value calculated 
	 * at the previous superstep. 
	 * Now, HybridGraph only provides a simple sum-aggregator. 
	 * The returned value only makes sense in 
	 * {@link BSP}.superstepSetup()/Cleanup(), vBlockSetup/Cleanup(), 
	 * and update().
	 * @return
	 */
	public final float getJobAgg() {
		return jobAgg;
	}
	
	/**
	 * Get the read-only superstep counter.
	 * It makes sense in {@link BSP}.superstepSetup()/Cleanup(), 
	 * vBlockSetup()/Cleanup(), update(), and getMessages().
	 * The counter starts from 1.
	 * @return
	 */
	public final int getSuperstepCounter() {
		return iteNum;
	}
	
	/**
	 * Get the iteration style of the current superstep.
	 * "style" is either {@link Constants}.STYLE.Pull or Push.
	 * Read-only.
	 * It makes sense in {@link BSP}.superstepSetup()/Cleanup(), 
	 * vBlockSetup()/Cleanup(), update(), and getMessages().
	 * @return
	 */
	public final int getIteStyle() {
		return iteStyle;
	}
	
	/**
	 * Get the read-only {@link BSPJob} object which 
	 * keeps configuration information 
	 * and static global statics of this job.
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
	 * Get the read-only VBlock id.
	 * It makes sense in {@link BSP}.vBlockSetup()/Cleanup() and update(). 
	 * @return
	 */
	public final int getVBlockId() {
		return this.vBlkId;
	}
	
	/**
	 * Return the id of the VBlock which the vid belongs to. 
	 * @param vid
	 * @return
	 */
	public final int getVBlockId(int vid) {
		return this.commRT.getDstLocalBlkIdx(this.taskId, vid);
	}
	
	/**
	 * Users invoke this function to indicate that 
	 * this vertex should send messages to its neighbors.
	 * This function only makes sense in {@link BSP}.update().
	 */
	public void setRespond() {
		this.resFlag = true;
	}
	
	/**
	 * A vertex votes to halt to indicate that it should not be 
	 * updated at the next superstep, i.e., inactive. 
	 * Note that the inactive vertex will become active again if it 
	 * receives messages at the next superstep.
	 * If all vertices become inactive, the iteration will 
	 * be terminated. 
	 * This function only makes sense in {@link BSP}.update().
	 */
	public void voteToHalt() {
		this.actFlag = false;
	}
	
	/**
	 * Set the sum-aggregator value based on this vertex.
	 * {@link JobInProgress} will sum up the values of all vertices 
	 * and then send the global aggregator value to all {@link BSPTask} 
	 * to be used at the next superstep by invoking getJobAgg().
	 * This function only makes sense in {@link BSP}.update().
	 * @param agg
	 */
	public void setVertexAgg(float agg) {
		this.vAgg = agg;
	}
	
	/**
	 * Set the update rule for one VBlock.
	 * This function only makes sense in {@link BSP}.vBlockSetup().
	 * @param rule
	 */
	public void setVBlockUpdateRule(VBlockUpdateRule rule) {
		this.vBlkUpdRule = rule;
	}
}