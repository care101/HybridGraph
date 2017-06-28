/**
 * Termite System
 * copyright 2012-2010
 */
package org.apache.hama.myhama.util;

import org.apache.hama.Constants;
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
	protected Constants.STYLE iteStyle;    //superstep level
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
	/** (Out-)Degree */
	protected int degree;
	
	/**
	 * Whether or not to load edges when performing 
	 * {@link BSPInterface}.update() and .getMessages() 
	 * at a given superstep. The value is valid only for 
	 * style.PUSH. User can specify its value in the 
	 * function {@link BSPInterface}.superstepSetup(). 
	 * True as default. Note that if the specified value 
	 * collides with that set by {@link BSPJob}.useAdjEdgeInUpdate(), 
	 * the former finally works.
	 */
	protected boolean useEdgesInPush;
	
	public Context(int _taskId, BSPJob _job, int _iteNum, Constants.STYLE _iteStyle, 
			final CommRouteTable<V, W, M, I> _commRT) {
		this.taskId = _taskId;
		this.job = _job;
		this.iteNum = _iteNum;
		this.iteStyle = _iteStyle;
		this.commRT = _commRT;
		this.vBlkId = -1;
		this.vBlkUpdRule = VBlockUpdateRule.MSG_DEPEND;
		this.useEdgesInPush = true;
	}
	
	/**
	 * Get a {@link GraphRecord}.
	 * It only makes sense for {@link BSP}.update() and getMessages(). 
	 * 
	 * Variables in {@link GraphRecord}, including vertex id and edges, 
	 * are always read-only. Differently, vertex value is read-write 
	 * in {@link BSP}.update(), but read-only in getMessages(). 
	 * 
	 * The edge set provided in {@link BSP}.getMessages() may vary with 
	 * models. For example, when running style.Pull, only edges in one 
	 * fragment are contained. For style.Push, all outgoing edges are 
	 * provided if "flag=true" in setUseEdgesInPush(flag) (true as default). 
	 * Otherwise, i.e., "flag=false" in setUseEdgesInPush(flag), the edge 
	 * set is null.
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
	public final Constants.STYLE getIteStyle() {
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
	 * Degree or out-degree for this vertex. 
	 * Note that the number of edges in {@link GraphRecord} may 
	 * not equal (out-)degree because the former may not initialize 
	 * its edges.
	 * @return
	 */
	public final int getDegree() {
		return degree;
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
	 * Users invoke this function to indicate that this vertex 
	 * should send messages to its (non-)neighboring vertices.
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
	
	/**
	 * Whether or not to load edges when performing 
	 * {@link BSPInterface}.update() and .getMessages() 
	 * at a given superstep. The value is valid only for style.PUSH. 
	 * User is recommended to specify its value in the function 
	 * {@link BSPInterface}.superstepSetup(). True as default. 
	 * Note that if the specified value collides with that set by 
	 * {@link BSPJob}.useAdjEdgeInUpdate(), the former finally works.
	 */
	public void setUseEdgesInPush(boolean flag) {
		this.useEdgesInPush = flag;
	}
}