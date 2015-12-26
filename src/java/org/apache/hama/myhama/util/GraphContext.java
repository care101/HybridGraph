/**
 * Termite System
 * copyright 2012-2010
 */
package org.apache.hama.myhama.util;

import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.api.GraphRecordInterface;
import org.apache.hama.myhama.api.MsgRecordInterface;

/**
 * GraphContext.
 * This class contains information about a {@link GraphRecord}, including:
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
public class GraphContext<V, W, M, I> 
		implements GraphContextInterface<V, W, M, I> {
	private int taskId;
	private BSPJob job;
	private GraphRecordInterface<V, W, M, I> graph;
	private int iteNum;
	private MsgRecordInterface<M> msg;
	private float jobAgg;
	private float vAgg;
	
	private boolean actFlag;
	private boolean resFlag;
	
	private int iteStyle;
	
	public GraphRecordInterface<V, W, M, I> getGraphRecord() {
		return graph;
	}
	
	public final MsgRecordInterface<M> getReceivedMsgRecord() {
		return msg;
	}
	
	public final float getJobAgg() {
		return jobAgg;
	}
	
	public final int getIteCounter() {
		return iteNum;
	}
	
	public final int getIteStyle() {
		return iteStyle;
	}
	
	public final BSPJob getBSPJobInfo() {
		return job;
	}
	
	public final int getTaskId() {
		return taskId;
	}
	
	public void setRespond() {
		this.resFlag = true;
	}
	
	public void voteToHalt() {
		this.actFlag = false;
	}
	
	public void setVertexAgg(float agg) {
		this.vAgg = agg;
	}
	
	
	//===================================================
	// Functions used by the core engine of HybridGraph
	//===================================================
	
	public GraphContext(int _taskId, BSPJob _job, int _iteNum, int _iteStyle) {
		this.taskId = _taskId;
		this.job = _job;
		this.iteNum = _iteNum;
		this.iteStyle = _iteStyle;
	}
	
	/**
	 * Initialize {@link GraphContext}.
	 * @param _graph
	 * @param _msg
	 * @param _jobAgg
	 * @param _actFlag
	 */
	public void initialize(GraphRecordInterface<V, W, M, I> _graph, 
			MsgRecordInterface<M> _msg, float _jobAgg, boolean _actFlag) {
		graph = _graph; 
		msg = _msg; 
		jobAgg = _jobAgg;
		actFlag = _actFlag;
	}
	
	/**
	 * Get the aggregator value from one vertex.
	 * @return
	 */
	public float getVertexAgg() {
		return this.vAgg;
	}
	
	public void reset() {
		this.actFlag = true;
		this.resFlag = false;
	}
	
	/**
	 * Does this vertex need to send messages to its neighbors?
	 * @return
	 */
	public boolean isRespond() {
		return this.resFlag;
	}
	
	/**
	 * Is this vertex still active at the next superstep?
	 * An active vertex means that it should be updated.
	 * Here, the active flag can be changed by invoking 
	 * voteToHalt(). However, an inactive vertex also 
	 * will be active if it receives messages from neighbors 
	 * at the next superstep.
	 * @return
	 */
	public boolean isActive() {
		return this.actFlag;
	}
}
