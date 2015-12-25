/**
 * Termite System
 * copyright 2012-2010
 */
package org.apache.hama.myhama.util;

import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;

/**
 * GraphContext.
 * This class contains information about a {@link GraphRecord}, including:
 * (1) {@link GraphRecord};
 * (2) superstepCounter;
 * (3) message;
 * (4) jobAgg;
 * (5) vAgg;
 * (6) iteStyle;
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class GraphContext<V, W, M, I> 
		implements GraphContextInterface<V, W, M, I> {
	private GraphRecord<V, W, M, I> graph;
	private int iteNum;
	private MsgRecord<M> msg;
	private float jobAgg;
	private float vAgg;
	
	private boolean actFlag;
	private boolean resFlag;
	
	private int iteStyle;
	
	public GraphContext() {
		
	}
	
	public void initialize(GraphRecord<V, W, M, I> _graph, int _iteNum, 
			MsgRecord<M> _msg, 
			float _jobAgg, boolean _actFlag, int _iteStyle) {
		graph = _graph; 
		msg = _msg; 
		iteNum = _iteNum; 
		jobAgg = _jobAgg;
		actFlag = _actFlag;
		iteStyle = _iteStyle;
	}
	
	public GraphRecord<V, W, M, I> getGraphRecord() {
		return graph;
	}
	
	public MsgRecord<M> getReceivedMsgRecord() {
		return msg;
	}
	
	public float getJobAgg() {
		return jobAgg;
	}
	
	public int getIteCounter() {
		return iteNum;
	}
	
	public int getIteStyle() {
		return iteStyle;
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
