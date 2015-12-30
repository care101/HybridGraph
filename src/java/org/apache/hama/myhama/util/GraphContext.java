/**
 * Termite System
 * copyright 2012-2010
 */
package org.apache.hama.myhama.util;

import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;

/**
 * GraphContext. 
 * The extended class is used by the core engine of Hybrid.
 * This class contains information about a {@link GraphRecord}, including:
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class GraphContext<V, W, M, I> extends Context<V, W, M, I> {
	
	public GraphContext(int _taskId, BSPJob _job, int _iteNum, int _iteStyle) {
		super(_taskId, _job, _iteNum, _iteStyle);
	}
	
	/**
	 * Initialize {@link GraphContext}.
	 * @param _graph
	 * @param _msg
	 * @param _jobAgg
	 * @param _actFlag
	 */
	public void initialize(GraphRecord<V, W, M, I> _graph, 
			MsgRecord<M> _msg, float _jobAgg, boolean _actFlag) {
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
