/**
 * Termite System
 * NEU SoftLab 401
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
 * 
 * @author WangZhigang
 * @version 0.1
 * @time 2013-10-11
 */
public class GraphContext {
	private GraphRecord graph;
	private int iteNum;
	private MsgRecord msg;
	private float jobAgg;
	private float vAgg;
	
	private boolean acFlag;
	private boolean upFlag;
	
	public GraphContext() {
		
	}
	
	@SuppressWarnings("unchecked")
	public GraphContext(GraphRecord _graph, int _iteNum, MsgRecord _msg, float _jobAgg) {
		graph = _graph; 
		msg = _msg; 
		iteNum = _iteNum; 
		jobAgg = _jobAgg;
	}
	
	@SuppressWarnings("unchecked")
	public void initialize(GraphRecord _graph, int _iteNum, MsgRecord _msg, 
			float _jobAgg, boolean _acFlag) {
		graph = _graph; 
		msg = _msg; 
		iteNum = _iteNum; 
		jobAgg = _jobAgg;
		this.acFlag = _acFlag;
	}
	
	@SuppressWarnings("unchecked")
	public GraphRecord getGraphRecord() {
		return graph;
	}
	
	@SuppressWarnings("unchecked")
	public MsgRecord getMsgRecord() {
		return msg;
	}
	
	public float getJobAgg() {
		return jobAgg;
	}
	
	public int getIteCounter() {
		return iteNum;
	}
	
	public float getVertexAgg() {
		return this.vAgg;
	}
	
	public void setVertexAgg(float agg) {
		this.vAgg = agg;
	}
	
	@SuppressWarnings("unchecked")
	public void reset() {
		this.acFlag = true;
		this.upFlag = false;
	}
	
	public void setUpdate() {
		this.upFlag = true;
	}
	
	public boolean isUpdate() {
		return this.upFlag;
	}
	
	public boolean isActive() {
		return this.acFlag;
	}
	
	public void voteToHalt() {
		this.acFlag = false;
	}
}
