package org.apache.hama.myhama.graph;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.myhama.api.UserTool;

public class VerMiniBucMgr {
	private static final Log LOG = LogFactory.getLog(VerMiniBucMgr.class);
	private UserTool userTool;
	private int bucId; //bucket id
	
	private VerMiniBucBeta miniBucket; //bucket id, beta
	
	/**
	 * Construct function.
	 * Construct a series of {@link MiniBucBeta} buckets with balanced #vertex, i.e., len.
	 * @param _miniBucNum
	 * @param _verNum
	 * @param _taskNum
	 * @param _bucNumTask
	 */
	public VerMiniBucMgr(int bucId, int _verMinId, int _verMaxId, int _verNum, 
			int _taskNum, int[] _bucNumTask, UserTool _userTool, int _bspStyle) {
		this.userTool = _userTool;
        this.bucId = bucId;
        this.miniBucket = 
    		new VerMiniBucBeta(0, _verMinId, _verMaxId, _verNum, _taskNum, _bucNumTask, 
    				_bspStyle);
	}
	
	public long getMemUsage() {
		return this.miniBucket.getMemUsage();
	}
	
	/**
	 * Update and record the length of vertex value file and graphInfo file for each bucket.
	 * @param vid
	 * @param _verLen
	 * @param _infoLen
	 */
	public void updateMiniBucVerInfoLen(int vid, long _verLen, long _infoLen) {
		this.miniBucket.updateVerInfoLen(_verLen, _infoLen);
	}
	
	/**
	 * Update and record the length of edges belonging to the (_dstTid, _dstBid) edge file, 
	 * for each mini bucket.
	 * @param _dstTid
	 * @param _dstBid
	 * @param _vid
	 * @param _edgeLen
	 */
	public void updateMiniBucEVidEdgeLen(int _dstTid, int _dstBid, int _vid, long _edgeLen) {
		this.miniBucket.updateEVidEdgeLen(_dstTid, _dstBid, _edgeLen);
	}
	
	/**
	 * Update and record the number of source vertices of edges 
	 * belonging to the (_dstTid, _dstBid) edge file, for the _srcBid local bucket.
	 * @param _dstTid
	 * @param _dstBid
	 * @param vid
	 * @param _n
	 */
	public void updateMiniBucEVerNum(int _dstTid, int _dstBid, int vid, int _n) {
		this.miniBucket.updateEVerNum(_dstTid, _dstBid, _n);
	}
	
	/**
	 * Set the updating flag for vid, for {@link VerMiniBucBeta}.
	 * This function is used for each vertex in {@link VerHashBucBeta}.
	 * @param type
	 * @param vid
	 * @param _flag
	 */
	public void setMiniBucUpdated(int type, int vid, boolean _flag) {
		this.miniBucket.setUpdated(type, _flag);
		this.miniBucket.updateActNum();
	}
	
	/**
	 * Set the updating flag for all {@link VerMiniBucBeta}.
	 * This function is used when the whole {@link VerHashBucBeta} is skipped.
	 * @param type
	 * @param _flag
	 */
	public void setMiniBucUpdated(int type, boolean _flag) {
		this.miniBucket.setUpdated(type, _flag);
	}
	
	/**
	 * In the loadGraphData() function of {@link GraphDataServer}, 
	 * it should be invoked after finishing loading graph data from HDFS, 
	 * to compute the total #vertices and #edges of this task, 
	 * and set the starting offset for edge data files of each local bucket.
	 */
	public void loadOver(long[][] sum_edge) {
		int taskNum = this.miniBucket.eVidEdgeLen.length;
		int[] bucNumTask = new int[taskNum];
		for (int i = 0; i < taskNum; i++) {
			bucNumTask[i] = this.miniBucket.eVidEdgeLen[i].length;
		}
		
		this.miniBucket.verStart = 0;
		this.miniBucket.infoStart = 0;
		for (int i = 0; i < taskNum; i++) {
			for (int j = 0; j < bucNumTask[i]; j++) {
				this.miniBucket.eVidEdgeStart[i][j] = sum_edge[i][j];
				sum_edge[i][j] += this.miniBucket.eVidEdgeLen[i][j];
			}
		}
	}
		
	public int getBucId() {
		return this.bucId;
	}
	
	/**
	 * Clear hasReadVer for each local bucket.
	 * It should be invoked in the function beginSuperStep of {@link BSPTask}.
	 */
	public void clearBefIte(int _iteNum) {
		this.miniBucket.clearBefIte(_iteNum);
	}
	
	public void clearAftIte(int _iteNum) {
		this.miniBucket.clearAftIte(_iteNum);
	}
	
	public int getMiniBucNum() {
		return 1;
	}
	
	public VerMiniBucBeta getVerMiniBucBeta(int _miniBid) {
		return this.miniBucket;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.miniBucket.toString()); sb.append("\n");
		return sb.toString();
	}
}
