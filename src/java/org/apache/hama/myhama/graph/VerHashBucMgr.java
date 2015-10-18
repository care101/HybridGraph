package org.apache.hama.myhama.graph;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.Constants;
import org.apache.hama.myhama.api.UserTool;

public class VerHashBucMgr {
	private static final Log LOG = LogFactory.getLog(VerHashBucMgr.class);
	private UserTool userTool;
	private int hashBucNum;
	private int hashBucLen;
	private int verMinId, verMaxId;
	private int verNum;
	private long edgeNum;
	private int maxBucVerNum;
	
	private VerHashBucBeta[] buckets; //bucket id, beta
	
	private int[] bucNumTask;
	private int perOffsetNum; //final
	
	/**
	 * Construct function.
	 * @param _verMinId
	 * @param _verMaxId
	 * @param _bucNum
	 * @param _bucLen
	 */
	public VerHashBucMgr(int _verMinId, int _verMaxId, int _bucNum, int _bucLen, 
			int _taskNum, int[] _bucNumTask, UserTool _userTool, int bspStyle) {
		this.userTool = _userTool;
		verMinId = _verMinId;
		verMaxId = _verMaxId;
		verNum = _verMaxId - _verMinId + 1;
        hashBucNum = _bucNum;
        hashBucLen = _bucLen;
        this.bucNumTask = _bucNumTask;
        for (int num: this.bucNumTask) {
        	this.perOffsetNum += (2*num);
        }
        
        int sum = _verMinId;
        buckets = new VerHashBucBeta[_bucNum];
        LOG.info("initialize VerHashBucMgr with #buckets=" + _bucNum);
        this.maxBucVerNum = 0;
        for (int bid = 0; bid < _bucNum; bid++) {
        	int tmpLen = hashBucLen;
        	if (bid == (hashBucNum-1)) {
        		tmpLen = _verMaxId - sum + 1;
        	}
        	
        	this.maxBucVerNum = this.maxBucVerNum>tmpLen? this.maxBucVerNum:tmpLen;
        	//distribute mini bucket evenly
        	buckets[bid] = new VerHashBucBeta(bid, sum, (sum+tmpLen-1), tmpLen, 
        			_taskNum, _bucNumTask, 1, this.userTool, bspStyle);
        	sum += tmpLen;
        }
	}
	
	public long getMemUsage() {
		long usage = 7 * 4; //seven int variables
		usage += 8; //one long variable
		usage += (4 * this.bucNumTask.length); //int array
		for (int bid = 1; bid < this.hashBucNum; bid++) {
			usage += this.buckets[bid].getMemUsage();
		}
		
		return usage;
	}
	
	public int[] getBucNumTasks() {
		return this.bucNumTask;
	}
	
	public int getMaxBucVerNum() {
		return this.maxBucVerNum;
	}
	
	/**
	 * Update the number of vertices and edges for each {@link VerHashBucBeta} 
	 * and its corresponding {@link VerMiniBucBeta}.
	 * @param bid
	 * @param vid
	 * @param _edgeNum
	 */
	public void updateBucEdgeNum(int bid, int vid, int _edgeNum) {
		this.edgeNum += _edgeNum;
	}
	
	/**
	 * Update and record the length of vertex value file and graphInfo file for 
	 * {@link VerHashBucBeta}'s corresponding {@link VerMiniBucBeta}.
	 * Now, it is used when updating or reading vertex value file and graphInfo file.
	 * @param bid
	 * @param vid
	 * @param _verLen
	 * @param _infoLen
	 */
	public void updateBucVerInfoLen(int bid, int vid, long _verLen, long _infoLen) {
		buckets[bid].updateVerInfoLen(vid, _verLen, _infoLen);
	}
	
	/**
	 * Update and record the length of edges belonging to the (_dstTid, _dstBid) edge file, 
	 * for each {@link VerHashBucBeta} and {@link VerMiniBucBeta}.
	 * @param _srcBid
	 * @param _dstTid
	 * @param _dstBid
	 * @param _vid
	 * @param _edgeLen
	 */
	public void updateBucEVidEdgeLen(int _srcBid, int _dstTid, int _dstBid, 
			int _vid, long _edgeLen) {
		buckets[_srcBid].updateEVidEdgeLen(_dstTid, _dstBid, _vid, _edgeLen);
	}
	
	/**
	 * Update and record the number of source vertices of edges 
	 * belonging to the (_dstTid, _dstBid) edge file, 
	 * for the _srcBid local bucket's corresponding {@link VerMiniBucBeta}.
	 * @param _srcBid
	 * @param _dstTid
	 * @param _dstBid
	 * @param _vid
	 * @param _n
	 */
	public void updateBucEVerNum(int _srcBid, int _dstTid, int _dstBid, int _vid, int _n) {
		this.buckets[_srcBid].updateEVerNum(_dstTid, _dstBid, _vid, _n);
	}
	
	/**
	 * Set the updating flag for vid, for {@link VerMiniBucBeta} in {@link VerHashBucBeta} with bid.
	 * This function is used for each vertex in {@link VerHashBucBeta} with bid.
	 * Note that this will not be invoked until the flag of vid is ture.
	 * @param type
	 * @param bid
	 * @param vid
	 * @param flag
	 */
	public void setBucUpdated(int type, int bid, int vid, boolean flag) {
		this.buckets[bid].setUpdated(type, vid, flag);
	}
	
	/**
	 * Set the updating flag for all {@link VerMiniBucBeta} in {@link VerHashBucBeta} with bid.
	 * This function is used when the {@link VerHashBucBeta} with bid is skipped.
	 * @param type
	 * @param bid
	 * @param flag
	 */
	public void setBucUpdated(int type, int bid, boolean flag) {
		this.buckets[bid].setUpdated(type, flag);
	}
	
	public void incUpdVerNumBuc(int bid) {
		this.buckets[bid].incUpdVerNum(1);
	}
	
	/**
	 * In the loadGraphData() function of {@link GraphDataServer}, 
	 * it should be invoked after finishing loading graph data from HDFS, 
	 * to compute the total #vertices and #edges of this task, 
	 * and set the starting offset for edge data files of each local bucket.
	 */
	public void loadOver(int bspStyle) {
		if (bspStyle == Constants.STYLE.Push) {
			return;
		}
		
		int taskNum = this.buckets[0].eVidEdgeLen.length;
		int[] bucNumTask = new int[taskNum];
		long[][] sum = new long[taskNum][];
		for (int i = 0; i < taskNum; i++) {
			bucNumTask[i] = this.buckets[0].eVidEdgeLen[i].length;
			sum[i] = new long[bucNumTask[i]];
			for (int j = 0; j < bucNumTask[i]; j++) {
				sum[i][j] += this.buckets[0].eVidEdgeLen[i][j];
			}
		}
		
		for (int bid = 1; bid < this.hashBucNum; bid++) {
			for (int i = 0; i < taskNum; i++) {
				for (int j = 0; j < bucNumTask[i]; j++) {
					this.buckets[bid].eVidEdgeStart[i][j] = sum[i][j];
					sum[i][j] += this.buckets[bid].eVidEdgeLen[i][j];
				}
			}
		}
		
		for (int bid = 0; bid < this.hashBucNum; bid++) {
			this.buckets[bid].loadOver();
		}
	}
	
	/**
	 * Clear hasReadVer for each local bucket.
	 * It should be invoked in the function beginSuperStep of {@link BSPTask}.
	 */
	public void clearBefIte(int _iteNum) {
		for (int bid = 0; bid < this.hashBucNum; bid++) {
			this.buckets[bid].clearBefIte(_iteNum);
		}
	}
	
	public void clearAftIte(int _iteNum) {
		for (int bid = 0; bid < this.hashBucNum; bid++) {
			this.buckets[bid].clearAftIte(_iteNum);
		}
	}
	
	public VerHashBucBeta getVerHashBucBeta(int _bid) {
		return this.buckets[_bid];
	}
	
	public int getHashBucNum() {
		return hashBucNum;
	}
	
	public int getHashBucLen() {
		return hashBucLen;
	}
	
	public int getVerMinId() {
		return verMinId;
	}
	
	public int getVerMaxId() {
		return verMaxId;
	}
	
	public int getVerNum() {
		return verNum;
	}
	
	public long getEdgeNum() {
		return edgeNum;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("Hash Bucket Info.\n");
		for (int i = 0; i < hashBucNum; i++) {
			sb.append(this.buckets[i].toString()); sb.append("\n");
		}
		return sb.toString();
	}
}
