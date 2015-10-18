package org.apache.hama.myhama.graph;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.Constants;

public class VerMiniBucBeta {
	private static final Log LOG = LogFactory.getLog(VerMiniBucBeta.class);
	private int bucId;
	private int bspStyle;
	private int granularity = 0; //#tasks * #blocks per task
	
	/** used for vertices */
	private int vMinId, vMaxId; //source vertex id
	private int vNum = 0;
	public long verStart = 0L;
	public long verLen= 0L;
	public long infoStart = 0L;
	public long infoLen = 0L;
	
	/** used for edges */
	private int[][] eVerNum;
	public long[][] eVidEdgeStart;
	public long[][] eVidEdgeLen;
	
	/** flag this bucket is updated or not */
	private boolean[] update; //type
	private int actNum = 0;
	
	public VerMiniBucBeta(int _bucId, int _vMinId, int _vMaxId, int _vNum, 
			int _taskNum, int[] _bucNumTask, int _bspStyle) {
		bucId = _bucId;
		this.bspStyle = _bspStyle;
		vMinId = _vMinId;
		vMaxId = _vMaxId;
		vNum = _vNum;
		this.update = new boolean[2]; Arrays.fill(this.update, false);
		
		if (this.bspStyle != Constants.STYLE.Push) {
			this.eVidEdgeStart = new long[_taskNum][];
			this.eVidEdgeLen = new long[_taskNum][];
			this.eVerNum = new int[_taskNum][];
			for (int tid = 0; tid < _taskNum; tid++) {
				this.eVidEdgeStart[tid] = new long[_bucNumTask[tid]];
				this.eVidEdgeLen[tid] = new long[_bucNumTask[tid]];
				this.eVerNum[tid] = new int[_bucNumTask[tid]];
				this.granularity += _bucNumTask[tid];
			}
		}
	}
	
	public long getMemUsage() {
		long usage = 7 * 4; //seven int variables
		usage += 4 * 8; //four long variables
		usage += 2; //two boolean variables
		usage += (8*2) * this.granularity;
		usage += 4 * this.granularity;
		
		return usage;
	}
	
	public boolean isInSrcVidBound(int vid) {
		if ((vid>=this.vMinId) && (vid<=this.vMaxId)) {
			return true;
		} else {
			return false;
		}
	}
	
	public void updateActNum() {
		this.actNum++;
	}
	
	public void setUpdated(int type, boolean _flag) {
		this.update[type] = _flag;
	}
	
	public boolean isUpdated(int type) {
		return this.update[type];
	}
	
	public int getBucId() {
		return bucId;
	}
	
	public int getVerNum() {
		return vNum;
	}
	
	public int getVerMinId() {
		return vMinId;
	}
	
	public int getVerMaxId() {
		return vMaxId;
	}
	
	public void updateVerInfoLen(long _verLen, long _infoLen) {
		verLen += _verLen;
		infoLen += _infoLen;
	}
	
	public void updateEVidEdgeLen(int _dstTid, int _dstBid, long _eVidEdgeLen) {
		this.eVidEdgeLen[_dstTid][_dstBid] += _eVidEdgeLen;
	}
	
	public long getEVidEdgeLen(int dstTid, int dstBid) {
		return 
			this.bspStyle==Constants.STYLE.Push? 0:this.eVidEdgeLen[dstTid][dstBid];
	}
	
	public long getEVidEdgeStart(int _tid, int _bid) {
		return 
			this.bspStyle==Constants.STYLE.Push? 0:this.eVidEdgeStart[_tid][_bid];
	}
	
	public void updateEVerNum(int _tid, int _bid, int _n) {
		this.eVerNum[_tid][_bid] += _n;
	}
	
	public int getEVerNum(int _tid, int _bid) {
		return 
			this.bspStyle==Constants.STYLE.Push? 0:this.eVerNum[_tid][_bid];
	}
	
	public long getVerStart() {
		return verStart;
	}
	
	public long getInfoStart() {
		return infoStart;
	}
	
	public void clearBefIte(int _iteNum) {
		this.update[(_iteNum+1)%2] = false;
		this.actNum = 0;
	}
	
	public void clearAftIte(int _iteNum) {
		
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("MminiBucId=" + bucId);
		sb.append(" vId=[" + vMinId + " " + vMaxId + "]");
		sb.append(" num=[" + vNum + "]");
		sb.append(" verOff=[" + verStart + " " + verLen + "]");
		sb.append("\n");
		for (int i = 0; i < this.eVerNum.length; i++) {
			sb.append("dstTaskId=" + i); sb.append("\n");
			sb.append("eVerNum=" + Arrays.toString(this.eVerNum[i])); sb.append("\n");
			sb.append("startoff=" + Arrays.toString(this.eVidEdgeStart[i])); sb.append("\n");
			sb.append("len=" + Arrays.toString(this.eVidEdgeLen[i])); sb.append("\n");
		}
		return sb.toString();
	}
}
