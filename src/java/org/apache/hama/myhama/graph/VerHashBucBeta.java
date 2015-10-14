package org.apache.hama.myhama.graph;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.Constants;
import org.apache.hama.myhama.api.UserTool;

public class VerHashBucBeta {
	private static final Log LOG = LogFactory.getLog(VerHashBucBeta.class);
	private UserTool userTool;
	private int bucId;
	private int bspStyle;
	private int granularity = 0; //#tasks * #blocks per task
	
	private int vMinId, vMaxId; //source vertex id
	private int vNum = 0;
	private int updVerNum = 0;
	private int hasReadVer = 0;
	
	private VerMiniBucMgr miniBucMgr;
	
	/** used for computing variables in {@link VerMiniBucBeta}*/
	public long[][] eVidEdgeStart;
	public long[][] eVidEdgeLen;
	private int[][] eVerNum; //it will not be changed during adjusting.
	/** flag this bucket is updated or not */
	private boolean[] update; //type, it will not be changed during adjusting.
	
	private long verLen = 0L;
	private long infoLen = 0L;
	
	public VerHashBucBeta(int _bucId, int _verMinId, int _verMaxId, int _verNum, 
			int _taskNum, int[] _bucNumTask, int _enlarge, UserTool _userTool, 
			int _bspStyle) {
		this.userTool = _userTool;
		bucId = _bucId;
		this.bspStyle = _bspStyle;
		vMinId = _verMinId;
		vMaxId = _verMaxId;
		vNum = _verNum;
		this.update = new boolean[2]; Arrays.fill(this.update, false);
		this.granularity = 0;
		
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
		
		this.miniBucMgr = new VerMiniBucMgr(this.bucId, _verMinId, _verMaxId, _verNum, 
				_taskNum, _bucNumTask, this.userTool, this.bspStyle);
	}
	
	public long getMemUsage() {
		long usage = 10 * 4; //ten int variables
		usage += 2; //two boolean variables
		usage += (8*2) * this.granularity;
		usage += 4 * this.granularity;
		usage += this.miniBucMgr.getMemUsage();
		
		return usage;
	}
	
	public void updateVerInfoLen(int _vid, long _verLen, long _infoLen) {
		this.verLen += _verLen;
		this.infoLen += _infoLen;
		this.miniBucMgr.updateMiniBucVerInfoLen(_vid, _verLen, _infoLen);
	}
	
	public long getVerLen() {
		return this.verLen;
	}
	
	public long getInfoLen() {
		return this.infoLen;
	}
	
	public void updateEVidEdgeLen(int _dstTid, int _dstBid, int _vid, long _eVidEdgeLen) {
		this.eVidEdgeLen[_dstTid][_dstBid] += _eVidEdgeLen;
		this.miniBucMgr.updateMiniBucEVidEdgeLen(_dstTid, _dstBid, _vid, _eVidEdgeLen);
	}
	
	public long getEVidEdgeLen(int _dstTid, int _dstBid) {
		return 
			this.bspStyle==Constants.STYLE.Push? 0:this.eVidEdgeLen[_dstTid][_dstBid];
	}
	
	public void updateEVerNum(int _tid, int _bid, int vid, int _n) {
		this.eVerNum[_tid][_bid] += _n;
		this.miniBucMgr.updateMiniBucEVerNum(_tid, _bid, vid, _n);
	}
	
	public int getEVerNum(int _tid, int _bid) {
		return 
		this.bspStyle==Constants.STYLE.Push? 0:this.eVerNum[_tid][_bid];
	}
	
	public void setUpdated(int type, int vid, boolean flag) {
		this.update[type] = flag;
		this.miniBucMgr.setMiniBucUpdated(type, vid, flag);
	}
	
	public boolean isUpdated(int type) {
		return this.update[type];
	}
	
	public void setUpdated(int type, boolean flag) {
		this.update[type] = flag;
		this.miniBucMgr.setMiniBucUpdated(type, flag);
	}
	
	public void incUpdVerNum(int _num) {
		this.updVerNum += _num;
	}
	
	public int getUpdVerNum() {
		return this.updVerNum;
	}
	
	public void clearBefIte(int _iteNum) {
		hasReadVer = 0;
		this.updVerNum = 0;
		int type = (_iteNum+1)%2;
		this.update[type] = false;
		
		this.miniBucMgr.clearBefIte(_iteNum);
	}
	
	public void clearAftIte(int _iteNum) {
		this.miniBucMgr.clearAftIte(_iteNum);
	}
	
	public void loadOver() {
		if (this.bspStyle != Constants.STYLE.Push) {
			long[][] tmp = new long[this.eVidEdgeStart.length][];
			for (int i = 0; i < this.eVidEdgeStart.length; i++) {
				tmp[i] = new long[this.eVidEdgeStart[i].length];
				for (int j = 0; j < this.eVidEdgeStart[i].length; j++) {
					tmp[i][j] = this.eVidEdgeStart[i][j];
				}
			}
			this.miniBucMgr.loadOver(tmp);
		}
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
	
	public VerMiniBucMgr getVerMiniBucMgr() {
		return this.miniBucMgr;
	}
	
	public int getVerId() {
		return this.vMinId + (hasReadVer++);
	}
	
	public boolean hasNext() {
		return (hasReadVer<vNum);
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("bucId=" + bucId);
		sb.append(" vId=[" + vMinId + " " + vMaxId + "]");
		sb.append(" num=[" + vNum + "]");
		sb.append("\n");
		sb.append(this.miniBucMgr.toString());
		return sb.toString();
	}
}
