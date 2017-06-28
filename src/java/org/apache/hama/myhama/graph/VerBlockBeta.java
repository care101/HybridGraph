package org.apache.hama.myhama.graph;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.Constants;

/**
 * Metadata of one VBlock
 * @author root
 */
public class VerBlockBeta {
	private static final Log LOG = LogFactory.getLog(VerBlockBeta.class);
	
	private int bid;
	private Constants.STYLE bspStyle;
	private int granularity = 0; //#tasks * #blocks per task
	
	private int vMinId, vMaxId; //source vertex id in this VBlock
	private int vNum = 0; //number of vertices in this VBlock
	private int resVerNum = 0; //number of responding vertices
	//number of vertices which have been read at one superstep
	private int hasReadVerNum = 0;
	
	private long[][] eFragStart;
	private long[][] eFragLen;
	private int[][] fragNum; //number of fragments in (dstTid, dstBid) file
	private int totalFragNum;
	/** true: some/all vertices need to respond pull requests, false: none */
	private boolean[] respond; //type, exchange between superstep t and t+1.
	/** true: all vertices need to respond pull requests, false: none */
	private boolean[] respondOfAllVerts; //all vertices or not?
	/** true: some/all vertices are active, false: none */
	private boolean active;
	/** true: all vertices are active, false: none */
	private boolean activeOfAllVerts;
	private int actVerNum = 0;
	
	public VerBlockBeta(int _bid, int _verMinId, int _verMaxId, int _verNum, 
			int _taskNum, int[] _blkNumTask, 
			Constants.STYLE _bspStyle) {
		bid = _bid;
		bspStyle = _bspStyle;
		vMinId = _verMinId;
		vMaxId = _verMaxId;
		vNum = _verNum;
		respond = new boolean[2]; 
		respondOfAllVerts = new boolean[2];
		Arrays.fill(this.respond, false);
		Arrays.fill(this.respondOfAllVerts, false);
		active = false;
		activeOfAllVerts = false;
		granularity = 0;
		totalFragNum = 0;
		
		if (this.bspStyle != Constants.STYLE.PUSH) {
			this.eFragStart = new long[_taskNum][];
			this.eFragLen = new long[_taskNum][];
			this.fragNum = new int[_taskNum][];
			for (int tid = 0; tid < _taskNum; tid++) {
				this.eFragStart[tid] = new long[_blkNumTask[tid]];
				this.eFragLen[tid] = new long[_blkNumTask[tid]];
				this.fragNum[tid] = new int[_blkNumTask[tid]];
				this.granularity += _blkNumTask[tid];
			}
		}
	}
	
	public long getMemUsage() {
		long usage = 8 * 4; //ten int variables
		usage += 2; //two boolean variables
		usage += (8*2) * this.granularity; //eFragStart and eFragLen
		usage += 4 * this.granularity; //eFragNum
		usage += 4; //totalFragNum
		
		return usage;
	}
	
	public void updateFragmentLenAndNum(int _dstTid, int _dstBid, 
			long _eFragLen) {
		this.eFragLen[_dstTid][_dstBid] += _eFragLen;
		this.fragNum[_dstTid][_dstBid]++;
		this.totalFragNum++;
	}
	
	/**
	 * Get the byte size of all fragments whose source vertices belong to 
	 * this VBlock and target vertices belong to the _dstBid-th VBlock 
	 * on the _dstTid-th task.
	 * 
	 * @param _dstTid
	 * @param _dstBid
	 * @return
	 */
	public long getFragmentLen(int _dstTid, int _dstBid) {
		return 
			this.bspStyle==Constants.STYLE.PUSH? 
					0:this.eFragLen[_dstTid][_dstBid];
	}
	
	/**
	 * Set the starting offset of fragments 
	 * in the (_dstTid,dstBid)-th edge file. 
	 * Source vertices of these fragments belong to
	 * this VBlock.
	 * 
	 * @param _dstTid
	 * @param _dstBid
	 * @param start
	 */
	public void setFragmentStart(int _dstTid, int _dstBid, long start) {
		this.eFragStart[_dstTid][_dstBid] = start;
	}
	
	/**
	 * Get the starting offset of fragments 
	 * in the (_dstTid,dstBid)-th edge file. 
	 * Source vertices of these fragments belong to
	 * this VBlock.
	 * 
	 * @param _dstTid
	 * @param _dstBid
	 * @return
	 */
	public long getFragmentStart(int _dstTid, int _dstBid) {
		return 
			this.bspStyle==Constants.STYLE.PUSH? 
					0:this.eFragStart[_dstTid][_dstBid];
	}
	
	/**
	 * Get the number of fragments whose source vertices 
	 * belong to this VBlock and target vertices belong 
	 * to the _dstBid-th VBlock on the _dstTid-th task.
	 * 
	 * @param _dstTid
	 * @param _dstBid
	 * @return
	 */
	public int getFragmentNum(int _dstTid, int _dstBid) {
		return 
		this.bspStyle==Constants.STYLE.PUSH? 
				0:this.fragNum[_dstTid][_dstBid];
	}
	
	/**
	 * Get the total number of fragments whose source vertices 
	 * belong to this VBlock.
	 * @return
	 */
	public int getFragmentNum() {
		return this.totalFragNum;
	}
	
	public void setActive(boolean flag) {
		active = flag;
	}
	
	public boolean isActive() {
		return active;
	}
	
	public boolean isAllActive() {
		return activeOfAllVerts;
	}
	
	public void incActiveVerNum() {
		actVerNum++;
	}
	
	public void setRespond(int type, boolean flag) {
		this.respond[type] = flag;
	}
	
	/**
	 * Is there any vertex eager to respond pull requests?
	 * @return
	 */
	public boolean isRespond(int type) {
		return this.respond[type];
	}
	
	/**
	 * Is every vertex eager to respond pull requests?
	 * @param type
	 * @return
	 */
	public boolean isAllRespond(int type) {
		return this.respondOfAllVerts[type];
	}
	
	public void incRespondVerNum() {
		this.resVerNum++;
	}
	
	/**
	 * Get the number of source vertices eager to 
	 * send messages to their target vertices, 
	 * i.e., responding pull requests from target vertices.
	 * @return
	 */
	public int getRespondVerNum() {
		return this.resVerNum;
	}
	
	public void clearBefIte(int _iteNum) {
		hasReadVerNum = 0;
		resVerNum = 0;
		respond[(_iteNum+1)%2] = false;
		respondOfAllVerts[(_iteNum+1)%2] = false;
		
		actVerNum = 0;
		active = false;
		activeOfAllVerts = false;
	}
	
	public void clearAftIte(int _iteNum, int flagOpt) {
		if (flagOpt == 1) { //true, log flags
			respondOfAllVerts[(_iteNum+1)%2] = 
				(resVerNum==vNum);
			activeOfAllVerts = (actVerNum==vNum);
		}
	}
	
	/**
	 * Get the number of source vertices in this VBlock.
	 * @return
	 */
	public int getVerNum() {
		return vNum;
	}
	
	/**
	 * Get the minimum source vertex id in this VBlock.
	 * @return
	 */
	public int getVerMinId() {
		return vMinId;
	}
	
	/**
	 * Get the maximum source vertex id in this VBlock.
	 * @return
	 */
	public int getVerMaxId() {
		return vMaxId;
	}
	
	/**
	 * Get the next source vertex id in this VBlock.
	 * This function is used when looping all source vertices.
	 * @return
	 */
	public int getVerId() {
		return this.vMinId + (hasReadVerNum++);
	}
	
	/**
	 * Doese this VBlock has the next source vertex?
	 */
	public boolean hasNext() {
		return (hasReadVerNum<vNum);
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("blkId=" + bid);
		sb.append(" verMinMaxId=[" + vMinId + ", " + vMaxId + "]");
		sb.append(" verNum=" + vNum);
		return sb.toString();
	}
}
