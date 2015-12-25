package org.apache.hama.monitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

public class GlobalSketchGraph implements Writable {
	public static final Log LOG = LogFactory.getLog(GlobalSketchGraph.class);
	private int verNum;
	private int iteNum;
	private int taskNum;
	private int[] minVerIdTask;
	private int[] maxVerIdTask;
	private int[] bucLenTask;
	private int[] bucNumTask;
	
	private int bucNumJob = 0;
	private int[] tidToBid; //taskId to the beginning of bucketId
	
	private int[] verNumBlks;
	/** number of responding source vertices of each VBlock */
	private int[] resVerNumBlks;
	/** dependency relationship among all VBlocks with responding vertices */
	private boolean[][] resDepend;
	
	public GlobalSketchGraph() {
		
	}
	
	/**
	 * SketchGraph
	 * @param taskNum
	 * @param minVerIdTask
	 * @param maxVerIdTask
	 * @param reaBucLenTask
	 * @param reaBucNumTask
	 */
	public GlobalSketchGraph(int _verNum, int _iteNum, int _taskNum, 
			int[] _minVerIdTask, int[] _maxVerIdTask, 
			int[] _bucLenTask, int[] _bucNumTask) {
		this.verNum = _verNum;
		this.iteNum = _iteNum;
		this.taskNum = _taskNum;
		this.minVerIdTask = _minVerIdTask;
		this.maxVerIdTask = _maxVerIdTask;
		this.bucLenTask = _bucLenTask;
		this.bucNumTask = _bucNumTask;
		
		for (int num: _bucNumTask) {
			this.bucNumJob += num;
		}
		
		this.tidToBid = new int[taskNum];
		int bidCounter = 0;
		for (int tid = 0; tid < this.taskNum; tid++) {
			this.tidToBid[tid] = bidCounter;
			bidCounter += this.bucNumTask[tid];
		}
		
		this.verNumBlks = new int[this.bucNumJob];
		this.resVerNumBlks = new int[this.bucNumJob];
		this.resDepend = new boolean[this.bucNumJob][this.bucNumJob];
	}
	
	public void buildRespondDependency(int tid, boolean[][] _resDepend) {
		if (_resDepend == null) {
			return;
		}
		
		int beginId = this.tidToBid[tid];
		for (int i = 0; i < this.bucNumTask[tid]; i++) {
			this.resDepend[i+beginId] = _resDepend[i];
		}
	}
	
	public void buildVerNumBlks(int tid, int[] nums) {
		if (nums == null) {
			return;
		}
		
		int beginId = this.tidToBid[tid];
		for (int i = 0; i < this.bucNumTask[tid]; i++) {
			this.verNumBlks[i+beginId] = nums[i];
			this.resVerNumBlks[i+beginId] = 0;
		}
	}
	
	public void updateRespondVerNumBlks(int curIteNum, int tid, int[] nums) {
		int beginId = this.tidToBid[tid];
		for (int i = 0; i < this.bucNumTask[tid]; i++) {
			this.resVerNumBlks[i+beginId] = nums[i];
		}
	}
	
	public ArrayList<Integer>[] getRealCommRoute(int curIteNum, int _dstTid) {
		int beginId = this.tidToBid[_dstTid];
		int bucNum = this.bucNumTask[_dstTid];
		int dstBucId = 0;
		
		ArrayList<Integer>[] route = new ArrayList[bucNum];
		
		for (int i = 0; i < bucNum; i++) {
			route[i] = new ArrayList<Integer>();
			dstBucId = beginId + i;
			
			for (int srcTid = 0; srcTid < this.taskNum; srcTid++) {
				int begSrcBucId = this.tidToBid[srcTid];
				int srcBucId = 0;
				boolean yes = false;
				for (int j = 0; j < this.bucNumTask[srcTid]; j++) {
					srcBucId = begSrcBucId + j;
					if (this.resVerNumBlks[srcBucId]>0 
							&& this.resDepend[srcBucId][dstBucId]) {
						//has updated, && has edges.
						yes = true;
						break;
					}
				}
				if (yes) {
					route[i].add(srcTid);
				}
			}
		}
		
		return route;
	}
	
	public int getBucNumJob() {
		return this.bucNumJob;
	}
	
	public int[] getBucNumTask() {
		return this.bucNumTask;
	}
	
	public int getBucNumTask(int tid) {
		return this.bucNumTask[tid];
	}
	
	public int[] getBucLenTask() {
		return this.bucLenTask;
	}
	
	public int getBucLenTask(int tid) {
		return this.bucLenTask[tid];
	}
	
	/**
	 * Get the local bucket index for the given task, namely the bucket id.
	 * @param taskId
	 * @param vId
	 * @return
	 */
	public int getTaskBucIndex(int tid, int vid) {
		return (vid-this.minVerIdTask[tid])/this.bucLenTask[tid];
	}
	
	public int getGlobalBucIndex(int _dstTid, int _dstBid) {
		return (this.tidToBid[_dstTid] + _dstBid);
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("\n");
		
		return sb.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.bucNumJob = in.readInt();
		this.taskNum = in.readInt();
		
		this.minVerIdTask = new int[this.taskNum];
		this.bucNumTask = new int[this.taskNum];
		this.bucLenTask = new int[this.taskNum];
		this.tidToBid = new int[this.taskNum];
		for (int i = 0; i < this.taskNum; i++) {
			this.minVerIdTask[i] = in.readInt();
			this.bucNumTask[i] = in.readInt();
			this.bucLenTask[i] = in.readInt();
			this.tidToBid[i] = in.readInt();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.bucNumJob);
		out.writeInt(this.taskNum);
		for (int i = 0; i < this.taskNum; i++) {
			out.writeInt(this.minVerIdTask[i]);
			out.writeInt(this.bucNumTask[i]);
			out.writeInt(this.bucLenTask[i]);
			out.writeInt(this.tidToBid[i]);
		}
	}
}
