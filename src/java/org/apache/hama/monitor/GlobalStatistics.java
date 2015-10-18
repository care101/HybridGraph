package org.apache.hama.monitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPJob;

public class GlobalStatistics implements Writable {
	//private static final Log LOG = LogFactory.getLog(GlobalStatistics.class);
	private BSPJob job;
	private int taskNum = 0;
	private int verNum = 0;
	private long edgeNum = 0L;
	private int verMinId = Integer.MAX_VALUE, verMaxId = 0;
	private int[] taskIds, rangeMins, rangeMaxs, ports;
	private String[] hostNames;
	
	private GlobalSketchGraph skGraph;
	
	private int cachePerTask;
	
	public GlobalStatistics() {
		
	}
	
	public GlobalStatistics (BSPJob _job, int _taskNum) {
		this.job = _job;
		this.taskNum = _taskNum;
		this.taskIds = new int[_taskNum];
		this.rangeMins = new int[_taskNum];
		this.rangeMaxs = new int[_taskNum];
		this.ports = new int[_taskNum];
		this.hostNames = new String[_taskNum];
	}
	
	public void initialize(int _verNum) {
		initRangeMaxs(_verNum);
		
		int[] reaBucNumTask = new int[taskNum];
		int[] reaBucLenTask = new int[taskNum];
		for (int i = 0; i < taskNum; i++) {
			reaBucNumTask[i] = compReaBucNum(_verNum);
			reaBucLenTask[i] = (int)Math.ceil((double)(rangeMaxs[i]-rangeMins[i]+1)/reaBucNumTask[i]);
			//reaBucLenTask[i] = (rangeMaxs[i]-rangeMins[i]+1)/reaBucNumTask[i];
		}
		skGraph = new GlobalSketchGraph(_verNum, this.job.getNumSuperStep(), taskNum, 
				rangeMins, rangeMaxs, reaBucLenTask, reaBucNumTask);
	}
	
	private int compReaBucNum(int _verNum) {
		int bucNumPerTask = this.job.getNumBucketsPerTask();
		int outdegree = 1;
		this.cachePerTask = (outdegree*_verNum/bucNumPerTask) 
		    + (_verNum/(this.taskNum*bucNumPerTask)) 
			+ (2*2*bucNumPerTask*this.taskNum*bucNumPerTask);
		//HamaConfiguration conf = new HamaConfiguration();
		//LOG.info("taskMem=" + conf.get("bsp.child.java.opts"));
		return bucNumPerTask;
	}
	
	private void initRangeMaxs(int _verNum) {
		int[] tmpMin = new int[taskNum], tmpId = new int[taskNum];
		for (int i = 0; i < taskNum; i++) {
			tmpMin[i] = rangeMins[i];
			tmpId[i] = taskIds[i];
		}

		for (int i = 0, swap = 0; i < taskNum; i++) {
			for (int j = i + 1; j < taskNum; j++) {
				if (tmpMin[i] > tmpMin[j]) {
					swap = tmpMin[j]; tmpMin[j] = tmpMin[i]; tmpMin[i] = swap;
					swap = tmpId[j]; tmpId[j] = tmpId[i]; tmpId[i] = swap;
				}
			}
		}

		for (int i = 1; i < taskNum; i++) {
			rangeMaxs[tmpId[i-1]] = tmpMin[i] - 1;
		}
		rangeMaxs[tmpId[taskNum-1]] = _verNum + rangeMins[tmpId[0]] - 1;
		this.verMinId = tmpMin[0];
		this.verMaxId = rangeMaxs[tmpId[taskNum-1]];
		this.verNum = _verNum;
	}
	
	public int getVerNum() {
		return this.verNum;
	}
	
	public long getEdgeNum() {
		return this.edgeNum;
	}
	
	public int getVerMinId() {
		return this.verMinId;
	}
	
	public int getVerMaxId() {
		return this.verMaxId;
	}
	
	public int[] getTaskIds() {
		return taskIds;
	}
	
	public int[] getRangeMinIds() {
		return rangeMins;
	}
	
	public int getRangeMinId(int _taskId) {
		return rangeMins[_taskId];
	}
	
	public int[] getRangeMaxIds() {
		return rangeMaxs;
	}
	
	public int getRangeMaxId(int _taskId) {
		return rangeMaxs[_taskId];
	}
	
	public int[] getPorts() {
		return ports;
	}
	
	public String[] getHostNames() {
		return hostNames;
	}
	
	public GlobalSketchGraph getGlobalSketchGraph() {
		return skGraph;
	}
	
	public synchronized void updateInfo(int parId, LocalStatistics local) {
		this.taskIds[parId] = local.getTaskId();
		this.rangeMins[parId] = local.getVerMinId();
		this.ports[parId] = local.getPort();
		this.hostNames[parId] = local.getHostName();
		
		if (this.skGraph != null) {
			this.skGraph.buildEdgeMatrix(parId, local.getLocalMatrix());
			this.skGraph.buildVerNumBucs(parId, local.getVerNumBucs());
			this.edgeNum += local.getEdgeNum();
		}
	}
	
	public synchronized void updateActVerNumBucs(int curIteNum, int parId, int[] nums) {
		this.skGraph.updateActVerNumBucs(curIteNum, parId, nums);
	}
	
	public ArrayList<Integer>[] getRealCommRoute(int curIteNum, int _dstTid) {
		return this.skGraph.getRealCommRoute(curIteNum, _dstTid);
	}
	
	public int getCachePerTask() {
		return this.cachePerTask;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("\n");
		sb.append("================ Global Statistics Data ================");
		sb.append("\nminVertexId="); sb.append(this.verMinId);
		sb.append(" maxVertexId="); sb.append(this.verMaxId);
		sb.append("\nVertexNumber="); sb.append(this.verNum);
		sb.append(" EdgeNumber="); sb.append(this.edgeNum);
		sb.append("\nTaskNumber="); sb.append(this.taskNum);
		sb.append("\nCachePerTask="); sb.append(this.cachePerTask);
		sb.append(this.skGraph.toString()); sb.append("\n"); //display the sketch graph.
		sb.append("\n========================================================");
		
		return sb.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.verNum = in.readInt();
		this.edgeNum = in.readLong();
		this.verMinId = in.readInt();
		this.verMaxId = in.readInt();
		this.cachePerTask = in.readInt();
		
		this.taskNum = in.readInt();
		this.taskIds = new int[taskNum];
		this.rangeMins = new int[taskNum];
		this.rangeMaxs = new int[taskNum];
		this.ports = new int[taskNum];
		this.hostNames = new String[taskNum];
		for (int i = 0; i < taskNum; i++) {
			taskIds[i] = in.readInt();
			rangeMins[i] = in.readInt();
			rangeMaxs[i] = in.readInt();
			ports[i] = in.readInt();
			hostNames[i] = Text.readString(in);
		}
		
		this.skGraph = new GlobalSketchGraph();
		this.skGraph.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.verNum);
		out.writeLong(this.edgeNum);
		out.writeInt(this.verMinId);
		out.writeInt(this.verMaxId);
		out.writeInt(this.cachePerTask);
		
		out.writeInt(this.taskNum);
		for (int i = 0; i < taskNum; i++) {
			out.writeInt(taskIds[i]);
			out.writeInt(rangeMins[i]);
			out.writeInt(rangeMaxs[i]);
			out.writeInt(ports[i]);
			Text.writeString(out, hostNames[i]);
		}
		
		this.skGraph.write(out);
	}
}
