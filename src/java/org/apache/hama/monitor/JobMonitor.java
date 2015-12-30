package org.apache.hama.monitor;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hama.myhama.util.Counters;
import org.apache.hama.myhama.util.Counters.COUNTER;

public class JobMonitor {
	private int[] verNumOfTasks;
	private long[] edgeNumOfTasks;
	private long loadByte;
	
	private ArrayList<Float> agg_list;
	private ArrayList<Counters> counters_list;
	
	/**
	 * Construct a job monitor.
	 * @param iterNum #iteration
	 * @param taskNum #tasks
	 */
	public JobMonitor (int _iterNum, int _taskNum) {
		this.verNumOfTasks = new int[_taskNum];
		this.edgeNumOfTasks = new long[_taskNum];
		this.loadByte = 0L;
		
		this.agg_list = new ArrayList<Float>();
		this.counters_list = new ArrayList<Counters>();
	}
	
	public void addLoadByte(long _byte) {
		this.loadByte += _byte;
	}
	
	public void setVerNumOfTasks(int taskId, int verNum) {
		this.verNumOfTasks[taskId] = verNum;
	}
	
	public void setEdgeNumOfTasks(int taskId, long num) {
		this.edgeNumOfTasks[taskId] = num;
	}
	
	public synchronized void updateMonitor(int curIteNum, int taskId, 
			float aggTask, Counters counters) {
		if (this.agg_list.size() < curIteNum) { //first task
			this.agg_list.add(aggTask);
			this.counters_list.add(counters);
		} else { //remaining tasks, just add the value
			int idx = curIteNum - 1;
			this.agg_list.set(idx, this.agg_list.get(idx)+aggTask);
			Counters acc_counters = this.counters_list.get(idx);
			for (Enum<?> name: COUNTER.values()) {
				acc_counters.addCounter(name, counters.getCounter(name));
			}
		}
	}
	
	/**
	 * Return the global sum-aggregator.
	 * @param curIteNum
	 * @return
	 */
	public float getAgg(int curIteNum) {
		return this.agg_list.get(curIteNum-1);
	}
	
	public int getActVerNum(int curIteNum) {
		return 
			(int)this.counters_list.get(curIteNum-1).getCounter(COUNTER.Vert_Active);
	}
	
	public long getProducedMsgNum(int curIteNum) {
		return this.counters_list.get(curIteNum-1).getCounter(COUNTER.Msg_Produced);
	}
	
	/**
	 * Return the number of network messages due to combining/concatenating of pull.
	 * @param curIteNum
	 * @return
	 */
	public long getReducedNetMsgNum(int curIteNum) {
		return (this.counters_list.get(curIteNum-1).getCounter(COUNTER.Msg_Net) 
		    -this.counters_list.get(curIteNum-1).getCounter(COUNTER.Msg_Net_Actual));
	}
	
	public long getByteOfPull(int curIteNum) {
		return this.counters_list.get(curIteNum-1).getCounter(COUNTER.Byte_Pull);
	}
	
	public void addByteOfPush(int curIteNum, long bytes) {
		int idx = curIteNum - 1;
		Counters new_counters = this.counters_list.get(idx);
		new_counters.addCounter(COUNTER.Byte_Push, bytes);
	}
	
	public long getByteOfPush(int curIteNum) {
		return this.counters_list.get(curIteNum-1).getCounter(COUNTER.Byte_Push);
	}
	
	private String printCounterInfo(Enum<?> name) {
		StringBuffer sb = new StringBuffer();
		sb.append("\n");
		sb.append(name);
		sb.append("\n[");
		for (Counters counters: this.counters_list) {
			sb.append(counters.getCounter(name));
			sb.append(",");
		}
	    sb.append("0]");
	    return sb.toString();
	}
	
	public String printJobMonitor(int currIterNum) {
		StringBuffer sb = new StringBuffer();
	    
		sb.append("\nLoadBytes=" + Long.toString(this.loadByte));
		
		sb.append("\nVertices/task");
		sb.append("\n"); sb.append(Arrays.toString(this.verNumOfTasks));
		
		sb.append("\nEdges/task");
		sb.append("\n"); sb.append(Arrays.toString(this.edgeNumOfTasks));
		
		sb.append(printCounterInfo(COUNTER.Vert_Read));
		sb.append(printCounterInfo(COUNTER.Vert_Active));
		sb.append(printCounterInfo(COUNTER.Vert_Respond));
		
		sb.append(printCounterInfo(COUNTER.Edge_Read));
		sb.append(printCounterInfo(COUNTER.Fragment_Read));
		
		sb.append(printCounterInfo(COUNTER.Msg_Produced));
		sb.append(printCounterInfo(COUNTER.Msg_Received));
		sb.append(printCounterInfo(COUNTER.Msg_Net));
		sb.append(printCounterInfo(COUNTER.Msg_Net_Actual));
		sb.append(printCounterInfo(COUNTER.Msg_Disk));
	    
		sb.append(printCounterInfo(COUNTER.Byte_Actual));
		sb.append(printCounterInfo(COUNTER.Byte_Push));
		sb.append(printCounterInfo(COUNTER.Byte_Pull));
		
		sb.append(printCounterInfo(COUNTER.Mem_Used));
	    
	    return sb.toString();
	}
}
