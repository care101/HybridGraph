package org.apache.hama.monitor;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hama.myhama.util.Counters;
import org.apache.hama.myhama.util.Counters.COUNTER;

public class JobMonitor {
	private int taskNum;
	private int[] verNumTasks;
	private long[] edgeNumTasks;
	private long loadByte;
	
	private ArrayList<Float> agg_list;
	private ArrayList<Counters> counters_list;
	
	/**
	 * Construct a job monitor.
	 * @param iterNum #iteration
	 * @param taskNum #tasks
	 */
	public JobMonitor (int _iterNum, int _taskNum) {
		this.taskNum = _taskNum;
		this.verNumTasks = new int[this.taskNum];
		this.edgeNumTasks = new long[this.taskNum];
		this.loadByte = 0L;
		
		this.agg_list = new ArrayList<Float>();
		this.counters_list = new ArrayList<Counters>();
	}
	
	public void incLoadByte(long _byte) {
		this.loadByte += _byte;
	}
	
	public void setVerNumTask(int taskId, int verNum) {
		this.verNumTasks[taskId] = verNum;
	}
	
	public void setEdgeNumTask(int tid, long num) {
		this.edgeNumTasks[tid] = num;
	}
	
	public synchronized void updateMonitor(int curIteNum, int taskId, 
			float aggTask, Counters counters) {
		if (this.agg_list.size() < curIteNum) { //first task, record
			this.agg_list.add(aggTask);
			this.counters_list.add(counters);
		} else { //remaining tasks, just add the value
			int idx = curIteNum - 1;
			float new_agg = this.agg_list.get(idx) + aggTask;
			this.agg_list.set(idx, new_agg);
			Counters new_counters = this.counters_list.get(idx);
			for (Enum<?> name: COUNTER.values()) {
				new_counters.addCounter(name, counters.getCounter(name));
			}
		}
	}
	
	public float getAggJob(int curIteNum) {
		return this.agg_list.get(curIteNum-1);
	}
	
	public int getActVerNumJob(int curIteNum) {
		return (int)this.counters_list.get(curIteNum-1).getCounter(COUNTER.Ver_Act);
	}
	
	public long getProMsgNumJob(int curIteNum) {
		return this.counters_list.get(curIteNum-1).getCounter(COUNTER.Msg_Pro);
	}
	
	public long getSavedMsgNumNet(int curIteNum) {
		return (this.counters_list.get(curIteNum-1).getCounter(COUNTER.Msg_Init_Net) 
		    -this.counters_list.get(curIteNum-1).getCounter(COUNTER.Msg_Net));
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
	
	private String getInfo(Enum<?> name) {
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
		sb.append("\n"); sb.append(Arrays.toString(this.verNumTasks));
		
		sb.append("\nEdges/task");
		sb.append("\n"); sb.append(Arrays.toString(this.edgeNumTasks));
		
		sb.append(getInfo(COUNTER.Ver_Read));
		sb.append(getInfo(COUNTER.Ver_Act));
		sb.append(getInfo(COUNTER.Ver_Upd));
		
		sb.append(getInfo(COUNTER.Edge_Read));
		sb.append(getInfo(COUNTER.Fragment_Read));
		
		sb.append(getInfo(COUNTER.Msg_Pro));
		sb.append(getInfo(COUNTER.Msg_Rec));
		sb.append(getInfo(COUNTER.Msg_Init_Net));
		sb.append(getInfo(COUNTER.Msg_Net));
		sb.append(getInfo(COUNTER.Msg_Disk));
	    
		sb.append(getInfo(COUNTER.Byte_Total));
		sb.append(getInfo(COUNTER.Byte_Push));
		sb.append(getInfo(COUNTER.Byte_Pull));
		
		sb.append(getInfo(COUNTER.Mem_Used));
		sb.append(getInfo(COUNTER.Mem_Used_PushEldSendBuf));
	    
	    return sb.toString();
	}
}
