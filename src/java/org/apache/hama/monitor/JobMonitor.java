package org.apache.hama.monitor;

import java.util.ArrayList;

import org.apache.hama.myhama.util.Counters;
import org.apache.hama.myhama.util.Counters.COUNTER;
import org.apache.hama.myhama.util.MiniCounters;
import org.apache.hama.myhama.util.MiniCounters.MINICOUNTER;

public class JobMonitor {
	private int[] verNumOfTasks;
	private long[] edgeNumOfTasks;
	private long loadByte;
	
	/** pre-defined aggregator value at every superstep */
	private ArrayList<Float> agg_list;
	/** built-in counter values at every superstep */
	private ArrayList<Counters> counters_list;
	
	/** elapsed time since the last checkpoint, i.e., recovery overhead */
	private double accumulatedRuntime;
	/** number of completed checkpoints */
	private int numOfCkps;
	/** total runtime for "numOfCkps" checkpoints */
	private double timeOfArchiveCkp;
	/** how long it takes to be aware of existing failures */
	private double timeOfFindFailure;
	/** runtime of loading input graph & ckp during failure recovery */
	private double timeOfReloadData;
	
	/** aggregators collected when recovering failures */
	private ArrayList<Float> agg_list_recovery;
	/** counters collected when recovering failures */
	private ArrayList<Counters> counters_list_recovery;
	
	private ArrayList<MiniCounters> minicounters_list;
	
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
		
		this.accumulatedRuntime = 0;
		this.numOfCkps = 0;
		this.timeOfArchiveCkp = 0;
		this.timeOfFindFailure = 0;
		this.timeOfReloadData = 0;
		this.agg_list_recovery = new ArrayList<Float>();
		this.counters_list_recovery = new ArrayList<Counters>();
		
		this.minicounters_list = new ArrayList<MiniCounters>();
	}
	
	/**
	 * Accumulate checkpoint runtime to compute the average overhead. 
	 * Meanwhile, accumulated normal iterative computation runtime 
	 * is reset to zero.
	 * @param time
	 */
	public void accumulateCheckPointTime(double time) {
		this.numOfCkps++;
		this.timeOfArchiveCkp += time;
		resetAccumulatedRuntime();
	}
	
	/**
	 * Compute the average overhead per checkpoint.
	 * @return
	 */
	private double averageCheckpointTime() {
		if (this.numOfCkps > 0) {
			return (this.timeOfArchiveCkp/this.numOfCkps);
		} else {
			return 0.0;
		}
	}
	
	/**
	 * Accumulate iterative computation runtime since the last 
	 * checkpoint. The accumulated runtime indicates the recovery 
	 * overhead under Constants.CheckPoint.Policy.CompleteRecovery.
	 * @param time
	 */
	public void accumulateRuntime(double time) {
		this.accumulatedRuntime += time;
	}
	
	/**
	 * Return accumulated iterative computation runtime since the 
	 * last checkpoint (i.e., the overhead of complete recovery).
	 * @return
	 */
	private double getAccumulatedRuntime() {
		return this.accumulatedRuntime;
	}
	
	/**
	 * Reset accumulated iterative computation runtime to zero when 
	 * a new checkpoint has already been archived, so as to accumulate 
	 * new computation runtime since this checkpoint.
	 */
	private void resetAccumulatedRuntime() {
		this.accumulatedRuntime = 0;
	}
	
	/**
	 * A new dynamic checkpoint is required only when recovering failures 
	 * costs (accumulated iteration runtime) more than archiving this 
	 * checkpoint (the average checkpoint overhead). 
	 * @return
	 */
	public boolean isDynCheckPointRequired() {
		return (averageCheckpointTime() <= getAccumulatedRuntime());
	}
	
	/**
	 * How long does it takes for {@link JobInProgress} to be aware of 
	 * some existing task failures.
	 * @param time
	 */
	public void setTimeOfFindFailure(double time) {
		this.timeOfFindFailure = time;
	}
	
	/**
	 * How long does it takes for new launched tasks to load input graph 
	 * and re-build corresponding VEBlocks (also the checkpoint file).
	 * @param time
	 */
	public void setTimeOfReloadData(double time) {
		this.timeOfReloadData = time;
	}
	
	public void registerInfo(int taskId, TaskInformation tif) {
		this.loadByte += tif.getLoadByte();
		this.verNumOfTasks[taskId] = tif.getVerNum();
		this.edgeNumOfTasks[taskId] = tif.getEdgeNum();
	}
	
	/**
	 * Update pre-defined aggregator and counters during failure-free 
	 * excution. Local values reported by every task are automatically 
	 * accumulated to compute global values.
	 * @param curIteNum
	 * @param taskId
	 * @param aggTask
	 * @param counters
	 */
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
	
	public synchronized void updateMiniMonitor(int curIteNum, int taskId, 
			MiniCounters minicounters) {
		if (this.minicounters_list.size() < curIteNum) { //first task
			this.minicounters_list.add(minicounters);
		} else { //remaining tasks, just add the value
			int idx = curIteNum - 1;
			MiniCounters acc_minicounters = this.minicounters_list.get(idx);
			for (Enum<?> name: MINICOUNTER.values()) {
				acc_minicounters.addCounter(name, minicounters.getCounter(name));
			}
		}
	}
	
	public MiniCounters getMiniCounters(int curIteNum) {
		return this.minicounters_list.get((curIteNum-1));
	}
	
	/**
	 * Reports from some normal tasks have been accumulated when failures 
	 * happen. Now these reports will be deleted for correctness.
	 * @param curIteNum
	 */
	public synchronized void rollbackMonitor(int curIteNum) {
		if (!this.agg_list.isEmpty() 
				&& this.agg_list.size()==curIteNum) {
			this.agg_list.remove(curIteNum-1);
			this.counters_list.remove(curIteNum-1);
		}
	}
	
	/**
	 * Update pre-defined aggregator and counters when recovering failures. 
	 * Local values reported by every task are automatically accumulated to 
	 * compute global values.
	 * @param size
	 * @param taskId
	 * @param aggTask
	 * @param counters
	 */
	public synchronized void updateMonitorRecovery(int size, int taskId, 
			float aggTask, Counters counters) {
		if (this.agg_list_recovery.size()>size 
				|| this.counters_list_recovery.size()>size) {
			this.agg_list_recovery.clear();
			this.counters_list_recovery.clear();
		}
				
		if (this.agg_list_recovery.size() == (size-1)) { 
			//first task
			this.agg_list_recovery.add(aggTask);
			this.counters_list_recovery.add(counters);
		} else if (this.agg_list_recovery.size() == size){ 
			//remaining tasks, just add the value
			int idx = size - 1;
			this.agg_list_recovery.set(idx, this.agg_list_recovery.get(idx)+aggTask);
			Counters acc_counters = this.counters_list_recovery.get(idx);
			for (Enum<?> name: COUNTER.values()) {
				acc_counters.addCounter(name, counters.getCounter(name));
			}
		}
	}
	
	/**
	 * Return the global summation aggregator computed in failure-free execution.
	 * @param curIteNum
	 * @return
	 */
	public float getAgg(int curIteNum) {
		return this.agg_list.get(curIteNum-1);
	}
	
	/**
	 * Return the global summation aggregator computed in failure recovery.
	 * @param size
	 * @return
	 */
	public float getAggRecovery(int size) {
		if (this.agg_list_recovery.size()==size && size>0) {
			return this.agg_list_recovery.get(size-1);
		} else {
			return 0.0f;
		}
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
	
	public long getByteOfVertInPull(int curIteNum) {
		return this.counters_list.get(curIteNum-1).getCounter(COUNTER.Byte_Pull_Vert);
	}
	
	public void addByteOfPush(int curIteNum, long bytes) {
		int idx = curIteNum - 1;
		Counters new_counters = this.counters_list.get(idx);
		new_counters.addCounter(COUNTER.Byte_Push, bytes);
	}
	
	public long getByteOfPush(int curIteNum) {
		return this.counters_list.get(curIteNum-1).getCounter(COUNTER.Byte_Push);
	}
	
	private String printCounterInfo(Enum<?> name, boolean isRecovery) {
		StringBuffer sb = new StringBuffer();
		sb.append("\n");
		if (isRecovery) {
			sb.append("Recovery_");
		}
		sb.append(name);
		sb.append("\n[");
		
		if (isRecovery) {
			for (Counters counters: this.counters_list_recovery) {
				sb.append(counters.getCounter(name));
				sb.append(",");
			}
			if (this.counters_list_recovery.size() > 0) {
				sb.deleteCharAt(sb.length()-1); //delete the last ","
			}
		} else {
			for (Counters counters: this.counters_list) {
				sb.append(counters.getCounter(name));
				sb.append(",");
			}
			if (this.counters_list.size() > 0) {
				sb.deleteCharAt(sb.length()-1); //delete the last ","
			}
		}
		
	    sb.append("]");
	    return sb.toString();
	}
	
	private String printMiniCounterInfo(Enum<?> name) {
		StringBuffer sb = new StringBuffer();
		sb.append("\nMini_");
		sb.append(name);
		sb.append("\n[");
		
		for (MiniCounters counters: this.minicounters_list) {
			sb.append(counters.getCounter(name));
			sb.append(",");
		}
		if (this.minicounters_list.size() > 0) {
			sb.deleteCharAt(sb.length()-1); //delete the last ","
		}
		
	    sb.append("]");
	    return sb.toString();
	}
	
	public String printJobMonitor(int currIterNum) {
		StringBuffer sb = new StringBuffer();
	    
		sb.append("\n   TimeOfFindFailure = ");
		sb.append(this.timeOfFindFailure);
		sb.append(" sec");
		
		sb.append("\n   TimeOfReloadData = ");
		sb.append(this.timeOfReloadData);
		sb.append(" sec\n");
		
		sb.append("\n   TimeOfCheckPoint = ");
		sb.append(this.timeOfArchiveCkp);
		sb.append("sec, #checkpoints = ");
		sb.append(this.numOfCkps);
		
		//sb.append("\nLoadBytes=" + Long.toString(this.loadByte));
		
		//sb.append("\nVertices/task");
		//sb.append("\n"); sb.append(Arrays.toString(this.verNumOfTasks));
		
		//sb.append("\nEdges/task");
		//sb.append("\n"); sb.append(Arrays.toString(this.edgeNumOfTasks));
		
		sb.append(printCounterInfo(COUNTER.Vert_Read, false));
		sb.append(printCounterInfo(COUNTER.Vert_Active, false));
		sb.append(printCounterInfo(COUNTER.Vert_Respond, false));
		
		sb.append(printCounterInfo(COUNTER.Edge_Read, false));
		sb.append(printCounterInfo(COUNTER.Fragment_Read, false));
		
		sb.append(printCounterInfo(COUNTER.Msg_Produced, false));
		sb.append(printCounterInfo(COUNTER.Msg_Received, false));
		sb.append(printCounterInfo(COUNTER.Msg_Net, false));
		sb.append(printCounterInfo(COUNTER.Msg_Net_Actual, false));
		sb.append(printCounterInfo(COUNTER.Msg_Disk, false));
	    
		sb.append(printCounterInfo(COUNTER.Byte_Actual, false));
		sb.append(printCounterInfo(COUNTER.Byte_Push, false));
		sb.append(printCounterInfo(COUNTER.Byte_Pull, false));
		sb.append(printCounterInfo(COUNTER.Byte_Pull_Vert, false));
		sb.append(printCounterInfo(COUNTER.Byte_LOG, false));
		
		//sb.append(printCounterInfo(COUNTER.Mem_Used));
	    
		//sb.append(printCounterInfo(COUNTER.Msg_Produced, true));
		//sb.append(printCounterInfo(COUNTER.Byte_Actual, true));
		//sb.append(printCounterInfo(COUNTER.Byte_Pull_Vert, true));
		//sb.append(printCounterInfo(COUNTER.Byte_Write, true));
		//sb.append("\nRecovery_Aggregator\n");
		//sb.append(agg_list_recovery.toString());
		
		sb.append(printMiniCounterInfo(MINICOUNTER.Msg_Estimate));
		
	    return sb.toString();
	}
}
