package org.apache.hama.myhama.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Task report contains the computation progress within an 
 * iteration, memory footprint, status of this task (completed 
 * or not).
 */
public class TaskReportContainer implements Writable {
	/** workload per iteration, now it is #local buckets */
	private int fullWorkload = 0;
	/** how many local buckets have been computed */
	private int completedWorkload = 0;
	/** computation progress within one iteration */
	private float lastProg = 0.0f, curProg = 0.0f;
	/** estimate the current memory footprint (Java) */
	private float totalMem = 0.0f, usedMem = 0.0f; 
	/** this task is done or not? */
	private boolean isDone = false;
	
	public TaskReportContainer() {
		
	}
	
	public void clearBefIte() {
		completedWorkload = 0;
		isDone = false;
	}
	
	public void setFullWorkload(int full) {
		fullWorkload = full;
	}
	
	public void completeWorkload() {
		completedWorkload++;
	}
	
	public void setDone(boolean _isDone) {
		isDone = _isDone;
	}
	
	public boolean isDone() {
		return isDone;
	}
	
	public float getCurrentProgress() {
		return curProg;
	}

	public void updateCurrentProgress() {
		curProg = completedWorkload/(float)fullWorkload;
	}
	
	public void updateCurrentProgress(float _prog) {
		curProg = _prog;
	}
	
	public boolean isProgressUpdated() {
		return !(lastProg==curProg);
	}
	
	public void finalizeCurrentProgress() {
		lastProg = curProg;
	}

	public float getUsedMemory() {
		return usedMem;
	}

	public void updateMemoryInfo(float _usedMem, float _totalMem) {
		usedMem = _usedMem;
		totalMem = _totalMem;
	}

	public float getTotalMemory() {
		return totalMem;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		curProg = in.readFloat();
		usedMem = in.readFloat();
		totalMem = in.readFloat();
		isDone = in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(curProg);
		out.writeFloat(usedMem);
		out.writeFloat(totalMem);
		out.writeBoolean(isDone);
	}
}
