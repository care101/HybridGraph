package org.apache.hama.myhama.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TaskReportContainer implements Writable {
	
	private float currentProgress = 0.0f;
	private float usedMemory = 0.0f;
	private float totalMemory = 0.0f;
	
	public TaskReportContainer() {
		
	}
	
	/**
	 * Construct a report
	 * @param currentProgress
	 * @param usedMemory
	 * @param totalMemory
	 */
	public TaskReportContainer(float currentProgress, float usedMemory, float totalMemory) {
		this.currentProgress = currentProgress;
		this.usedMemory = usedMemory;
		this.totalMemory = totalMemory;
	}
	
	public float getCurrentProgress() {
		return currentProgress;
	}

	public void setCurrentProgress(float currentProgress) {
		this.currentProgress = currentProgress;
	}

	public float getUsedMemory() {
		return usedMemory;
	}

	public void setUsedMemory(float usedMemory) {
		this.usedMemory = usedMemory;
	}

	public float getTotalMemory() {
		return totalMemory;
	}

	public void setTotalMemory(float totalMemory) {
		this.totalMemory = totalMemory;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.currentProgress = in.readFloat();
		this.usedMemory = in.readFloat();
		this.totalMemory = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(this.currentProgress);
		out.writeFloat(this.usedMemory);
		out.writeFloat(this.totalMemory);
	}
}
