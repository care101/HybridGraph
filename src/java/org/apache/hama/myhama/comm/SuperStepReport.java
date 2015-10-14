package org.apache.hama.myhama.comm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hama.myhama.util.Counters;

public class SuperStepReport implements Writable {
	private Counters counters;
	private float taskAgg;
	
	private int[] actVerNumBucs;
	
	public SuperStepReport() {
		this.counters = new Counters();
	}
	
	public void setCounters(Counters counters) {
		this.counters = counters;
	}
	
	public Counters getCounters() {
		return this.counters;
	}
	
	public float getTaskAgg() {
		return this.taskAgg;
	}
	
	public void setTaskAgg(float taskAgg) {
		this.taskAgg = taskAgg;
	}
	
	public void setActVerNumBucs(int[] nums) {
		this.actVerNumBucs = nums;
	}
	
	public int[] getActVerNumBucs() {
		return this.actVerNumBucs;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.counters.readFields(in);
		this.taskAgg = in.readFloat();
		
		int locNum = in.readInt();
		this.actVerNumBucs = new int[locNum];
		for (int i = 0; i < locNum; i++) {
			this.actVerNumBucs[i] = in.readInt();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.counters.write(out);
		out.writeFloat(this.taskAgg);
		
		int locNum = this.actVerNumBucs.length;
		out.writeInt(locNum);
		for (int i = 0; i < locNum; i++) {
			out.writeInt(this.actVerNumBucs[i]);
		}
	}
	
	@Override
	public String toString() {
		return "[TaskAgg] = " + this.taskAgg 
			+ "\n" + this.counters.toString();
	}
}
