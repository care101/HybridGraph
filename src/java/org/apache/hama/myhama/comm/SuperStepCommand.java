package org.apache.hama.myhama.comm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hama.Constants;
import org.apache.hama.Constants.CommandType;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class SuperStepCommand implements Writable {
	private CommandType commandType;
	private float jobAgg;
	private ArrayList<Integer>[] route;
	
	private int iteStyle;
	private boolean estimatePullByte;
	
	//local variable
	private double metricQ;
	
	public SuperStepCommand() {
		
	}
	
	public void setMetricQ(double _q) {
		this.metricQ = _q;
	}
	
	public double getMetricQ() {
		return this.metricQ;
	}
	
	public CommandType getCommandType() {
		return this.commandType;
	}
	
	public void setCommandType(CommandType commandType) {
		this.commandType = commandType;
	}

	public float getJobAgg() {
		return this.jobAgg;
	}
	
	public void setJobAgg(float jobAgg) {
		this.jobAgg = jobAgg;
	}
	
	public void setRealRoute(ArrayList<Integer>[] _route) {
		this.route = _route;
	}
	
	public ArrayList<Integer>[] getRealRoute() {
		return this.route;
	}
	
	public void setIteStyle(int _style) {
		this.iteStyle = _style;
	}
	
	public int getIteStyle() {
		return this.iteStyle;
	}
	
	public void setEstimatePullByte(boolean flag) {
		this.estimatePullByte = flag;
	}
	
	public boolean isEstimatePullByte() {
		return this.estimatePullByte;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.commandType = WritableUtils.readEnum(in, CommandType.class);
		this.jobAgg = in.readFloat();
		this.iteStyle = in.readInt();
		this.estimatePullByte = in.readBoolean();
		
		int len = in.readInt();
		this.route = new ArrayList[len];
		for (int i = 0; i < len; i++) {
			this.route[i] = new ArrayList<Integer>();
			int size = in.readInt();
			for (int j = 0; j < size; j++) {
				this.route[i].add(in.readInt());
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeEnum(out, this.commandType);
		out.writeFloat(this.jobAgg);
		out.writeInt(this.iteStyle);
		out.writeBoolean(this.estimatePullByte);
		
		out.writeInt(route.length);
		for (int i = 0; i < route.length; i++) {
			out.writeInt(route[i].size());
			for (int tid: route[i]) {
				out.writeInt(tid);
			}
		}
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("command="); sb.append(this.commandType);
		sb.append("\tsum-agg="); sb.append(this.jobAgg);
		sb.append("\tstyle="); 
		if (this.iteStyle == Constants.STYLE.Pull) {
			sb.append("Pull");
		} else {
			sb.append("Push");
		}
		return sb.toString();
	}
}
