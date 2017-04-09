package org.apache.hama.myhama.comm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hama.Constants;
import org.apache.hama.Constants.CommandType;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class SuperStepCommand implements Writable {
	private CommandType commandType;
	private float jobAgg;
	private ArrayList<Integer>[] route;
	
	private int preIteStyle;
	private int curIteStyle;
	private boolean estimatePullByte;
	
	private int iteNum;
	private int ckpVersion;
	
	//local variable
	private double metricQ;
	private double iteTime;
	private HashSet<Integer> failedTaskIds;
	
	public SuperStepCommand() {
		
	}
	
	public void setIterationTime(double time) {
		iteTime = time;
	}
	
	public double getIterationTime() {
		return iteTime;
	}
	
	/**
	 * Reduce the storage space requirement by releasing the memory 
	 * space of some variables, such as route table (ArrayList[]).
	 */
	public void compact() {
		route = null;
	}
	
	/**
	 * Copy some variables provided by the given command.
	 * @param command
	 */
	public void copy(SuperStepCommand command) {
		jobAgg = command.getJobAgg();
		preIteStyle = command.getPreIteStyle();
		curIteStyle = command.getCurIteStyle();
		estimatePullByte = command.isEstimatePullByte();
		metricQ = command.getMetricQ();
	}
	
	public void setIteNum(int num) {
		iteNum = num;
	}
	
	/**
	 * Get the counter of the next superstep.
	 * @return
	 */
	public int getIteNum() {
		return iteNum;
	}
	
	public void setAvailableCheckPointVersion(int version) {
		ckpVersion = version;
	}
	
	public int getAvailableCheckPointVersion() {
		return ckpVersion;
	}
	
	public void setFailedTaskIds(HashSet<Integer> _failed) {
		failedTaskIds = new HashSet();
		for (int id: _failed) {
			failedTaskIds.add(id);
		}
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
	
	public void setIteStyle(int _preStyle, int _curStyle) {
		this.preIteStyle = _preStyle;
		this.curIteStyle = _curStyle;
	}
	
	public int getCurIteStyle() {
		return this.curIteStyle;
	}
	
	public int getPreIteStyle() {
		return this.preIteStyle;
	}
	
	public void setEstimatePullByte(boolean flag) {
		this.estimatePullByte = flag;
	}
	
	public boolean isEstimatePullByte() {
		return this.estimatePullByte;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		commandType = 
			WritableUtils.readEnum(in, CommandType.class);
		jobAgg = in.readFloat();
		preIteStyle = in.readInt();
		curIteStyle = in.readInt();
		estimatePullByte = in.readBoolean();
		iteNum = in.readInt();
		ckpVersion = in.readInt();
		
		int len = in.readInt();
		route = new ArrayList[len];
		for (int i = 0; i < len; i++) {
			route[i] = new ArrayList<Integer>();
			int size = in.readInt();
			for (int j = 0; j < size; j++) {
				route[i].add(in.readInt());
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeEnum(out, commandType);
		out.writeFloat(jobAgg);
		out.writeInt(preIteStyle);
		out.writeInt(curIteStyle);
		out.writeBoolean(estimatePullByte);
		out.writeInt(iteNum);
		out.writeInt(ckpVersion);
		
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
		sb.append("command="); 
		sb.append(this.commandType);
		
		sb.append("\tsum-agg="); 
		sb.append(this.jobAgg);
		
		sb.append("\tstyle="); 
		if (this.curIteStyle == Constants.STYLE.Pull) {
			sb.append("PULL");
		} else {
			sb.append("PUSH");
		}
		
		sb.append("\t");
		sb.append(iteTime);
		
		sb.append("\t");
		sb.append(metricQ);
		
		if (failedTaskIds != null) {
			sb.append("\tfailures=[");
			for (Integer id: failedTaskIds) {
				sb.append(id.toString());
				sb.append(",");
			}
			sb.deleteCharAt(sb.length()-1); //delete the last ","
			sb.append("]");
		}
		return sb.toString();
	}
}
