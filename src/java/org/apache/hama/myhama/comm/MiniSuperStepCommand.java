package org.apache.hama.myhama.comm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hama.Constants;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class MiniSuperStepCommand implements Writable {
	private Constants.STYLE style;
	
	private int iteNum;
	private double miniQ;
	
	public MiniSuperStepCommand() {
		
	}
	
	/**
	 * Get the style of the next superstep.
	 * @return
	 */
	public Constants.STYLE getStyle() {
		return this.style;
	}
	
	/**
	 * Set the style of the next superstep.
	 * @param commandType
	 */
	public void setStyle(Constants.STYLE _style) {
		this.style = _style;
	}
	
	public void setMiniQ(double _miniQ) {
		this.miniQ = _miniQ;
	}
	
	public void setIteNum(int _iteNum) {
		this.iteNum = _iteNum;
	}
	
	public int getIteNum() {
		return this.iteNum;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		style = WritableUtils.readEnum(in, Constants.STYLE.class);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeEnum(out, style);
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		
		sb.append(this.style);
		sb.append("\t");
		sb.append(miniQ);
		
		return sb.toString();
	}
}
