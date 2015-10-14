/**
 * NeuSoft Termite System
 * copyright 2012-2010
 */
package org.apache.hama.myhama.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Counters.
 * This class contains relative {@link COUNTER} counters. 
 * 
 * @author WangZhigang
 * @version 0.1
 */
public class Counters implements Writable {
	
	public static enum COUNTER {
		/** counters of vertices */
		Ver_Read, //load from local disk
		Ver_Act,  //active vertices (may be updated)
		Ver_Upd,  //has been updated
		
		/** counters of edges */
		Edge_Read,     //load from local disk (adjacency list or EBlock)
		Fragment_Read, //useful for style.Pull
		
		/** counters of messages */
		Msg_Pro,      //produced messages
		Msg_Rec,      //received messages (from local and remote tasks)
		Msg_Init_Net, //original network messages at the sender-side
		Msg_Net,      //network messages after being combined at the sender-side if possible
		Msg_Disk,     //messages resident on disk (only for push)
		
		/** counters of runtime */
		Time_Pull, //runtime of pulling msgs from source vertices
		Time_Ite,  //runtime of one whole iteration
		
		/** counters of io_bytes */
		Byte_Push,  //io_bytes under "PUSH" model, accurate or estimated
		Byte_Pull,  //io_bytes under "PULL" model, accurate or estimated
		Byte_Total, //io_bytes of one iteration, accurate
		
		/** counters of memory */
		Mem_Used,                //memory size used during iteration
		Mem_Used_PushEldSendBuf  //memory size used in Push, excluding sendBuf
	}
	
	public static int SIZE = COUNTER.values().length;
	
	private long[] counters;
	
	public Counters() {
		this.counters = new long[SIZE];
		this.clearValues();
	}
	
	/**
	 * Add the value of one counter and return the new summary.
	 * If the counter does not exist, throw an Exception.
	 * 
	 * @param name Enum<?> {@link COUNTER}
	 * @param value long
	 */
	public void addCounter(Enum<?> name, long value) {
		this.counters[name.ordinal()] += value;
	}
	
	/**
	 * Add all values of a given {@link Counters} other to the current one.
	 * @param other
	 */
	public void addCounters(Counters other) {
		for (Enum<?> name: COUNTER.values()) {
			this.counters[name.ordinal()] += other.getCounter(name);
		}
	}
	
	/**
	 * Return the value of a counter.
	 * 
	 * @param name Enum<?> {@link COUNTER}
	 * @return value long
	 */
	public long getCounter(Enum<?> name) {
		return this.counters[name.ordinal()];
	}

	/**
	 * Clear values of all {@link COUNTER}s.
	 * That means the value is set to zero.
	 */
	public void clearValues() {
		Arrays.fill(this.counters, 0L);
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("Counters:");
		for (Enum<?> name: COUNTER.values()) {
			sb.append("\n");
			sb.append(name); sb.append("=");
			sb.append(this.counters[name.ordinal()]);
		}
		
		return sb.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		for (int i = 0; i < SIZE; i++) {
			COUNTER name = WritableUtils.readEnum(in, COUNTER.class);
			this.counters[name.ordinal()] = in.readLong();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		for (Enum<?> name: COUNTER.values()) {
			WritableUtils.writeEnum(out, name);
			out.writeLong(this.counters[name.ordinal()]);
		}
	}
}
