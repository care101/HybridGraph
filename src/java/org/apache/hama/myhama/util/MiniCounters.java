/**
 * Termite System
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
 * MiniCounters.
 * This class contains relative {@link MINICOUNTER} counters. 
 * 
 * @author 
 * @version 0.1
 */
public class MiniCounters implements Writable {
	
	public static enum MINICOUNTER {
		/** estimated number of messages at the first mini-superstep */
		Msg_Estimate,
		/** io bytes of randomly reading source vertices when pulling messages */
		Byte_RandReadVert, 
		/** io bytes of sequentially reading source vertices when pushing messages at the second mini-superstep */
		Byte_SeqReadVert, 
		/** io bytes of sequentially reading edges in PUSH */
		Byte_PushEdge, 
		/** io bytes of sequentially reading edges, auxiliary data and GraphInfo in PULL */
		Byte_PullSeqRead
	}
	
	public static int SIZE = MINICOUNTER.values().length;
	
	private long[] counters;
	
	public MiniCounters() {
		this.counters = new long[SIZE];
		this.clearValues();
	}
	
	/**
	 * Add the value of one counter and return the new summary.
	 * If the counter does not exist, throw an Exception.
	 * 
	 * @param name Enum<?> {@link MINICOUNTER}
	 * @param value long
	 */
	public void addCounter(Enum<?> name, long value) {
		this.counters[name.ordinal()] += value;
	}
	
	/**
	 * Add all values of a given {@link MINICounters} other to the current one.
	 * @param other
	 */
	public void addCounters(MiniCounters other) {
		for (Enum<?> name: MINICOUNTER.values()) {
			this.counters[name.ordinal()] += other.getCounter(name);
		}
	}
	
	/**
	 * Return the value of a counter.
	 * 
	 * @param name Enum<?> {@link MINICOUNTER}
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
		StringBuffer sb = new StringBuffer("Mini-Counters:");
		for (Enum<?> name: MINICOUNTER.values()) {
			sb.append("\n");
			sb.append(name); sb.append("=");
			sb.append(this.counters[name.ordinal()]);
		}
		
		return sb.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		for (int i = 0; i < SIZE; i++) {
			MINICOUNTER name = WritableUtils.readEnum(in, MINICOUNTER.class);
			this.counters[name.ordinal()] = in.readLong();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		for (Enum<?> name: MINICOUNTER.values()) {
			WritableUtils.writeEnum(out, name);
			out.writeLong(this.counters[name.ordinal()]);
		}
	}
}
