/**
 * Termite System
 * copyright 2012-2010
 */
package org.apache.hama.myhama.util;

import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.comm.CommRouteTable;

/**
 * TaskContext.
 * This class contains information about this task, which can be obtained 
 * by user-defined {@link BSP.taskSetup} function.
 * 
 * @author
 * @version 0.1
 */
public class TaskContext {
	
	private int partitionId;
	private BSPJob job;
	private CommRouteTable commRT;
	
	public TaskContext() {
		
	}
	
	public TaskContext(int partitionId, 
			final BSPJob job, final CommRouteTable commRT) {
		this.partitionId = partitionId;
		this.job = job;
		this.commRT = commRT;
	}
	
	public void setPartitionID(int partitionId) {
		this.partitionId = partitionId;
	}
	
	public int getPartitionID() {
		return this.partitionId;
	}
	
	public void setBSPJob(final BSPJob job) {
		this.job = job;
	}
	
	public final BSPJob getBSPJob() {
		return this.job;
	}
	
	public void setCommRouteTable(final CommRouteTable commRT) {
		this.commRT = commRT;
	}
	
	public final CommRouteTable getCommRouteTable() {
		return this.commRT;
	}
}
