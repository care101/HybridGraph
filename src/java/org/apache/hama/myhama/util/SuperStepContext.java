/**
 * copyright 2012-2010
 */
package org.apache.hama.myhama.util;

import org.apache.hama.bsp.BSPJob;

/**
 * SuperStepContext.
 * This class contains information about this task, which can be obtained 
 * by user-defined {@link BSP.superstepSetup} function.
 * 
 * @author 
 * @version 0.1
 */
public class SuperStepContext {
	
	private BSPJob job;
	
	public SuperStepContext() {
		
	}
	
	public SuperStepContext(final BSPJob job) {
		this.job = job;
	}
	
	public void setBSPJob(final BSPJob job) {
		this.job = job;
	}
	
	public final BSPJob getBSPJob() {
		return this.job;
	}
}
