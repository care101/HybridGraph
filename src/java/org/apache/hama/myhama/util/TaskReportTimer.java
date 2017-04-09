package org.apache.hama.myhama.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.BSPTaskTrackerProtocol;
import org.apache.hama.bsp.BSPTask;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * Report the real-time progress of this task.
 * Two phase:
 *   (1)load graph data: #loaded vertices / total vertices of this task;
 *   (2)computation: #processed buckets / total #buckets;
 * @author 
 *
 */
public class TaskReportTimer extends Thread {
	private static final Log LOG = LogFactory.getLog(TaskReportTimer.class);
	private BSPJobID jobId;
	private TaskAttemptID taskId;
	private BSPTask task;
	private BSPTaskTrackerProtocol workerAgent;
	private float progress = 0.0f;
	private int interval;
	
	public TaskReportTimer(BSPJobID _jobId, TaskAttemptID _taskId, 
			BSPTask _task, int _interval) {
		this.jobId = _jobId;
		this.taskId = _taskId;
		this.task = _task;
		this.interval = _interval;
	}
	
	public void setAgent(BSPTaskTrackerProtocol _workerAgent) {
		this.workerAgent = _workerAgent;
	}
	
	private void report() throws Exception {
		TaskReportContainer trc = this.task.getProgress();
		if ((trc != null) && (progress != trc.getCurrentProgress())) {
			this.workerAgent.ping(this.jobId, this.taskId, trc);
		}
	}
	
	public void force() {
		try {
			report();
		} catch (Exception e) {
			LOG.error("TaskReprotTimer.force()", e);
			System.exit(-1);
		}
	}
	
	@Override
	public void run() {
		while (true) {
			try {
				Thread.sleep(this.interval);
				report();
			} catch (Exception e) {
				LOG.error("TaskReprotTimer", e);
				System.exit(-1);
			}
		}
	}
}
