package org.apache.hama.bsp;

import java.io.*;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.bsp.GroomServer.TaskInProgress;
import org.apache.hama.myhama.util.LocalFileOperation;
import org.apache.hama.myhama.util.TaskReportContainer;

/**
 * Manage tasks assigned to the same GroomServer and belong to the same job.
 * @author root
 *
 */
public class LocalJobInProgress {
	public static final Log LOG = LogFactory.getLog(LocalJobInProgress.class);
	private BSPJobID jobId = null;
	private Map<TaskAttemptID, TaskInProgress> tips;
	private BSPTaskTrackerProtocol umbilical;
	private LocalFileOperation localFileOpt;

	public LocalJobInProgress(BSPJobID _jobId, BSPTaskTrackerProtocol _umbilical) 
			throws IOException {
		jobId = _jobId;
		tips = new LinkedHashMap<TaskAttemptID, TaskInProgress>();
		umbilical = _umbilical;
		localFileOpt = new LocalFileOperation();
	}
	
	public boolean containTask(TaskAttemptID tid) {
		return tips.containsKey(tid);
	}
	
	public TaskInProgress getTaskInProgress(TaskAttemptID tid) {
		return tips.get(tid);
	}
	
	public Collection<TaskInProgress> getAllTaskInProgress() {
		return tips.values();
	}
	
	public void addTaskInProgress(TaskAttemptID tid, TaskInProgress tip) {
		tips.put(tid, tip);
	}

	public int getLocalTaskNumber() {
		return tips.size();
	}
	
	/**
	 * Remove a task because of "Killed" or "Successful".
	 * Return ture if all tasks have been removed, false otherwise.
	 * 
	 * @param tid
	 * @return
	 */
	public boolean removeTaskInProgress(TaskAttemptID tid) 
			throws IOException {
		if (tips.containsKey(tid)) {
			tips.remove(tid);
			String dir = umbilical.getLocalTaskDir(jobId, tid); 
			//localFileOpt.deleteDir(new File(dir));
		}
		
		return tips.isEmpty();
	}
	
	/**
	 * Kill all alive tasks and delete all local files.
	 * Return true if close() is done successfully.
	 * 
	 * @throws IOException
	 */
	public boolean close() throws IOException {
		for (TaskInProgress tip: tips.values()) {
			if (tip.runner.isAlive()) {
				tip.killAndCleanup(true);
				LOG.info(tip.getStatus().getTaskId() + " is killed by system");
			}
		}
		
		String dir = umbilical.getLocalJobDir(jobId);
		localFileOpt.deleteDir(new File(dir));
		
		return true;
	}

	public void updateProgress(TaskAttemptID taskId, TaskReportContainer report) {
		tips.get(taskId).updateProgress(report);
	}

	public void runtimeError(TaskAttemptID taskId) 
		throws IOException {
		tips.get(taskId).runtimeError();
	}
}