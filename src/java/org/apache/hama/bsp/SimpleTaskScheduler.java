/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.ipc.WorkerProtocol;

import org.apache.hama.bsp.JobInProgress;

/**
 * A simple task scheduler.
 */
class SimpleTaskScheduler extends TaskScheduler {
	private static final Log LOG = LogFactory.getLog(SimpleTaskScheduler.class);

	public static final String WAIT_QUEUE = "waitQueue";
	public static final String PROCESSING_QUEUE = "processingQueue";
	public static final String FINISHED_QUEUE = "finishedQueue";

	private QueueManager queueManager;
	private volatile boolean initialized;
	private JobListener jobListener;
	private JobProcessor jobProcessor;

	private class JobListener extends JobInProgressListener {
		@Override
		public void jobAdded(JobInProgress job) throws IOException {
			queueManager.initJob(job); // init task

			//lock to WAIT_QUEUE, control(JobProcessor.run()--find(WAIT_QUEUE))
			synchronized (WAIT_QUEUE) {
				queueManager.addJob(WAIT_QUEUE, job);
				queueManager.resortWaitQueue(WAIT_QUEUE);
				LOG.info(job.getJobID() + " is added to Waiting-Queue");
			}
		}

		@Override
		public void jobRemoved(JobInProgress job) throws IOException {
			queueManager.moveJob(PROCESSING_QUEUE, FINISHED_QUEUE, job);
			LOG.info(job.getJobID() + " is moved to Finished-Queue");
			
			Queue<JobInProgress> queue = queueManager.findQueue(FINISHED_QUEUE);
			if (queue.size() > 100) {
				JobInProgress jip = queue.removeJob();
				LOG.warn("Finished-Queue is too long (>100), delete the oldest " 
						+ jip.getJobID());
			}
		}
	}

	private class JobProcessor extends Thread implements Schedulable {
		JobProcessor() {
			super("JobProcess");
		}

		/**
		 * Main logic scheduling task to GroomServer(s). Also, it will move
		 * JobInProgress from Wait Queue to Processing Queue.
		 * @throws Exception 
		 */
		public void run() {
			if (false == initialized) {
				throw new IllegalStateException(
						"SimpleTaskScheduler initialization"
								+ " is not yet finished!");
			}
			
			while (initialized) {
				Queue<JobInProgress> queue;
				// lock to WAIT_QUEUE
				synchronized (WAIT_QUEUE) {
					queue = queueManager.findQueue(WAIT_QUEUE);
				}
				if (null == queue) {
					String errorInfo = WAIT_QUEUE + " does not exist.";
					LOG.error(errorInfo);
					throw new NullPointerException(errorInfo);
				}
				//move a job from the wait queue to the processing queue
				JobInProgress j = null;
				try {
					//LOG.info("trying to get a waiting job");
					j = queue.removeJob();
					//LOG.info("gettting a waiting job with id=" + j.getJobID());
				} catch (Exception e) {
					LOG.debug("Fail to get a job from the WAIT_QUEUE," +
							" and will retry again...");
				}
				
				if (j != null) {
					//wheather or not there are enough task slots
					ClusterStatus cs;
					int runningTasks = 0;
					int retry = 0;
					boolean ready = false;
					while (true) {
						retry++;
						try {
							Thread.sleep(2000);
						} catch (Exception e) {
							LOG.debug(e);
						}
						cs = groomServerManager.getClusterStatus(false);
						runningTasks = cs.getNumOfRunningTasks();
						if (j.getNumBspTask() <= 
							(cs.getNumOfTaskSlots()-runningTasks)) {
							ready = true;
							break;
						}
						
						if (retry >= 10) {
							LOG.warn(j.getJobID() + " scheduling aborts after trying " 
									+ retry + " times.");
							break;
						}
						LOG.warn(j.getNumBspTask() + " task slots are required, but only " 
								+ (cs.getNumOfTaskSlots()-runningTasks) + " are available, try " 
								+ retry + " times...");
					}
					if (!ready) {
						j.failedJob();
						continue;
					}
					
					queueManager.addJob(PROCESSING_QUEUE, j);
					LOG.info(j.getJobID() 
							+ " is moved to Processing-Queue");
					try {
						schedule(j, runningTasks);
					} catch (Exception e) {
						LOG.error("Fail to schedule " + j.getJobID(), e);
					}
				}
			}
		}

		/**
		 * Schedule job to GroomServers immediately.
		 * @param job to be scheduled.
		 * @param int number of running tasks
		 */
		public void schedule(JobInProgress job, int runningTasks) 
				throws Exception {
			int jobStatus = job.getStatus().getRunState(); 
			if (jobStatus!=JobStatus.PREP 
					&& jobStatus!=JobStatus.RESTART) {
				LOG.warn(job.getJobID() + " should not be scheduled");
				return;
			}
			
			int unscheduledTasks = job.getNumBspTask();
			List<JobInProgress> jip_list;
			synchronized (WAIT_QUEUE) {
				jip_list = 
					new ArrayList<JobInProgress>(
							queueManager.findQueue(WAIT_QUEUE).getJobs());
			}
			for (JobInProgress jip : jip_list) {
				unscheduledTasks += jip.getNumBspTask();
			}
			groomServerManager.setWaitTasks(
					unscheduledTasks-job.getNumBspTask());
			
			int totalWorkloads = runningTasks + unscheduledTasks;
			// get the new GroomServerStatus Cache
			Collection<GroomServerStatus> gssList = 
				groomServerManager.groomServerStatusKeySet();
			/** compute the fair factor used in fair scheduler */
			int factor = (int)Math.ceil(totalWorkloads/(double)gssList.size());
			LOG.info("[fair scheduler]: #workloads=" + totalWorkloads 
					+ ", #slaves=" + gssList.size() + ", fair factor=" + factor);
			
			/** schedule some (recovery) or all tasks for this job */
			TaskInProgress[] tips = job.getTaskInProgress(); 
			/** task-scheduling action set per slave */
			HashMap<GroomServerStatus, Directive> schedulingActions = 
				new HashMap<GroomServerStatus, Directive>();
			LOG.info("schedule " + tips.length + " tasks onto " 
					+ gssList.size() + " slaves");
			int index = 0; 
			boolean over = false;
			for (GroomServerStatus gss: gssList) {
				int usedSlots = gss.getNumOfRunningTasks();
				if (usedSlots >= factor) {
					continue;
				} //i am too tired
				
				for (int i = usedSlots; i < factor; i++) {
					Task task = tips[index].getTaskToRun(gss, 
							jobStatus==JobStatus.RESTART);
					LaunchTaskAction action = new LaunchTaskAction(task);
					
					if (schedulingActions.containsKey(gss)) {
						schedulingActions.get(gss).addAction(action);
					} else {
						Directive d = new Directive(
								groomServerManager.currentGroomServerPeers(),
								new ArrayList<GroomServerAction>());
						d.addAction(action);
						schedulingActions.put(gss, d);
					}
					
					//update the GroomServerStatus cache
					gss.setNumOfRunningTasks(gss.getNumOfRunningTasks() + 1);
					LOG.info(task.getTaskAttemptId() + " == " 
							+ gss.getGroomName());
					index++;
					if (index >= tips.length) {
						over = true;
						break;
					}
				}
				
				if (over) {
					break;
				}
			}
			
			switch(jobStatus) {
			case JobStatus.PREP: 
				job.setStartTime();
				job.getStatus().setRunState(JobStatus.LOAD);
				LOG.info(job.getJobID() + " is scheduled for running");
				break;
			case JobStatus.RESTART: 
				//job.getStatus().setRunState(JobStatus.RECOVERY);
				LOG.info(job.getJobID() + " is scheduled for recovery");
				break;
			default:
				String warnInfo = "\n ignore the scheduling request for " 
					+ job.getJobID().toString() 
					+ " with status:" + job.getStatus().getRunState();
				LOG.warn(warnInfo);
			}
			
			// dispatch tasks to workers
			for (Entry<GroomServerStatus, Directive> e : 
					schedulingActions.entrySet()) {
				try {
					WorkerProtocol worker = 
						groomServerManager.findGroomServer(e.getKey());
					//long startTime = System.currentTimeMillis();
					worker.dispatch(job.getJobID(), e.getValue());
					//long endTime = System.currentTimeMillis();
					/*LOG.info("schedule to " + e.getKey().getGroomName() 
							+ " cost " + (endTime - startTime) + " ms");*/
				} catch (IOException ioe) {
					LOG.error("fail to dispatch tasks to GroomServer "
							+ e.getKey().getGroomName(), ioe);
				}
			}
		}
	}

	public SimpleTaskScheduler() {
		this.jobListener = new JobListener();
		this.jobProcessor = new JobProcessor();

		//change in version-0.2.4 new function:get the zookeeperaddress in
		// order to connect to zookeeper
		this.conf = new HamaConfiguration();
	}

	@Override
	public void start() {
		this.queueManager = new QueueManager(getConf()); // TODO: need
															// factory?
		this.queueManager.createFCFSQueue(WAIT_QUEUE);
		this.queueManager.createFCFSQueue(PROCESSING_QUEUE);
		this.queueManager.createFCFSQueue(FINISHED_QUEUE);
		groomServerManager.addJobInProgressListener(this.jobListener);
		this.initialized = true;
		this.jobProcessor.start();
	}

	@SuppressWarnings("deprecation")
	@Override
	public void stop() throws Exception {
		this.initialized = false;
		if (null != this.jobListener) {
			groomServerManager.removeJobInProgressListener(this.jobListener);
		}
		this.jobProcessor.stop();
	}

	@Override
	public Collection<JobInProgress> getJobs(String queue) {
		return (queueManager.findQueue(queue)).getJobs();
	}
}
