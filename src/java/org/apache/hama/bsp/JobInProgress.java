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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hama.Constants;
import org.apache.hama.Constants.CommandType;
import org.apache.hama.bsp.BSPJobClient.RawSplit;
import org.apache.hama.bsp.TaskStatus.State;
import org.apache.hama.ipc.CommunicationServerProtocol;
import org.apache.hama.monitor.JobInformation;
import org.apache.hama.monitor.JobMonitor;
import org.apache.hama.monitor.TaskInformation;
import org.apache.hama.myhama.comm.SuperStepCommand;
import org.apache.hama.myhama.comm.SuperStepReport;
import org.apache.hama.myhama.util.JobLog;

/**
 * JobInProgress maintains all the info for keeping a Job on the straight and
 * narrow. It keeps its JobProfile and its latest JobStatus, plus a set of
 * tables for doing bookkeeping of its Tasks.ss
 */
class JobInProgress {
	/**
	 * Used when the a kill is issued to a job which is initializing.
	 */
	static class KillInterruptedException extends InterruptedException {
		private static final long serialVersionUID = 1L;

		public KillInterruptedException(String msg) {
			super(msg);
		}
	}

	JobLog MyLOG;
	private static final Log LOG = LogFactory.getLog(JobInProgress.class);
	private static int ONE_KB = 1024;
	boolean tasksInited = false, jobInited = false;

	JobProfile profile;
	Path jobFile = null, localJobFile = null, localJarFile = null;
	LocalFileSystem localFs;
	
	Configuration conf;
	BSPJob job;
	volatile JobStatus status;
	BSPJobID jobId;
	final BSPMaster master;
	TaskInProgress tips[] = new TaskInProgress[0];

	private int taskNum = 0, maxIteNum = 1, curIteNum = 0;
	private String priority = Constants.PRIORITY.NORMAL;//default
	
	private HashMap<Integer, CommunicationServerProtocol> comms = 
		new HashMap<Integer, CommunicationServerProtocol>();
	private AtomicInteger reportCounter;

	private JobInformation jobInfo;
	private JobMonitor jobMonitor;
	private String[] taskToWorkerName;

	/** how long of scheduling, loading graph, saving result */
	private double scheTaskTime, loadDataTime, saveDataTime;
	/** the time of submitting, scheduling and finishing time */
	private long submitTime, startTime, finishTime;
	private long startTimeIte = 0;

	private ConcurrentHashMap<TaskAttemptID, Float> Progress = 
		new ConcurrentHashMap<TaskAttemptID, Float>();
	
	private int preIteStyle, curIteStyle;
	private int switchCounter = 0;
	private int byteOfOneMessage = 0;
	private boolean isAccumulated = false;
	private int recMsgBuf = 0;
	/**
	 * Local cluster with HDD:
	 *   1) randWrite = 1210KB/s (tested by fio)
	 *   2) randRead = 1205KB/s  (tested by fio)
	 *   3) seqWrite = 2414KB/s  (tested by fio)
	 *   4) seqRead = 2415KB/s   (tested by fio)
	 *   5) network = 112MB/s    (tested by iperf)
	 * Amazon cluster with SSD (c3.xlarge, 30GB SSD):
	 *   1) randWrite = 18631KB/s (tested by fio)
	 *   2) randRead = 18613KB/s  (tested by fio)
	 *   3) seqWrite = 18730KB/s  (tested by fio)
	 *   4) seqRead = 18708KB/s   (tested by fio)
	 *   5) network = 116MB/s     (tested by iperf)
	 */
	private float randWriteSpeed = 1210*ONE_KB;
	private float randReadSpeed = 1205*ONE_KB;
	private float seqWriteSpeed = 2414*ONE_KB;
	private float seqReadSpeed = 2415*ONE_KB;
	private float netSpeed = 112*ONE_KB*ONE_KB;    
	private double lastCombineRatio = 0.0;

	public JobInProgress(BSPJobID _jobId, Path _jobFile, BSPMaster _master,
			Configuration _conf) throws IOException {
		this.randReadSpeed = _conf.getFloat(Constants.HardwareInfo.RD_Read_Speed, 
				Constants.HardwareInfo.Def_RD_Read_Speed)*ONE_KB;
		this.randWriteSpeed = _conf.getFloat(Constants.HardwareInfo.RD_Write_Speed, 
				Constants.HardwareInfo.Def_RD_Write_Speed)*ONE_KB;
		this.seqReadSpeed = _conf.getFloat(Constants.HardwareInfo.Seq_Read_Speed, 
				Constants.HardwareInfo.Def_Seq_Read_Speed)*ONE_KB;
		this.seqWriteSpeed = _conf.getFloat(Constants.HardwareInfo.Seq_Write_Speed, 
				Constants.HardwareInfo.Def_Seq_Write_Speed)*ONE_KB;
		this.netSpeed = _conf.getFloat(Constants.HardwareInfo.Network_Speed, 
				Constants.HardwareInfo.Def_Network_Speed)*ONE_KB*ONE_KB;
		LOG.info("hardware info: RD_Read_Speed=" + this.randReadSpeed/ONE_KB + "KB/s" 
				+ ", RD_Write_Speed=" + this.randWriteSpeed/ONE_KB + "KB/s" 
				+ ", Seq_Read_Speed=" + this.seqReadSpeed/ONE_KB + "KB/s"
				+ ", Seq_Write_Speed=" + this.seqWriteSpeed/ONE_KB + "KB/s"
				+ ", Network_Speed=" + this.netSpeed/(ONE_KB*ONE_KB) + "MB/s");
		
		jobId = _jobId; master = _master; MyLOG = new JobLog(jobId);
		localFs = FileSystem.getLocal(_conf); jobFile = _jobFile;
		localJobFile = master.getLocalPath(BSPMaster.SUBDIR + "/" + jobId + ".xml");
		localJarFile = master.getLocalPath(BSPMaster.SUBDIR + "/" + jobId + ".jar");
		Path jobDir = master.getSystemDirectoryForJob(jobId);
		FileSystem fs = jobDir.getFileSystem(_conf);
		fs.copyToLocalFile(jobFile, localJobFile);
		job = new BSPJob(jobId, localJobFile.toString());
		conf = job.getConf();
		
		priority = job.getPriority();
		profile = new JobProfile(job.getUser(), jobId, 
				jobFile.toString(), job.getJobName());
		String jarFile = job.getJar();
		if (jarFile != null) {
			fs.copyToLocalFile(new Path(jarFile), localJarFile);
		}
		
		curIteNum = 0; 
		maxIteNum = job.getNumSuperStep(); 
		taskNum = job.getNumBspTask();
		recMsgBuf = job.getMsgRecBufSize();
		jobMonitor = new JobMonitor(maxIteNum, taskNum);
		initialize();
		
		status = new JobStatus(jobId, profile.getUser(),
				new float[] { 0.0f, 0.0f }, new int[] {-1, -1}, 0, JobStatus.PREP);
		status.setTotalSuperStep(maxIteNum); status.setTaskNum(taskNum);
		status.setUsername(job.getUser());
		submitTime = System.currentTimeMillis();
		status.setSubmitTime(submitTime);
		
		this.preIteStyle = job.getStartIteStyle();
		this.curIteStyle = this.preIteStyle;
		this.reportCounter = new AtomicInteger(0);
	}

	private void initialize() {
		jobInfo = new JobInformation(this.job, this.taskNum);
		taskToWorkerName = new String[this.taskNum];
	}

	public JobProfile getProfile() {
		return profile;
	}

	public JobStatus getStatus() {
		return status;
	}

	public long getSubmitTime() {
		return submitTime;
	}

	public long getStartTime() {
		return startTime;
	}

	/**
	 * This job begin to be scheduled.
	 * Record the startTime;
	 * Compute the waitScheTime; 
	 */
	public void setStartTime() {
		this.startTime = System.currentTimeMillis();
		this.status.setStartTime(this.startTime);
		this.scheTaskTime = this.startTime - this.submitTime;
	}

	public String getPriority() {
		return priority;
	}

	public int getNumBspTask() {
		return this.taskNum;
	}

	public TaskInProgress[] getTaskInProgress() {
		return tips;
	}

	public long getFinishTime() {
		return finishTime;
	}

	/**
	 * @return the number of desired tasks.
	 */
	public int desiredBSPTasks() {
		return this.taskNum;
	}

	/**
	 * @return The JobID of this JobInProgress.
	 */
	public BSPJobID getJobID() {
		return jobId;
	}

	public synchronized TaskInProgress findTaskInProgress(TaskID id) {
		if (areTasksInited()) {
			for (TaskInProgress tip : tips) {
				if (tip.getTaskId().equals(id)) {
					return tip;
				}
			}
		}
		return null;
	}

	public synchronized boolean areTasksInited() {
		return this.tasksInited;
	}

	public String toString() {
		return "jobName:" + profile.getJobName() + "\n" + "submit user:"
				+ profile.getUser() + "\n" + "JobId:" + jobId + "\n"
				+ "JobFile:" + jobFile + "\n";
	}
	
	public void updateWorker(int i, String _taskId, String _worker) {
		this.taskToWorkerName[i] = _taskId + "==" + _worker;
	}

	public synchronized void initTasks() throws IOException {
		if (tasksInited) {
			return;
		}
		//change in version=0.2.3 read the input split info from HDFS
		Path sysDir = new Path(this.master.getSystemDir());
		FileSystem fs = sysDir.getFileSystem(conf);
		DataInputStream splitFile = fs.open(new Path(conf.get("bsp.job.split.file")));
		RawSplit[] splits;
		try {
			splits = BSPJobClient.readSplitFile(splitFile);
		} finally {
			splitFile.close();
		}
		// adjust number of map tasks to actual number of splits
		this.tips = new TaskInProgress[this.taskNum];
		for (int i = 0; i < this.taskNum; i++) {
			if (i < splits.length) {
				tips[i] = new TaskInProgress(getJobID(), this.jobFile.toString(), 
						this.master, this.conf, this, i, splits[i]);
			} else {
				//change in version=0.2.6 create a disable split. this only happen in Hash.
				RawSplit split = new RawSplit();
				split.setClassName("no");
				split.setDataLength(0);
				split.setBytes("no".getBytes(), 0, 2);
				split.setLocations(new String[] { "no" });
				//this task will not load data from DFS
				tips[i] = new TaskInProgress(getJobID(), this.jobFile.toString(), 
						this.master, this.conf, this, i, split);
			}
		}

		this.status.setRunState(JobStatus.PREP);
		tasksInited = true;
	}

	public void completedJob() {
		this.finishTime = System.currentTimeMillis();
		this.status.setProgress(new float[] {1.0f, 1.0f}, new int[]{-1, -1});
		this.status.setSuperStepCounter(curIteNum);
		this.status.setRunState(JobStatus.SUCCEEDED);
		this.status.setFinishTime(this.finishTime);
		garbageCollect();
		MyLOG.info("Job successfully done.");
		MyLOG.close();
	}

	public synchronized void failedJob() {
		this.finishTime = System.currentTimeMillis();
		this.status.setProgress(new float[] {1.0f, 1.0f}, new int[]{-1, -1});
		this.status.setSuperStepCounter(curIteNum);
		this.status.setRunState(JobStatus.FAILED);
		this.status.setFinishTime(this.finishTime);
		garbageCollect();
		MyLOG.close();
	}

	public void killJob() {
		this.finishTime = System.currentTimeMillis();
		this.status.setProgress(new float[] {1.0f, 1.0f}, new int[]{-1, -1});
		this.status.setSuperStepCounter(curIteNum);
		this.status.setRunState(JobStatus.KILLED);
		this.status.setFinishTime(this.finishTime);
		garbageCollect();
		for (int i = 0; i < tips.length; i++) {
			tips[i].kill();
		}
		MyLOG.close();
	}

	/**
	 * The job is dead. We're now GC'ing it, getting rid of the job from all
	 * tables. Be sure to remove all of this job's tasks from the various tables.
	 */
	synchronized void garbageCollect() {
		try {
			// Definitely remove the local-disk copy of the job file
			if (localJobFile != null) {
				localFs.delete(localJobFile, true);
				localJobFile = null;
			}
			if (localJarFile != null) {
				localFs.delete(localJarFile, true);
				localJarFile = null;
			}
			// JobClient always creates a new directory with job files
			// so we remove that directory to cleanup
			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path(profile.getJobFile()).getParent(), true);

			writeJobInformation();
		} catch (IOException e) {
			LOG.error("Error cleaning up " + profile.getJobID(), e);
		}
	}

	public void updateJobStatus() {
		float minPro = 1.0f, maxPro = 0.0f;
		int minTid = -1, maxTid = -1;
		if (Progress.size() != 0) {
			for (Entry<TaskAttemptID, Float> e: Progress.entrySet()) {
				float pro = e.getValue();
				if (minPro > pro) {
					minPro = pro;
					minTid = e.getKey().getIntegerId();
				}
				if (maxPro < pro) {
					maxPro = pro;
					maxTid = e.getKey().getIntegerId();
				}
			}
		} else {
			minPro = 0.0f;
			minTid = -1;
			maxTid = -1;
		}

		this.status.setProgress(new float[] {minPro, maxPro}, 
				new int[] {minTid, maxTid});
		this.status.setCurrentTime(System.currentTimeMillis());
	}

	public void updateTaskStatus(TaskAttemptID taskId, TaskStatus ts) {
		Progress.put(taskId, ts.getProgress());
		this.status.updateTaskStatus(taskId, ts);
		if (ts.getRunState() == State.FAILED && 
				this.status.getRunState() == JobStatus.RUNNING) {
			this.status.setRunState(JobStatus.FAILED);
		}
	}
	
	/** Just synchronize, do nothing. */
	public void sync(int parId) {
		//LOG.info("[sync] taskId=" + parId);
		int finished = this.reportCounter.incrementAndGet();
		if (finished == this.taskNum) {
			this.reportCounter.set(0);

			for (CommunicationServerProtocol comm : this.comms.values()) {
				try {
					comm.quitSync();
				} catch (Exception e) {
					LOG.error("[sync:quitSync]", e);
				}
			}
		}
	}
	
	/** Build route-table by loading the first record of each task */
	public void buildRouteTable(TaskInformation tif) {
		/*LOG.info("[ROUTETABLE] tid=" + s.getTaskId() + "\t verMinId=" + s.getVerMinId());*/
		this.byteOfOneMessage = tif.getByteOfOneMessage();
		this.isAccumulated = tif.isAccumulated();
		this.jobInfo.buildInfo(tif.getTaskId(), tif);
		InetSocketAddress address = new InetSocketAddress(tif.getHostName(), tif.getPort());
		try {
			CommunicationServerProtocol comm = 
				(CommunicationServerProtocol) RPC.waitForProxy(
						CommunicationServerProtocol.class,
							CommunicationServerProtocol.versionID, address, conf);
			comms.put(tif.getTaskId(), comm);
		} catch (Exception e) {
			LOG.error("[buildRouteTable:save comm]", e);
		}
		
		int finished = this.reportCounter.incrementAndGet();
		if (finished == taskNum) {
			this.reportCounter.set(0);
			this.jobInfo.initAftBuildingInfo(this.job.getNumTotalVertices());
			for (CommunicationServerProtocol comm : comms.values()) {
				try {
					comm.buildRouteTable(jobInfo);
				} catch (Exception e) {
					LOG.error("[buildRouteTable:buildRouteTable]", e);
				}
			}
		}
	}
	
	/** Register after loading graph data and building VE-Block */
	public void registerTask(TaskInformation tif) {
		//LOG.info("[REGISTER] tid=" + statis.getTaskId());
		this.jobInfo.registerInfo(tif.getTaskId(), tif);
		this.jobMonitor.addLoadByte(tif.getLoadByte());
		this.jobMonitor.setVerNumOfTasks(tif.getTaskId(), tif.getVerNum());
		this.jobMonitor.setEdgeNumOfTasks(tif.getTaskId(), tif.getEdgeNum());
		int finished = this.reportCounter.incrementAndGet();
		if (finished == this.taskNum) {
			this.reportCounter.set(0);

			for (CommunicationServerProtocol comm : this.comms.values()) {
				try {
					comm.setPreparation(this.jobInfo);
				} catch (Exception e) {
					LOG.error("[registerTask:setPreparation]", e);
				}
			}
			
			this.loadDataTime = 
				System.currentTimeMillis() - this.startTime;
			this.status.setRunState(JobStatus.RUNNING);
			LOG.info(jobId.toString() + " completes loading, begins to compute, "
							+ "please wait...");
		}
	}

	/** Prepare over before running an iteration */
	public void beginSuperStep(int partitionId) {
		int finished = this.reportCounter.incrementAndGet();
		if (finished == 1 && curIteNum == 0) {
			this.startTimeIte = System.currentTimeMillis();
		}
		//LOG.info("[PREPROCESS] tid=" + partitionId + "\tOVER!");
		if (finished == this.taskNum) {
			this.reportCounter.set(0);
			for (CommunicationServerProtocol comm : this.comms.values()) {
				try {
					comm.startNextSuperStep();
				} catch (Exception e) {
					LOG.error("[beginSuperStep:startNextSuperStep]", e);
				}
			}
			curIteNum++;
			this.status.setSuperStepCounter(curIteNum);
			Progress.clear();
			/*LOG.info("===**===Begin the SuperStep-"
					+ this.curIteNum + " ===**===");*/
		}
	}
	
	/** Clean over after one iteraiton */
	public void finishSuperStep(int parId, SuperStepReport ssr) {
		this.jobInfo.updateRespondVerNumOfBlks(parId, ssr.getActVerNumBucs());
		this.jobMonitor.updateMonitor(curIteNum, parId, ssr.getTaskAgg(), ssr.getCounters());
		//LOG.info("[SUPERSTEP] taskID=" + parId);
		int finished = this.reportCounter.incrementAndGet();
		if (finished == this.taskNum) {
			this.reportCounter.set(0);
			
			SuperStepCommand ssc = getNextSuperStepCommand();
			for (Entry<Integer, CommunicationServerProtocol> entry: this.comms.entrySet()) {
				try {
					ssc.setRealRoute(
							this.jobInfo.getActualCommunicationRouteTable(entry.getKey()));
					entry.getValue().setNextSuperStepCommand(ssc);
				} catch (Exception e) {
					LOG.error("[finishSuperStep:setNextSuperStepCommand]", e);
				}
			}
			
			double time = (System.currentTimeMillis() - this.startTimeIte) / 1000.0;
			this.jobInfo.recordIterationInformation(time, ssc.getMetricQ(), ssc.toString());
			this.startTimeIte = System.currentTimeMillis();
			if (ssc.getCommandType() == CommandType.STOP) {
				this.status.setRunState(JobStatus.SAVE);
			}
			
			//LOG.info("===**===Finish the SuperStep-" + this.curIteNum + " ===**===");
		}
	}

	private SuperStepCommand getNextSuperStepCommand() {
		if (this.curIteStyle == Constants.STYLE.Pull) {
			long diskMsgCount = 
				this.jobMonitor.getProducedMsgNum(curIteNum)-(this.taskNum*this.recMsgBuf);
			diskMsgCount = diskMsgCount<0? 0:diskMsgCount;
			this.jobMonitor.addByteOfPush(curIteNum, diskMsgCount*this.byteOfOneMessage*2);
		}
		
		SuperStepCommand ssc = new SuperStepCommand();
		ssc.setJobAgg(this.jobMonitor.getAgg(curIteNum));
		//LOG.info("debug. curIteNum=" + this.curIteNum);
		double Q = 0.0;
		if (this.curIteNum > 2) {
			
			long diskMsgNum = 
				this.jobMonitor.getProducedMsgNum(curIteNum)-(this.taskNum*this.recMsgBuf);
			diskMsgNum = diskMsgNum<0? 0:diskMsgNum;
			double diskMsgWriteCost = (diskMsgNum*this.byteOfOneMessage) / this.randWriteSpeed;
			
			long seqDiskByteDiff = this.jobMonitor.getByteOfPush(curIteNum) 
				- diskMsgNum*this.byteOfOneMessage 
				- (this.jobMonitor.getByteOfPull(curIteNum) 
						- this.jobMonitor.getByteOfVertInPull(curIteNum));
			double diskReadCostDiff = seqDiskByteDiff/this.seqReadSpeed
				- this.jobMonitor.getByteOfVertInPull(curIteNum)/this.randReadSpeed;
			
			double reducedNetMsgNum = (double)this.jobMonitor.getReducedNetMsgNum(curIteNum);
			if (this.curIteStyle == Constants.STYLE.Push) {
				reducedNetMsgNum = this.jobMonitor.getProducedMsgNum(curIteNum) * this.lastCombineRatio;
			} else {
				this.lastCombineRatio = reducedNetMsgNum / this.jobMonitor.getProducedMsgNum(curIteNum);
			}
			double reducedNetCost = 
				this.isAccumulated? (reducedNetMsgNum*this.byteOfOneMessage)/this.netSpeed 
						: (reducedNetMsgNum*4)/this.netSpeed;
			
			Q = diskMsgWriteCost + diskReadCostDiff + reducedNetCost; //push-pull
			ssc.setMetricQ(Q);
			
			/** Set the change automically
			 *  Suppose that:
			 *  1 Starting style=style.Pull.
			 *  2 When #act_vertices is increasing, style.Pull is always performed, 
			 *    because the growing speed is usually fast, 
			 *    which means frequent switching operations are not cost effective.
			 *  3 Otherwise, switch dynamically:
			 *    1) if Q >= 0
			 *          switch from push to pull.
			 *    2) else
			 *          switch from pull to push.
			 *    3) specially, the switching function is closed 
			 *       if |Q|<=2.0, this is because the switching benefit 
			 *       is so tiny (negligible) that switching is not cost effective.
			 * */
			if (this.jobMonitor.getActVerNum(curIteNum) < 
					this.jobMonitor.getActVerNum(curIteNum-1)) { //decreasing
				if (this.preIteStyle==this.curIteStyle) {
					this.preIteStyle = this.curIteStyle;
					if (this.job.getBspStyle()==Constants.STYLE.Hybrid 
							&& Math.abs(Q)>2.0) {
						if (Q >= 0.0) {
							this.curIteStyle = Constants.STYLE.Pull;
						} else {
							this.curIteStyle = Constants.STYLE.Push;
						}
					}
				} else {
					this.preIteStyle = this.curIteStyle;
				}
			} else {
				this.preIteStyle = this.curIteStyle;
			}
		} else {
			this.preIteStyle = this.curIteStyle;
		}
		
		/**
		 * About the switchCounter value:
		 * 1) switchCounter=1 (old style): prepare to switch at the next iteration, but the current iteration is old style): ;
		 * 2) switchCounter=2 (new style): switching from old to new style (collected info. may not be accurate);
		 * 3) switchCounter=3 (new style): do a complete iteration using the new style (collected info. is accurate);
		 * Thus, the switching interval w=2.
		 */
		if (this.switchCounter != 0) {
			this.switchCounter++;
			if (this.switchCounter == 3) {
				this.switchCounter = 0;
			}
		}
		if (this.preIteStyle!=this.curIteStyle) {
			if (this.switchCounter != 0) {
				this.curIteStyle = this.preIteStyle; //invalid switch
			} else {
				this.switchCounter++;
			}
		}
		
		//just for testing
		//this.curIteStyle = Constants.STYLE.Pull; //always pull in the hybrid
		
		//for the next superstep
		ssc.setIteStyle(this.curIteStyle);
		if (this.job.getBspStyle()==Constants.STYLE.Hybrid 
				&& this.curIteStyle==Constants.STYLE.Push) {
			ssc.setEstimatePullByte(true);
		} else {
			ssc.setEstimatePullByte(false);
		}
		
		if (this.jobMonitor.getActVerNum(curIteNum)==0
				|| (curIteNum==maxIteNum)) {
			ssc.setCommandType(CommandType.STOP);
		} else {
			ssc.setCommandType(CommandType.START);
		}
		
		return ssc;
	}
	
	public void saveResultOver(int parId, int saveRecordNum) {
		int finished = this.reportCounter.incrementAndGet();
		if (finished == this.taskNum) {
			this.reportCounter.set(0);
			this.saveDataTime = System.currentTimeMillis() - this.startTimeIte;
			this.completedJob();
		}
	}

	private void writeJobInformation() {
		StringBuffer sb = new StringBuffer(
				"\n=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=");
		if (this.status.getRunState() == JobStatus.SUCCEEDED) {
			sb.append("\n    Job has been completed successfully!");
		} else {
			sb.append("\n       Job has been quited abnormally!");
		}
		
		sb.append("\n=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=");
		sb.append("\n              STATISTICS DATA");
		sb.append("\nMaxIterator:  " + this.maxIteNum);
		sb.append("\nAllIterator:  " + this.curIteNum);
		sb.append("\nScheTaskTime: " + this.scheTaskTime / 1000.0 + " seconds");
		sb.append("\nJobRunTime:  " + this.status.getRunCostTime()
				+ " seconds");
		sb.append("\nLoadDataTime: " + this.loadDataTime / 1000.0 + " seconds");
		sb.append("\nIteCompuTime: " + (this.status.getRunCostTime()*1000.0f-
				this.loadDataTime-this.saveDataTime) / 1000.0 + " seconds");
		sb.append("\nSaveDataTime: " + this.saveDataTime / 1000.0 + " seconds");
		
		sb.append(this.jobInfo.toString());
		sb.append(this.jobMonitor.printJobMonitor(this.curIteNum));
		
		sb.append("\nOther Information:");
		sb.append("\n    (1)JobID: " + jobId.toString());
		sb.append("\n    (2)#total_vertices: " + this.jobInfo.getVerNum());
		sb.append("\n    (3)#total_edges: " + this.jobInfo.getEdgeNum());
		sb.append("\n    (4)TaskToWorkerName:");
		for (int index = 0; index < taskToWorkerName.length; index++) {
			sb.append("\n              " + taskToWorkerName[index]);
		}
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sb.append("\n=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=");
		sb.append("\nlog time: " + sdf.format(new Date()));
		sb.append("\nauthor: HybridGraph");

		MyLOG.info(sb.toString());
		this.jobInfo = null;
		this.jobMonitor = null;
	}
}
