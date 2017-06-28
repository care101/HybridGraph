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
import java.util.HashSet;
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
import org.apache.hama.myhama.comm.MiniSuperStepCommand;
import org.apache.hama.myhama.comm.SuperStepCommand;
import org.apache.hama.myhama.comm.SuperStepReport;
import org.apache.hama.myhama.util.JobLog;
import org.apache.hama.myhama.util.MiniCounters;
import org.apache.hama.myhama.util.MiniCounters.MINICOUNTER;

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

	/** how long of scheduling, loading graph, saving result */
	private double scheTaskTime, loadDataTime, saveDataTime;
	/** the time of submitting, scheduling and finishing time */
	private long submitTime, startTime, finishTime;
	private long startTimeIte = 0;

	private ConcurrentHashMap<TaskAttemptID, Float> Progress = 
		new ConcurrentHashMap<TaskAttemptID, Float>();
	
	private Constants.STYLE preIteStyle, curIteStyle;
	private int switchCounter = 0;
	private int byteOfOneMessage = 0;
	private boolean isAccumulated = false;
	private int recMsgBuf = 0;
	/**
	 * Local cluster with HDD:
	 *   1) randRead = 1205KB/s  (tested by fio)
	 *   2) randWrite = 1210KB/s (tested by fio)
	 *   3) seqRead = 2415KB/s   (tested by fio)
	 *   4) seqWrite = 2414KB/s  (tested by fio)
	 *   5) network = 112MB/s    (tested by iperf)
	 * Amazon cluster with SSD (c3.xlarge, 30GB SSD):
	 * 	 1) randRead = 18613KB/s  (tested by fio)
	 *   2) randWrite = 18631KB/s (tested by fio)
	 *   3) seqRead = 18708KB/s   (tested by fio)
	 *   4) seqWrite = 18730KB/s  (tested by fio)
	 *   5) network = 116MB/s     (tested by iperf)
	 */
	private float randWriteSpeed = 1210*ONE_KB;
	private float randReadSpeed = 1205*ONE_KB;
	//private float seqWriteSpeed = 2414*ONE_KB;
	private float seqReadSpeed = 2415*ONE_KB;
	private float netSpeed = 112*ONE_KB*ONE_KB;    
	private double lastCombineRatio = 0.0;
	
	/**
	 * Record failed tasks. Now only failures between beginSuperStep() 
	 * and finishSuperStep() (excluding barrier points) can be recovered.
	 */
	private HashSet<Integer> failedTaskIds;
	private int recoveryIteNum = 0;

	public JobInProgress(BSPJobID _jobId, Path _jobFile, BSPMaster _master,
			Configuration _conf) throws IOException {
		this.randReadSpeed = 
			_conf.getFloat(Constants.HardwareInfo.RD_Read_ThroughPut, 
				Constants.HardwareInfo.Def_RD_Read_ThroughPut)*ONE_KB;
		this.randWriteSpeed = 
			_conf.getFloat(Constants.HardwareInfo.RD_Write_ThroughPut, 
				Constants.HardwareInfo.Def_RD_Write_ThroughPut)*ONE_KB;
		this.seqReadSpeed = 
			_conf.getFloat(Constants.HardwareInfo.Seq_Read_ThroughPut, 
				Constants.HardwareInfo.Def_Seq_Read_ThroughPut)*ONE_KB;
		this.netSpeed = 
			_conf.getFloat(Constants.HardwareInfo.Network_ThroughPut, 
				Constants.HardwareInfo.Def_Network_ThroughPut)*ONE_KB*ONE_KB;
		LOG.info(print("=*", 38) + "=");
/*		LOG.info("hardware info " 
				+ "\nrandom.read.throughput = " + randReadSpeed/ONE_KB + " KB/s" 
				+ "\nrandom.write.throughput = " + randWriteSpeed/ONE_KB + " KB/s" 
				+ "\nsequential.read.throughput = " + seqReadSpeed/ONE_KB + " KB/s"
				+ "\nsequential.write.throughput = " + seqWriteSpeed/ONE_KB + " KB/s"
				+ "\nnetwork.throughput = " + netSpeed/(ONE_KB*ONE_KB) + " MB/s");*/
		
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
		
		preIteStyle = job.getStartIteStyle();
		curIteStyle = this.preIteStyle;
		reportCounter = new AtomicInteger(0);
		
		failedTaskIds = new HashSet<Integer>();
	}

	private void initialize() {
		jobInfo = new JobInformation(this.job, this.taskNum);
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
	
	/**
	 * For the first scheduling operation, return #tasks. 
	 * For a recovery scheduling operation, return #failedTasks.
	 * @return
	 */
	public int getNumBspTask() {
		if (this.status.getRunState() == JobStatus.RESTART) {
			return this.failedTaskIds.size();
		} else {
			return this.taskNum;
		}
	}
	
	/**
	 * Return all {@link TaskInProgress}s for the first scheduling. 
	 * Return failed {@link TaskInProgress}s for the recovery scheduling.
	 * @return
	 */
	public TaskInProgress[] getTaskInProgress() {
		if (this.status.getRunState() == JobStatus.RESTART) {
			TaskInProgress[] result = 
				new TaskInProgress[failedTaskIds.size()];
			int idx = 0; 
			for (Integer id: failedTaskIds) {
				result[idx] = tips[id];
				idx++;
			}
			return result;
		} else {
			return tips;
		}
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

	public synchronized void initTasks() throws IOException {
		if (tasksInited) {
			return;
		}
		//change in version=0.2.3 read the input split info from HDFS
		Path sysDir = new Path(this.master.getSystemDir());
		FileSystem fs = sysDir.getFileSystem(conf);
		DataInputStream splitFile = 
			fs.open(new Path(conf.get("bsp.job.split.file")));
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
				tips[i] = new TaskInProgress(getJobID(), 
						this.jobFile.toString(), 
						this.master, this.conf, this, i, splits[i]);
			} else {
				//change in version=0.2.6 create a disable split. 
				//this only happen in Hash.
				RawSplit split = new RawSplit();
				split.setClassName("no");
				split.setDataLength(0);
				split.setBytes("no".getBytes(), 0, 2);
				split.setLocations(new String[] { "no" });
				//this task will not load data from DFS
				tips[i] = new TaskInProgress(getJobID(), 
						this.jobFile.toString(), 
						this.master, this.conf, this, i, split);
			}
		}

		this.status.setRunState(JobStatus.PREP);
		tasksInited = true;
	}

	private void completedJob() {
		this.finishTime = System.currentTimeMillis();
		this.status.setProgress(new float[] {1.0f, 1.0f}, new int[]{-1, -1});
		this.status.setSuperStepCounter(curIteNum);
		this.status.setRunState(JobStatus.SUCCEEDED);
		this.status.setFinishTime(this.finishTime);
		garbageCollect();
		MyLOG.close();
		LOG.info(jobId.toString() + " is done.");
		LOG.info(print("=*", 38) + "=");
	}

	public void failedJob() {
		this.finishTime = System.currentTimeMillis();
		this.status.setProgress(new float[] {1.0f, 1.0f}, new int[]{-1, -1});
		this.status.setSuperStepCounter(curIteNum);
		this.status.setRunState(JobStatus.FAILED);
		this.status.setFinishTime(this.finishTime);
		garbageCollect();
		MyLOG.close();
		LOG.warn(jobId.toString() + " finally fails.");
		LOG.info(print("=*", 38) + "=");
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
		LOG.warn(jobId.toString() + " is killed.");
		LOG.info(print("=*", 38) + "=");
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
			
			Path ckpJobDir = master.getCheckPointDirectoryForJob(jobId);
			if (fs.delete(ckpJobDir, true)) {
				/*LOG.info(jobId.toString() + " deletes checkpoint dir:\n" 
						+ ckpJobDir.toString());*/
			} else {
				LOG.error("fail to delete checkpoint dir:" 
						+ ckpJobDir.toString());
			}
			master.clearJob(jobId, this);
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

	synchronized public void updateTaskStatus(TaskAttemptID taskId, 
			TaskStatus ts) {
		Progress.put(taskId, ts.getProgress());
		this.status.updateTaskStatus(taskId, ts);
		/**
		 * Status of failed/killed/succeeded tasks will be updated only once.
		 * Killed/succeeded tasks can be ignored. While, failed tasks trigger 
		 * failure recovery operations.
		 */
		if (ts.getRunState() == State.FAILED) {
			LOG.warn(ts.getTaskId() + " fails");
			if (failedTaskIds.isEmpty()) {
				this.status.setRunState(JobStatus.RESTART);
			}
			
			/** 
			 * {@link CheckTimeOut} and {@link BSPMaster}.report may report 
			 * a failed task twice. Only the first report is handled.
			 * */
			Integer id = taskId.getIntegerId();
			if (!failedTaskIds.contains(id)) {
				failedTaskIds.add(id);
				this.finishSuperStep(id, null);
			}
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
					comm.quit();
				} catch (Exception e) {
					LOG.error("[sync:quitSync]", e);
				}
			}
		}
	}
	
	public void miniSync(int tid, MiniCounters minicounters) {
		int finished = this.reportCounter.incrementAndGet();
		this.jobMonitor.updateMiniMonitor(curIteNum, tid, minicounters);
		if (finished == this.taskNum) {
			this.reportCounter.set(0);
			
			MiniSuperStepCommand mssc = getNextMiniSuperStepCommand();
			this.jobInfo.archiveMiniCommand(mssc);
			for (CommunicationServerProtocol comm : this.comms.values()) {
				try {
					comm.setNextMiniSuperStepCommand(mssc);
				} catch (Exception e) {
					LOG.error("[miniSync:quitSync]", e);
				}
			}
		}
	}
	
	private MiniSuperStepCommand getNextMiniSuperStepCommand() {
		MiniSuperStepCommand mssc = new MiniSuperStepCommand();
		
		//runtime statistics, dynamically collected
		MiniCounters minicounters = this.jobMonitor.getMiniCounters(curIteNum);
		
		long msgCount = minicounters.getCounter(MINICOUNTER.Msg_Estimate);
		long diskMsgCount = msgCount - (this.taskNum*this.recMsgBuf);
		diskMsgCount = diskMsgCount<0? 0:diskMsgCount;
		
		//random writes
		double msgRandWritePushCost = 
			(diskMsgCount*byteOfOneMessage) / randWriteSpeed;
		
		//random reads
		double vertRandReadPullCost = 
			minicounters.getCounter(MINICOUNTER.Byte_RandReadVert) / randReadSpeed;
		
		//sequential reads
		double msgSeqReadPushCost = 
			(diskMsgCount*byteOfOneMessage) / seqReadSpeed;
		double vertSeqReadPushCost = 
			minicounters.getCounter(MINICOUNTER.Byte_SeqReadVert) / seqReadSpeed;
		double edgeSeqReadPushCost = 
			minicounters.getCounter(MINICOUNTER.Byte_PushEdge) / seqReadSpeed;
		double edgeSeqReadPullCost = 
			minicounters.getCounter(MINICOUNTER.Byte_PullSeqRead) / seqReadSpeed;
		
		//network
		long netMsgCount = (long)(msgCount * (1.0*(taskNum-1)/taskNum));
		double combinedRatio = 
			(jobInfo.getEdgeNum()*1.0-jobInfo.getVerNum()) / jobInfo.getEdgeNum();
		long reducedNetMsgCount = (long) (netMsgCount * combinedRatio);
		double reducedNetCost = 
			isAccumulated? (reducedNetMsgCount*byteOfOneMessage)/netSpeed 
					: (reducedNetMsgCount*4)/netSpeed;
		
		//runtime difference: push - pull
		double miniQ = -(vertRandReadPullCost+edgeSeqReadPullCost) 
		+(msgRandWritePushCost+msgSeqReadPushCost+vertSeqReadPushCost+edgeSeqReadPushCost+reducedNetCost);
		
		mssc.setMiniQ(miniQ);
		mssc.setIteNum(curIteNum);
		
		if (this.job.isSimulatePUSH()) {
			mssc.setStyle(Constants.STYLE.PUSH);
		} else if (this.job.getBspStyle() == Constants.STYLE.PULL) {
			mssc.setStyle(Constants.STYLE.PULL);
		} else {
			/**
			//manually switch between PULL and PUSH for MIS
			switch((curIteNum)%2) {
			case 0: mssc.setStyle(Constants.STYLE.PUSH); break;
			case 1: mssc.setStyle(Constants.STYLE.PULL); break;
			} 
			
			//manually switch between PULL and PUSH for MM
			switch((curIteNum-1)%4) {
			case 0: mssc.setStyle(Constants.STYLE.PULL); break;
			case 1: 
			case 2: 
			case 3: mssc.setStyle(Constants.STYLE.PUSH); break;
			} */
			
			//-230 for neu, -25 for amazon
			if ((miniQ<-230.0) || (miniQ==0.0)) {
				mssc.setStyle(Constants.STYLE.PUSH);
			} else {
				mssc.setStyle(Constants.STYLE.PULL);
			}
		}
		
		return mssc;
	}
	
	/** Build route-table by loading the first record of each task */
	public void buildRouteTable(TaskInformation tif) {
		/*LOG.info("[ROUTETABLE] tid=" + tif.getTaskId() 
				+ " minId=" + tif.getVerMinId() 
				+ " host=" + tif.getHostName()
				+ " port=" + tif.getPort());*/
		this.byteOfOneMessage = tif.getByteOfOneMessage();
		this.isAccumulated = tif.isAccumulated();
		this.jobInfo.buildInfo(tif.getTaskId(), tif);
		InetSocketAddress address = 
			new InetSocketAddress(tif.getHostName(), tif.getPort());
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
		if (finished==taskNum 
				|| (finished==failedTaskIds.size() 
						&& this.status.getRunState()==JobStatus.RESTART)) {
			this.reportCounter.set(0);
			
			//initialize variables related to checkpoint
			if (this.status.getRunState() != JobStatus.RESTART) {
				Path ckpJobDir = master.getCheckPointDirectoryForJob(jobId);
				try {
					FileSystem fs = ckpJobDir.getFileSystem(this.conf);
					if (fs.mkdirs(ckpJobDir)) {
						/*LOG.info(jobId.toString() + " creates checkpoint dir:\n" 
								+ ckpJobDir.toString() 
								+ ", interval=" + job.getCheckPointInterval());*/
					} else {
						LOG.error("fail to create checkpoint dir:" 
								+ ckpJobDir.toString());
					}
				} catch (Exception e) {
					LOG.error("fail to create checkpoint dir", e);
				}
				
				this.jobInfo.initAftBuildingInfo(this.job.getNumTotalVertices(), 
						ckpJobDir.toString());
			}
			
			for (Entry<Integer, CommunicationServerProtocol> entry: 
					comms.entrySet()) {
				try {
					if (this.status.getRunState()==JobStatus.RESTART 
							&& !failedTaskIds.contains(entry.getKey())) {
						//only update info kept on surviving tasks
						entry.getValue().updateRouteTable(jobInfo);
					} else {
						//set info and quit barrier
						entry.getValue().setRouteTable(jobInfo);
					}
				} catch (Exception e) {
					LOG.error("[buildRouteTable:setRouteTable] taskId=" 
							+ entry.getKey(), e);
				}
			}
		}
	}
	
	/** Register after loading graph data and building VE-Block */
	public void registerTask(TaskInformation tif) {
		//LOG.info("[REGISTER] tid=" + statis.getTaskId());
		if (this.status.getRunState() == JobStatus.RESTART) {
			recoveryRegisterTask(tif);
			return; //recovery register
		}
		
		this.jobInfo.registerInfo(tif.getTaskId(), tif);
		this.jobMonitor.registerInfo(tif.getTaskId(), tif);
		int finished = this.reportCounter.incrementAndGet();
		if (finished == this.taskNum) {
			this.reportCounter.set(0);

			for (CommunicationServerProtocol comm : comms.values()) {
				try {
					comm.setRegisterInfo(jobInfo);
				} catch (Exception e) {
					LOG.error("[registerTask:setRegistInfo]", e);
				}
			}
			
			this.loadDataTime = 
				System.currentTimeMillis() - this.startTime;
			this.status.setRunState(JobStatus.RUNNING);
			LOG.info(jobId.toString() + " starts with BspStyle=" 
					+ job.getBspStyle());
			LOG.info(jobId.toString() + " starts with CheckPoint.Policy=" 
					+ job.getCheckPointPolicy());
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
			/** archive a checkpoint successfully */
			if (curIteNum>=1 && 
					this.status.getRunState()==JobStatus.RUNNING) { 
				CommandType type = 
					this.jobInfo.getCommand(curIteNum).getCommandType();
				if (type == CommandType.ARCHIVE) {
					this.jobInfo.setAvailableCheckPoint(curIteNum);
					double time = (System.currentTimeMillis()-this.startTimeIte) / 1000.0;
					this.jobMonitor.accumulateCheckPointTime(time);
					this.startTimeIte = System.currentTimeMillis();
				}
			}
			/**
			 * Complete the failure recovery preparing work: 
			 * reloading input graph, 
			 * rebuilding VEBlocks, 
			 * and loading required checkpoint files.
			 */
			if (recoveryIteNum!=0 && 
					this.status.getRunState()==JobStatus.RECOVERY 
					&& recoveryIteNum==this.jobInfo.getAvailableCheckPoint()) {
				double time = (System.currentTimeMillis()-startTimeIte) / 1000.0;
				jobMonitor.setTimeOfReloadData(time);
				this.startTimeIte = System.currentTimeMillis();
			}
			
			for (CommunicationServerProtocol comm : this.comms.values()) {
				try {
					comm.quit();
				} catch (Exception e) {
					LOG.error("[beginSuperStep] startNextSuperStep-" 
							+ (curIteNum+1), e);
				}
			}
			if (this.status.getRunState() == JobStatus.RECOVERY) {
				recoveryIteNum++;
				this.status.setSuperStepCounter(recoveryIteNum);
				if (recoveryIteNum == curIteNum) {
					this.status.setRunState(JobStatus.RUNNING); //recovery is done
					this.failedTaskIds.clear();
					recoveryIteNum = 0;
					LOG.info(jobId.toString() + " continues computations");
				}
			} else {
				curIteNum++;
				this.status.setSuperStepCounter(curIteNum);
			}
			
			Progress.clear();
			/*LOG.info("===**===Begin the SuperStep-"
					+ this.curIteNum + " ===**===");*/
		}
	}
	
	/** Clean over after one iteraiton */
	public void finishSuperStep(int parId, SuperStepReport ssr) {
		if (this.status.getRunState() == JobStatus.RESTART) {
			this.jobMonitor.rollbackMonitor(curIteNum);
		} else if (this.status.getRunState() == JobStatus.RECOVERY) {
			int size = recoveryIteNum - this.jobInfo.getAvailableCheckPoint();
			this.jobMonitor.updateMonitorRecovery(size, parId, ssr.getTaskAgg(), 
					ssr.getCounters());
		} else {
			this.jobInfo.updateRespondVerNumOfBlks(parId, ssr.getActVerNumBucs());
			this.jobMonitor.updateMonitor(curIteNum, parId, ssr.getTaskAgg(), 
					ssr.getCounters());
		}
		
		//LOG.info("[SUPERSTEP] taskID=" + parId);
		int finished = this.reportCounter.incrementAndGet();
		if (finished==this.taskNum) {
			this.reportCounter.set(0);
			
			/** failures happen */
			if (this.status.getRunState() == JobStatus.RESTART) {
				LOG.warn(this.jobId + " fails");
				if (jobInfo.getAvailableCheckPoint() == 0) {
					LOG.warn(jobId + " cannot find any available checkpoint");
					failedJob(); //failure cannot be recovered
				} else {
					master.resubmitJob(jobId, this); //re-schedule failed tasks
				}
				//suspend surviving tasks until restart tasks enter 
				//recoveryRegisterTask()
				double time = (System.currentTimeMillis()-startTimeIte) / 1000.0;
				jobMonitor.setTimeOfFindFailure(time);
				this.startTimeIte = System.currentTimeMillis();
				return; 
			}
			
			SuperStepCommand ssc = getNextSuperStepCommand();
			if (ssc.getCommandType() == CommandType.STOP) {
				this.status.setRunState(JobStatus.SAVE);
			}
			
			double time = (System.currentTimeMillis()-startTimeIte) / 1000.0;
			ssc.setIterationTime(time);
			if (this.status.getRunState() == JobStatus.RECOVERY) {
				this.jobInfo.archiveRecoveryCommand(ssc);
			} else {
				this.jobInfo.archiveCommand(ssc);
				this.jobMonitor.accumulateRuntime(ssc.getIterationTime());
			}
			this.startTimeIte = System.currentTimeMillis();
			
			for (Entry<Integer, CommunicationServerProtocol> entry: 
				this.comms.entrySet()) {
				try {
					ssc.setRealRoute(
							this.jobInfo.getActualRouteTable(entry.getKey(), 
							ssc.getCommandType()));
					entry.getValue().setNextSuperStepCommand(ssc);
				} catch (Exception e) {
					LOG.error("[finishSuperStep:setNextSuperStepCommand]", e);
				}
			}
			
			/*LOG.info("===**===Finish the SuperStep-" 
			*+ this.curIteNum + " ===**===");*/
		}
	}
	
	/**
	 * Register a new task with respect to the failed one. Once new tasks 
	 * corresponding to all failed tasks kept in failedTaskIds have been 
	 * registered, finishSuperStep will continue to broadcast a new command 
	 * {@link SuperStepCommand} to recover failures. 
	 */
	private void recoveryRegisterTask(TaskInformation tif) {
		int recoveryTasks = this.reportCounter.incrementAndGet();
		/*LOG.info("[recoveryRegister] tid=" + tif.getTaskId() 
				+ ", " + recoveryTasks + " of " + failedTaskIds.size());*/
		if (recoveryTasks == failedTaskIds.size()) {
			this.reportCounter.set(0);
			this.status.setRunState(JobStatus.RECOVERY);
			this.recoveryIteNum = this.jobInfo.getAvailableCheckPoint();
			LOG.info(jobId.toString() + " restarts from superstep-" 
					+ (this.recoveryIteNum+1));
			
			SuperStepCommand ssc = getNextSuperStepCommand();
			ssc.adjustIteStyle(this.job.getCheckPointPolicy());
			if (ssc.getCommandType() == CommandType.STOP) {
				this.status.setRunState(JobStatus.SAVE);
			}
			
			ssc.setIterationTime(0.0);
			ssc.setFindError(true);
			this.jobInfo.archiveRecoveryCommand(ssc);
			
			/**
			 * Surviving tasks quit the finishSuperStep() barrier, 
			 * while restart tasks quit the registerTask() barrier. 
			 * Afterwards, all tasks enter the beginSuperStep() barrier.
			 */
			for (Entry<Integer, CommunicationServerProtocol> entry: 
				this.comms.entrySet()) {
				try {
					ssc.setRealRoute(
							this.jobInfo.getActualRouteTable(entry.getKey(), 
							ssc.getCommandType()));
					entry.getValue().setNextSuperStepCommand(ssc);
				} catch (Exception e) {
					LOG.error("[recoveryRegisterTask]", e);
				}
			}
		}
	}

	private SuperStepCommand getNextSuperStepCommand() {
		SuperStepCommand ssc = new SuperStepCommand();
		if (this.status.getRunState() == JobStatus.RECOVERY) {
			ssc.setAvailableCheckPointVersion(
					jobInfo.getAvailableCheckPoint()+1);
			if ((recoveryIteNum+1) == curIteNum) {
				//all tasks re-execute the failed iteration
				ssc.setCommandType(CommandType.REDO);
			} else {
				//surviving tasks and new tasks perform different operations
				ssc.setCommandType(CommandType.RECOVER);
				ssc.setFailedTaskIds(failedTaskIds);
			}
			
			ssc.setIteNum(recoveryIteNum);
			//anyway, other parameters have been available, just copy them
			SuperStepCommand archivedCommand = 
				this.jobInfo.getCommand(recoveryIteNum);
			ssc.copy(archivedCommand);
			
			return ssc;
		}
		
		if (this.curIteStyle == Constants.STYLE.PULL) {
			long diskMsgCount = 
				this.jobMonitor.getProducedMsgNum(curIteNum)
				-(this.taskNum*this.recMsgBuf);
			diskMsgCount = diskMsgCount<0? 0:diskMsgCount;
			this.jobMonitor.addByteOfPush(curIteNum, 
					diskMsgCount*this.byteOfOneMessage*2);
		}
		
		ssc.setIteNum(curIteNum);
		ssc.setJobAgg(this.jobMonitor.getAgg(curIteNum));
		//LOG.info("debug. curIteNum=" + this.curIteNum);
		double Q = 0.0;
		if (this.job.isMiniSuperStep()) {
			this.curIteStyle = this.jobInfo.getMiniCommand(curIteNum).getStyle(); 
			this.preIteStyle = this.curIteStyle;
			//this.curIteStyle = this.preIteStyle; //simulate original PUSH without mini-barriers
		} else if (this.curIteNum > 2) {
			long diskMsgNum = 
				this.jobMonitor.getProducedMsgNum(curIteNum)
				-(this.taskNum*this.recMsgBuf);
			diskMsgNum = diskMsgNum<0? 0:diskMsgNum;
			double diskMsgWriteCost = 
				(diskMsgNum*this.byteOfOneMessage) / this.randWriteSpeed;
			
			long seqDiskByteDiff = this.jobMonitor.getByteOfPush(curIteNum) 
				- diskMsgNum*this.byteOfOneMessage 
				- (this.jobMonitor.getByteOfPull(curIteNum) 
						- this.jobMonitor.getByteOfVertInPull(curIteNum));
			double diskReadCostDiff = seqDiskByteDiff/seqReadSpeed
				- jobMonitor.getByteOfVertInPull(curIteNum)/randReadSpeed;
			
			double reducedNetMsgNum = 
				(double)jobMonitor.getReducedNetMsgNum(curIteNum);
			if (curIteStyle == Constants.STYLE.PUSH) {
				reducedNetMsgNum = 
					jobMonitor.getProducedMsgNum(curIteNum) * lastCombineRatio;
			} else {
				lastCombineRatio = 
					reducedNetMsgNum / jobMonitor.getProducedMsgNum(curIteNum);
			}
			double reducedNetCost = 
				isAccumulated? (reducedNetMsgNum*byteOfOneMessage)/netSpeed 
						: (reducedNetMsgNum*4)/netSpeed;
			
			Q = diskMsgWriteCost + diskReadCostDiff + reducedNetCost; //push-pull

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
							this.curIteStyle = Constants.STYLE.PULL;
						} else {
							this.curIteStyle = Constants.STYLE.PUSH;
						}
					}
				} else {
					Q = this.jobInfo.getQ(curIteNum-1);
					this.preIteStyle = this.curIteStyle;
				}
			} else {
				Q = 0.0;
				this.preIteStyle = this.curIteStyle;
			}
			
			/*this.preIteStyle = this.curIteStyle;
			if (curIteNum >= 5) {
				this.curIteStyle = Constants.STYLE.PUSH;
			}*/
		} else {
			this.preIteStyle = this.curIteStyle;
		}
		
		/**
		 * About the switchCounter value:
		 * 1) switchCounter=1 (old style): prepare to switch at the next iteration, 
		 *    but the current iteration is old style);
		 * 2) switchCounter=2 (new style): switching from old to new style (collected 
		 *    info. may not be accurate);
		 * 3) switchCounter=3 (new style): do a complete iteration using the new 
		 *    style (collected info. is accurate);
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
		
		//for the next superstep
		ssc.setIteStyle(this.preIteStyle, this.curIteStyle);
		ssc.setMetricQ(Q);
		if (this.job.getBspStyle()==Constants.STYLE.Hybrid 
				&& this.curIteStyle==Constants.STYLE.PUSH) {
			ssc.setEstimatePullByte(true);
		} else {
			ssc.setEstimatePullByte(false);
		}
		
		if (this.jobMonitor.getActVerNum(curIteNum)==0
				|| (curIteNum==maxIteNum)) {
			ssc.setCommandType(CommandType.STOP);
		} else if (isCheckPoint()) {
			ssc.setCommandType(CommandType.ARCHIVE);
		} else {
			ssc.setCommandType(CommandType.START);
		}
		
		return ssc;
	}
	
	private boolean isCheckPoint() {
		int interval = job.getCheckPointInterval();
		if (job.getCheckPointPolicy()==Constants.CheckPoint.Policy.None 
				|| !job.isGraphDataOnDisk() || interval<=0) {
			return false;
		} else {
			//dynamically checkpoint
			boolean periodJudge = 
				(curIteNum-this.jobInfo.getAvailableCheckPoint())>=interval;
			return (periodJudge&&this.jobMonitor.isDynCheckPointRequired()); 
			//return periodJudge;
		}
	}
	
	public void dumpResult(int parId, int dumpNum) {
		int finished = this.reportCounter.incrementAndGet();
		if (finished == this.taskNum) {
			this.reportCounter.set(0);
			this.saveDataTime = System.currentTimeMillis() - this.startTimeIte;
			this.completedJob();
		}
	}

	private void writeJobInformation() {
		StringBuffer sb = new StringBuffer("\n" + print("=*", 24) + "=");
		if (this.status.getRunState() == JobStatus.SUCCEEDED) {
			sb.append("\n    Job has been completed successfully!");
		} else {
			sb.append("\n       Job has been quited abnormally!");
		}
		
		sb.append("\n" + print("=*", 24) + "=");
		sb.append("\n              STATISTICS DATA");
		sb.append("\nGiven Number of Iterations:  " + this.maxIteNum);
		sb.append("\nActual Number of Iterations: " + this.curIteNum);
		sb.append("\nRuntime of Scheduling Tasks: " + this.scheTaskTime / 1000.0 + " seconds");
		sb.append("\nRuntime of Processing Graph: " + this.status.getRunCostTime()
				+ " seconds");
		sb.append("\n               [load graph]: " + this.loadDataTime / 1000.0 + " seconds");
		sb.append("\n               [super-step]: " + (this.status.getRunCostTime()*1000.0f-
				this.loadDataTime-this.saveDataTime) / 1000.0 + " seconds");
		sb.append("\n               [save graph]: " + this.saveDataTime / 1000.0 + " seconds");
		
		sb.append(this.jobInfo.toString());
		sb.append(this.jobMonitor.printJobMonitor(this.curIteNum));
		
		sb.append("\nOther Information:");
		sb.append("\n    (1)JobID: " + jobId.toString());
		sb.append("\n    (2)#total_vertices: " + this.jobInfo.getVerNum());
		sb.append("\n    (3)#total_edges: " + this.jobInfo.getEdgeNum());
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sb.append("\n" + print("=*", 24) + "=");
		sb.append("\nHybridGraph, " + sdf.format(new Date()));

		MyLOG.info(sb.toString());
		this.jobInfo = null;
		this.jobMonitor = null;
	}
	
	/**
	 * Print the given flag x times.
	 * @param flag
	 * @param x
	 * @return
	 */
	private String print(String flag, int x) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < x; i++) {
			sb.append(flag);
		}
		return sb.toString();
	}
}
