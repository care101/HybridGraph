/**
 * copyright 2012-2010
 */
package org.apache.hama.bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hama.Constants;
import org.apache.hama.Constants.VBlockUpdateRule;
import org.apache.hama.ipc.MasterProtocol;
import org.apache.hama.monitor.TaskInformation;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.comm.CommunicationServer;
import org.apache.hama.myhama.comm.SuperStepCommand;
import org.apache.hama.myhama.comm.SuperStepReport;
import org.apache.hama.myhama.graph.GraphDataServer;
import org.apache.hama.myhama.graph.GraphDataServerDisk;
import org.apache.hama.myhama.graph.GraphDataServerMem;
import org.apache.hama.myhama.graph.MsgDataServer;
import org.apache.hama.myhama.util.Counters;
import org.apache.hama.myhama.util.GraphContext;
import org.apache.hama.myhama.util.TaskReportTimer;
import org.apache.hama.myhama.util.Counters.COUNTER;
import org.apache.hama.myhama.util.TaskReportContainer;

/**
 * Base class for task. 
 *
 * @author 
 * @version 0.1
 */
public class BSPTask<V, W, M, I> extends Task {
	private static final Log LOG = LogFactory.getLog(BSPTask.class);
	
	private BytesWritable rawSplit = new BytesWritable();
	private String rawSplitClass;
	
	/** 
	 * Establish network connection with {@link JobInProgress}, 
	 * in order to perform barriers (sending information).
	 * */
	private MasterProtocol jobServer;
	/**
	 * Establish network connection with {@link JobInProgress} 
	 * and other {@link BSPTask}, to perform barriers (receiving 
	 * information from {@link JobInProgress}), and send/receive 
	 * {@link MsgRecord}s among {@link BSPTask}s.
	 */
	private CommunicationServer<V, W, M, I> commServer;
	/** Manage graph data {@link GraphRecord}s (memory/disk) */
	private GraphDataServer<V, W, M, I> graphDataServer;
	/** Manage message data {@link MsgRecord}s (memory/disk) */
	private MsgDataServer<V, W, M, I> msgDataServer;
	
	private int iteNum = 0;
	private boolean termination;
	private BSP<V, W, M, I> bsp;
	private TaskInformation taskInfo;
	private float jobAgg = 0.0f; //the global aggregator
	private float taskAgg = 0.0f; //the local aggregator
	
	/** Runtime statistics collected after completing an iteration */
	private Counters counters;
	/** Runtime statistics collected within an iteration */
	TaskReportContainer report; 
	/** Periodically send {@link TaskReportContainer} to {@link BSPMaster}. */
	private TaskReportTimer reportTimer;
	
	/** 
	 * Memory space (bytes) required in the previous 
	 * iteration, accurately computed by {@link GraphDataServer}
	 * */
	private long memUsage = 0L;

	/** Style of this job (PUSH, PULL, or HYBRID) */
	private int bspStyle;
	/** Style of each iteration, updated per iteration when bspStyle=HYBRID */
	private int preIteStyle, curIteStyle; //pre=cur at the 1st iteration
	/** Need to estimate the I/O cost (bytes) of PULL? */
	private boolean estimatePullByte;
	/** Skip local computations or not, during failure recovery? */
	private boolean skipLocalComputation;
	
	public BSPTask() {
		
	}
	
	public BSPTask(BSPJobID jobId, String jobFile, TaskAttemptID taskid, 
			int parId, String splitClass, BytesWritable split, boolean _restart) {
		this.jobId = jobId;
		this.jobFile = jobFile;
		this.taskId = taskid;
		this.parId = parId;
		this.rawSplitClass = splitClass;
		this.rawSplit = split;
		this.restart = _restart;
	}

	@Override
	public BSPTaskRunner createRunner(GroomServer groom) {
		return new BSPTaskRunner(this, groom, job);
	}
	
	/**
	 * Get progress of this task, including progress and memory information.
	 * @return
	 * @throws Exception
	 */
	public TaskReportContainer getProgress() throws Exception {
		if (iteNum == 0) {
			//progress of loading graph
			report.updateCurrentProgress(graphDataServer.getProgress());
		} else {
			//progress of the current iteration
			report.updateCurrentProgress();
		}
		
		if (report.isProgressUpdated()) { 
			//send current progress only when the progress is updated
			report.finalizeCurrentProgress();
			return report;
		} else {
			return null;
		}
	}
	
	/**
	 * Update the memory information.
	 * Now, this function is invoked by beginSuperStep() 
	 * once at every two iteration since it is time-consuming.
	 */
	private void updateMemInfo() {
		MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
        float totalMem = (float)memoryUsage.getMax() / (1024 * 1024);
        float usedMem = (float)memoryUsage.getUsed() / (1024 * 1024);
        report.updateMemoryInfo(usedMem, totalMem);
	}

	/**
	 * Initialize all variables before executing the computation.
	 * @param job
	 * @param host
	 */
	private void initialize(BSPJob _job, String _hostName, 
			BSPTaskTrackerProtocol umbilical) throws Exception {
		job = _job;
		job.set("host", _hostName);
		bspStyle = job.getBspStyle();
		preIteStyle = job.getStartIteStyle();
		curIteStyle = preIteStyle;
		estimatePullByte = false;
		switch(bspStyle) {
		case Constants.STYLE.Push:
			LOG.info("initialize BspStyle = STYLE.Push, " +
					"IteStyle = STYLE.Push");
			break;
		case Constants.STYLE.Pull:
			LOG.info("initialize BspStyle = STYLE.Pull, " +
					"IteStyle = STYLE.Pull");
			break;
		case Constants.STYLE.Hybrid:
			if (preIteStyle == Constants.STYLE.Push) {
				LOG.info("initialize BspStyle = STYLE.Hybrid," +
						" IteStyle = STYLE.Push");
			} else {
				LOG.info("initialize BspStyle = STYLE.Hybrid," +
						" IteStyle = STYLE.Pull");
			}
			break;
		default:
			throw new Exception("invalid bspStyle=" + bspStyle);
		}
		
		if (job.isGraphDataOnDisk()) {
			graphDataServer = 
				new GraphDataServerDisk<V, W, M, I>(parId, job, 
					getRootDir(umbilical)+"/"+Constants.Graph_Dir);
		} else {
			graphDataServer = 
				new GraphDataServerMem<V, W, M, I>(parId, job, 
					getRootDir(umbilical)+"/"+Constants.Graph_Dir);
		}
		
		msgDataServer = new MsgDataServer<V, W, M, I>();
		commServer = 
			new CommunicationServer<V, W, M, I>(job, parId, taskId, 
					umbilical.getPort());
		jobServer = (MasterProtocol) RPC.waitForProxy(
				MasterProtocol.class, MasterProtocol.versionID,
				BSPMaster.getAddress(job.getConf()), job.getConf());
		
		counters = new Counters();
		iteNum = 0;
		termination = false;
		bsp = (BSP<V, W, M, I>) 
			ReflectionUtils.newInstance(job.getConf().getClass(
				"bsp.work.class", BSP.class), job.getConf());
		
		report = new TaskReportContainer();
		reportTimer = new TaskReportTimer(jobId, taskId, this, 3000);
	}
	
	/**
	 * Build route table and get the global information 
	 * about real and virtual hash buckets.
	 * First read only one {@link GraphRecord} and get the min vertex id.
	 * Second, report {@link TaskInformation} to the {@link JobInProgress}.
	 * The report information includes: verMinId, parId, 
	 * RPC server port and hostName.
	 * This function should be invoked before load().
	 * @throws Exception
	 */
	private void buildRouteTable(BSPTaskTrackerProtocol umbilical) 
			throws Exception {
		int verMinId = graphDataServer.getVerMinId(rawSplit, rawSplitClass);
		taskInfo = new TaskInformation(parId, verMinId, 
				commServer.getPort(), commServer.getAddress(), 
				graphDataServer.getByteOfOneMessage(), 
				graphDataServer.isAccumulated());
		
		LOG.info("enter the buildRouteTable() barrier");
		jobServer.buildRouteTable(jobId, taskInfo);
		commServer.suspend();
		LOG.info("leave the buildRouteTable() barrier");
		
		taskInfo.init(commServer.getJobInformation());
		
		report.setFullWorkload(taskInfo.getBlkNum());
		reportTimer.setAgent(umbilical);
		reportTimer.start();
	}
	
	/**
	 * Load data from HDFS, build VE-Block, 
	 * and then save them on the local disk.
	 * After that, begin to register to the {@link JobInProgress} to 
	 * report {@link TaskInformation}.
	 * The report information includes: #edges, 
	 * the relationship among virtual buckets.
	 */
	private void loadData(BSPTaskTrackerProtocol umbilical) 
			throws Exception {
		graphDataServer.initialize(taskInfo, 
				commServer.getCommRouteTable(), taskId);
		graphDataServer.initMemOrDiskMetaData();
		graphDataServer.loadGraphData(taskInfo, rawSplit, rawSplitClass);
		
		msgDataServer.init(job, taskInfo.getBlkLen(), taskInfo.getBlkNum(), 
				graphDataServer.getLocBucMinIds(), parId, getRootDir(umbilical));
		graphDataServer.bindMsgDataServer(msgDataServer);
		commServer.bindMsgDataServer(msgDataServer);
		commServer.bindGraphData(graphDataServer, taskInfo.getBlkNum());
		
		LOG.info("enter the registerTask() barrier");
		jobServer.registerTask(jobId, taskInfo);
		commServer.suspend();
		LOG.info("leave the registerTask() barrier");
	}
	
	/**
	 * Initialization for a restart task. 
	 * This function should be invoked right after loadData().
	 * @throws Exception
	 */
	private void recoveryInitialize() throws Exception {
		// Get the command from JobInProgress.
		SuperStepCommand ssc = commServer.getNextSuperStepCommand();
		jobAgg = ssc.getJobAgg();
		
		//this.preIteStyle = this.curIteStyle;
		preIteStyle = ssc.getPreIteStyle();
		curIteStyle = ssc.getCurIteStyle();
		iteNum = ssc.getIteNum();
		estimatePullByte = ssc.isEstimatePullByte();
		iteNum++;
		graphDataServer.loadCheckPoint(iteNum, 
				ssc.getAvailableCheckPointVersion());
		int flagOpt = -1;
		if ((job.getCheckPointPolicy()==
			Constants.CheckPoint.Policy.ConfinedRecoveryLogVert)
			|| (job.getCheckPointPolicy()==
				Constants.CheckPoint.Policy.ConfinedRecoveryLogMsg)) {
			flagOpt = 1; //log flags
		}
		graphDataServer.clearAftIte(iteNum-1, flagOpt); //re-execute, log flags
	}
	
	/**
	 * Do some preparetion work at the beginning of a new SuperStep.
	 * Do not report or get any information.
	 */
	private void beginIteration() throws Exception {
		LOG.info(print("=", 14) + " begin superstep-" + iteNum 
			  + " " + print("=", 14));
		
		counters.clearValues();
		taskAgg = 0.0f; //clear the local aggregator
		report.clearBefIte();
		memUsage = graphDataServer.getAndClearMemUsage()
						+ msgDataServer.getAndClearMemUsage();
		
		graphDataServer.clearBefIte(iteNum, preIteStyle, 
				curIteStyle, estimatePullByte);
		graphDataServer.clearBefIteMemOrDisk(iteNum);
		commServer.clearBefIte(iteNum, curIteStyle);
		msgDataServer.clearBefIte(iteNum, preIteStyle, curIteStyle);
		taskInfo.clear();
		setSkipLocalComputation(false);
		if (iteNum%2 == 0) {
			updateMemInfo();
		}
		
		if (iteNum > 1) {
			SuperStepCommand ssc = commServer.getNextSuperStepCommand();
			LOG.info("superstep command: \n" + ssc.toString());
			termination = false;
			switch (ssc.getCommandType()) {
			case RECOVERED:
		  	case START:
		  		graphDataServer.setUncompletedIteration(-1); //disable
		  		break;
		  	case ARCHIVE:
		  		graphDataServer.archiveCheckPoint(iteNum, iteNum);
		  		graphDataServer.setUncompletedIteration(-1); //disable
		  		break;
		  	case RECOVER:
		  		if (isRestart()) {
		  			LOG.info("i am a restart task");
		  		} else {
		  			LOG.info("i am a surviving task");
		  			setSkipLocalComputation(true);
		  		}
		  		break;
		  	case TERMITE:
		  		termination = true;
		  		break;
		  	default:
		  		throw new Exception("[Invalid Command Type] " 
		  				+ ssc.getCommandType());
			}
		}
		
		LOG.info("enter the beginSuperStep() barrier");
		jobServer.beginSuperStep(jobId, parId);
		commServer.suspend();
		LOG.info("leave the beginSuperStep() barrier");
	}
	
	/**
	 * Collect the task's information, update {@link SuperStepReport}, 
	 * and then send it to {@link JobInProgress}.
	 * After that, the local task will block itself 
	 * and wait for {@link SuperStepCommand} from 
	 * {@link JobInProgress} for the next SuperStep.
	 */
	private void finishIteration() throws Exception {
		SuperStepReport ssr = new SuperStepReport(); //collect local information
		ssr.setCounters(this.counters);
		ssr.setTaskAgg(this.taskAgg);
		ssr.setActVerNumBucs(this.taskInfo.getRespondVerNumBlks());
		//LOG.info("local information is as follows:\n" + ssr.toString());
		
		LOG.info("enter the finishSuperStep() barrier");
		this.jobServer.finishSuperStep(this.jobId, this.parId, ssr);
		this.commServer.suspend();
		LOG.info("leave the finishSuperStep() barrier");
		
		// Get the command from JobInProgress.
		SuperStepCommand ssc = this.commServer.getNextSuperStepCommand();
		this.jobAgg = ssc.getJobAgg();
		
		//this.preIteStyle = this.curIteStyle;
		this.preIteStyle = ssc.getPreIteStyle();
		this.curIteStyle = ssc.getCurIteStyle();
		this.iteNum = ssc.getIteNum();
		int flagOpt = -1; //do nothing
		if ((job.getCheckPointPolicy()==
			Constants.CheckPoint.Policy.ConfinedRecoveryLogVert) 
			|| (job.getCheckPointPolicy()==
				Constants.CheckPoint.Policy.ConfinedRecoveryLogMsg)) {
			flagOpt = 1; //log flags
			//surviving tasks will not log flags during failure recovery 
			//since they have been logged in previous iterations.
			if (!isRestart()) {
				if ((ssc.getCommandType()==Constants.CommandType.RECOVER) 
						|| (ssc.getCommandType()==Constants.CommandType.RECOVERED))
				flagOpt = 2; //load flags
			}
		}
		
		this.graphDataServer.clearAftIte(iteNum, flagOpt);
		this.estimatePullByte = ssc.isEstimatePullByte();
		if (this.curIteStyle!=Constants.STYLE.Push &&
				this.curIteStyle!=Constants.STYLE.Pull) {
			throw new Exception("invalid curIteStyle=" + this.curIteStyle);
		}
		
		LOG.info(print("=", 15) + " end superstep-" + iteNum
			  + " " + print("=", 15) + "\n");
	}
	
	/**
	 * Save local results onto distributed file system, such as HDFS.
	 * @throws Exception
	 */
	private void saveResult() throws Exception {
		int num = this.graphDataServer.saveAll(taskId, iteNum);
        LOG.info("enter the saveResultOver() barrier");
		this.jobServer.dumpResult(jobId, parId, num);
		LOG.info("leave the saveResultOver() barrier");
	}
	
	/**
	 * Whether to process this VBlock or not.
	 * @param VBlockUpdateRule
	 * @param msgNum
	 * @return
	 */
	private boolean isUpdateVBlock(VBlockUpdateRule rule, long msgNum) 
			throws Exception {
		boolean update = false;
		if (isSkipLocalComputation()) {
			return update;
		}
		
		switch (rule) {
		case UPDATE:
			update = true;
			break;
		case SKIP:
			update = false;
			break;
		case MSG_DEPEND:
			if (iteNum == 1) {
				update = true;
			} else {
				update = msgNum>0L? true:false;
			}
			break;
		default:
			throw new Exception("Invalid update rule " + rule);
		}
		
		return update;
	}
	
	/**
	 * Execute the local computation for a real hash bucket.
	 * Note that messages have been collected before invoking this function.
	 * The procedure is: read local graph data, 
	 * update local values, compute send values, 
	 * and then save them.
	 * @param bucketId
	 * @return
	 * @throws Exception
	 */
	private long runBucket(int bucketId) throws Exception {
		long bucStaTime, bucEndTime;
		bucStaTime = System.currentTimeMillis();
		GraphContext<V, W, M, I> context = 
			new GraphContext<V, W, M, I>(this.parId, this.job, 
					this.iteNum, this.curIteStyle, 
					this.commServer.getCommRouteTable());
		context.setVBlockId(bucketId);
		GraphRecord<V, W, M, I> graph = null;
		this.graphDataServer.openGraphDataStream(parId, bucketId, iteNum);
		
		while (this.graphDataServer.hasNextGraphRecord(bucketId)) {
			graph = this.graphDataServer.getNextGraphRecord(bucketId);
			context.reset();
			if (isActive(bucketId, graph.getVerId())) {
				MsgRecord<M> msg = this.msgDataServer.getMsg(bucketId, graph.getVerId());
				context.initialize(graph, msg, this.jobAgg, true);
				this.bsp.update(context); //execute the local computation
				this.taskAgg += context.getVertexAgg();
				
				this.counters.addCounter(COUNTER.Vert_Active, 1);
				if (context.isRespond()) {
					this.counters.addCounter(COUNTER.Vert_Respond, 1);
					
					if (this.preIteStyle==Constants.STYLE.Push && 
							this.curIteStyle==Constants.STYLE.Push) {
						MsgRecord<M>[] msgs = this.bsp.getMessages(context);
						this.commServer.pushMsgData(msgs);
					}
				}
			} else {
				context.voteToHalt();
			}
			this.graphDataServer.saveGraphRecord(bucketId, iteNum, 
					context.isActive(), context.isRespond());
			this.counters.addCounter(COUNTER.Vert_Read, 1);
		}
		
		this.graphDataServer.closeGraphDataStream(parId, bucketId, iteNum);
		bucEndTime = System.currentTimeMillis();
		return (bucEndTime-bucStaTime);
	}
	
	/**
	 * Is active or not for a given vertex.
	 * Return true if the active flag at the previous iteration is true 
	 * or this vertex has received new {@link MsgRecord}.
	 * @param _bid
	 * @param _vid
	 * @return
	 */
	private boolean isActive(int _bid, int _vid) {
		if (this.graphDataServer.isActive(_vid) 
				|| this.msgDataServer.hasMsg(_bid, _vid)) {
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * Run an iteration.
	 * @throws Exception
	 */
	private void runIteration() throws Exception {
		long iteStaTime, iteEndTime, msgTime = 0, compTime = 0, totalMsgTime=0;
		StringBuffer hbInfo = new StringBuffer();
		GraphContext<V, W, M, I> context = 
			new GraphContext<V, W, M, I>(this.parId, this.job, 
					this.iteNum, this.curIteStyle, 
					this.commServer.getCommRouteTable());
		int bucNum = this.taskInfo.getBlkNum();
		
		this.reportTimer.force();
		this.bsp.superstepSetup(context);
		
		hbInfo.setLength(0);
		hbInfo.append("begin local computations");
		iteStaTime = System.currentTimeMillis();
		for (int bucketId = 0; bucketId < bucNum; bucketId++) {
			hbInfo.append("\nVBlockId=" + bucketId); //loop real buckets
			this.msgDataServer.clearBefBucket(); //prepare to collect msgs
			msgTime = 0L;
			if (!isSkipLocalComputation()) {
				switch(bspStyle) {
				case Constants.STYLE.Push: 
					msgTime += msgDataServer.pullMsgFromLocal(bucketId, iteNum);
					break;
				case Constants.STYLE.Pull:
					msgTime += commServer.pullMsgFromSource(bucketId, iteNum);
					break;
				case Constants.STYLE.Hybrid:
					if (preIteStyle == Constants.STYLE.Push) {
						msgTime += msgDataServer.pullMsgFromLocal(bucketId, iteNum);
					} else if (preIteStyle == Constants.STYLE.Pull) {
						msgTime += commServer.pullMsgFromSource(bucketId, iteNum);
					} else {
						throw new Exception("invalid preIteStyle=" + preIteStyle);
					}
					break;
				}
			}
			
			if (commServer.findConnectionError()) {
				graphDataServer.setUncompletedIteration(iteNum);
				break;
			}
			
			long msgNum = this.msgDataServer.getMsgNum();
			totalMsgTime += msgTime;
			hbInfo.append("\tpullMsgTime=" + msgTime + "ms");
			
			context.setVBlockId(bucketId);
			this.bsp.vBlockSetup(context);;
			if (isUpdateVBlock(context.getVBlockUpdateRule(), msgNum)) {
				hbInfo.append("\tType=Normal");
				compTime = runBucket(bucketId);
				hbInfo.append("\tcompTime=" + compTime + "ms");
			} else {
				hbInfo.append("\tType=Skip\tcompTime=0ms");
				this.graphDataServer.skipBucket(parId, bucketId, iteNum);
			}
			report.completeWorkload();
			msgDataServer.clearAftBucket();
			bsp.vBlockCleanup(context);
		}
		
		this.taskInfo.setRespondVerNumBlks(this.graphDataServer.getRespondVerNumOfBlks());
		
		/** switch from Pull to Push in auto-version: first pull, and then push */
		if (this.preIteStyle==Constants.STYLE.Pull &&
				this.curIteStyle==Constants.STYLE.Push) {
			this.jobServer.sync(jobId, parId);
			this.commServer.suspend(); //ensure all tasks complete Pull
			this.runIterationOnlyForPush();
		}
		
		if (this.curIteStyle == Constants.STYLE.Push) {
			this.commServer.pushFlushMsgData();
		}
		
		iteEndTime = System.currentTimeMillis();
		this.counters.addCounter(COUNTER.Time_Pull, totalMsgTime);
		this.counters.addCounter(COUNTER.Time_Ite, (iteEndTime-iteStaTime));
		this.bsp.superstepCleanup(context);
		LOG.info(hbInfo.toString());
		LOG.info("complete local computations");
	}
	
	/**
	 * Run an iteration to generate and push messages based on current vertex values.
	 * This is invoked when switching from Pull to Push.
	 * The logic is similar to that of {@link GraphDataServer.getMsg()}, 
	 * i.e., the Pull operation in the next iteration.
	 * @throws Exception
	 */
	private void runIterationOnlyForPush() throws Exception {
		int bucNum = this.taskInfo.getBlkNum();
		
		int nextIteNum = iteNum + 1; //simulate the pull process of next superstep.
		this.graphDataServer.clearAftIte(iteNum, -1);
		this.graphDataServer.clearOnlyForPush(nextIteNum);
		
		GraphRecord<V, W, M, I> graph = null;
		GraphContext<V, W, M, I> context = 
			new GraphContext<V, W, M, I>(this.parId, this.job, 
					this.iteNum, this.curIteStyle, 
					this.commServer.getCommRouteTable());
		for (int bucketId = 0; bucketId < bucNum; bucketId++) {
			if (this.graphDataServer.isDoOnlyForPush(bucketId, nextIteNum)) {
				/** if not updated, will not read. If read it, 
				 * it must be updated and saved as iteNum+1. */
				this.graphDataServer.openGraphDataStreamOnlyForPush(
						parId, bucketId, nextIteNum);

				while (this.graphDataServer.hasNextGraphRecord(bucketId)) {
					graph = this.graphDataServer.getNextGraphRecordOnlyForPush(bucketId);
					if (this.graphDataServer.isUpdatedOnlyForPush(
							bucketId, graph.getVerId(), nextIteNum)) {
						context.reset();
						context.initialize(graph, null, this.jobAgg, true);
						MsgRecord<M>[] msgs = this.bsp.getMessages(context);
						this.commServer.pushMsgData(msgs);
					}
				}
				
				this.graphDataServer.closeGraphDataStreamOnlyForPush(
						parId, bucketId, iteNum);
			}
		}
	}
	
	/**
	 * Run and control all work of this task.
	 */
	@Override
	public void run(BSPJob job, Task task, BSPTaskTrackerProtocol umbilical, 
			String host) {
		LOG.info("\n" + print("=*", 55));
		if (isRestart()) {
			LOG.info(this.taskId + " restarts");
		} else {
			LOG.info(this.taskId + " starts");
		}
		Exception exception = null;
		try {
			initialize(job, host, umbilical);
			buildRouteTable(umbilical); //get the locMinVerId of each task
			loadData(umbilical);
			if (isRestart()) {
				//iteNum will be reset by the recoveryInitialize() function
				recoveryInitialize();
			} else {
				this.iteNum = 1;
			}
			
			GraphContext<V, W, M, I> context = 
				new GraphContext<V, W, M, I>(this.parId, job, -1, -1, 
						this.commServer.getCommRouteTable());
			this.bsp.taskSetup(context);
			
			/** run the job iteration by iteration */
			while (true) {
				beginIteration(); //preprocess before starting one iteration
				if (termination) {
					break;
				}
				
				/** Simulate an exception: only once. */
				if (job.getFailedIteration()==iteNum 
						&& parId>=(job.getNumBspTask()-job.getNumOfFailedTasks()) 
						&& !isRestart()) {
					throw new Exception("a simulated exception");
				}
				runIteration(); //run one iteration
				
				updateCounters();
				finishIteration(); //report counters and get the next command
				iteNum++;
			}
			
			saveResult(); //save results
			LOG.info("task is done");
		} catch (Exception e) {
			exception = e;
			LOG.error("task fails", e);
		} finally {
			LOG.info("shutdown in progress...");
			GraphContext<V, W, M, I> context = 
				new GraphContext<V, W, M, I>(this.parId, job, -1, -1, 
						this.commServer.getCommRouteTable());
			this.bsp.taskCleanup(context);
			
			try {
				clear();
			} catch (Exception e) {
				LOG.error("clear()", e);
			}
			
			if (exception != null) {
				umbilical.runtimeError(jobId, taskId);
			} 
			
			report.setDone(true);
			umbilical.ping(jobId, taskId, report); //termination
			LOG.info("goodbye\n" + print("=*", 55) + "=\n");
		}
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
	
	private void updateCounters() throws Exception {
		/** actual I/O bytes */
		this.counters.addCounter(COUNTER.Byte_Actual, 
				this.commServer.getIOByte());
		this.counters.addCounter(COUNTER.Byte_LOG, 
				this.commServer.getIOByteOfLoggedMsg());
		//LOG.info("bytes of logging messages are: " + this.commServer.getIOByteOfLoggedMsg());
		this.counters.addCounter(COUNTER.Byte_Actual, 
				this.graphDataServer.getLocVerIOByte());
		this.counters.addCounter(COUNTER.Byte_Actual, 
				this.graphDataServer.getLocInfoIOByte());
		this.counters.addCounter(COUNTER.Byte_Actual, 
				this.graphDataServer.getLocAdjEdgeIOByte());
		this.counters.addCounter(COUNTER.Byte_Actual, 
				this.msgDataServer.getLocMsgIOByte());
		
		/** bytes of style.Push, 
		 * including vertices, messages, and edges in the adjacency list */
		this.counters.addCounter(COUNTER.Byte_Push, 
				this.graphDataServer.getLocVerIOByte());
		this.counters.addCounter(COUNTER.Byte_Push, 
				this.msgDataServer.getLocMsgIOByte());
		if (this.estimatePullByte) {
			//curIteStyle=Push
			this.counters.addCounter(COUNTER.Byte_Push, 
				this.graphDataServer.getLocAdjEdgeIOByte());
		} else {
			this.counters.addCounter(COUNTER.Byte_Push, 
				this.graphDataServer.getEstimatedPushBytes(iteNum));
		}
		
		/** bytes of style.Pull */
		this.counters.addCounter(COUNTER.Byte_Pull, 
				this.graphDataServer.getLocVerIOByte());
		if (this.estimatePullByte) {
			//curIteStyle=Push
			this.counters.addCounter(COUNTER.Byte_Pull, 
				this.graphDataServer.getEstimatedPullBytes(iteNum));
			this.counters.addCounter(COUNTER.Byte_Pull_Vert, 
				this.graphDataServer.getEstimatePullVertBytes(iteNum));
		} else {
			this.counters.addCounter(COUNTER.Byte_Pull, 
				this.commServer.getIOByte());
			this.counters.addCounter(COUNTER.Byte_Pull, 
				this.graphDataServer.getLocInfoIOByte());
			this.counters.addCounter(COUNTER.Byte_Pull, 
				this.graphDataServer.getLocAdjEdgeIOByte());
			this.counters.addCounter(COUNTER.Byte_Pull_Vert, 
				this.commServer.getIOByteOfVertInPull());
		}
		
		this.counters.addCounter(COUNTER.Edge_Read, 
				this.commServer.getReadEdgeNum());
		this.counters.addCounter(COUNTER.Edge_Read, 
				this.graphDataServer.getLocReadAdjEdgeNum());
		this.counters.addCounter(COUNTER.Fragment_Read, 
				this.commServer.getReadFragmentNum());
		
		this.counters.addCounter(COUNTER.Msg_Produced, 
				this.commServer.getMsgProNum());
		this.counters.addCounter(COUNTER.Msg_Received, 
				this.commServer.getMsgRecNum());
		this.counters.addCounter(COUNTER.Msg_Net, 
				this.commServer.getMsgNetNum());
		this.counters.addCounter(COUNTER.Msg_Net_Actual, 
				this.commServer.getMsgNetActualNum());
		this.counters.addCounter(COUNTER.Msg_Disk, 
				this.commServer.getMsgOnDisk());
		
		this.counters.addCounter(COUNTER.Mem_Used, 
				this.memUsage);
	}
	
	@SuppressWarnings("deprecation")
	private void clear() throws Exception {
		this.graphDataServer.close();
		this.msgDataServer.close();
		this.commServer.close();
		this.reportTimer.stop();
	}
	
	private String getRootDir(BSPTaskTrackerProtocol umbilical) 
			throws IOException {
		return umbilical.getLocalTaskDir(jobId, taskId);
	}
	
	private void setSkipLocalComputation(boolean flag) {
		skipLocalComputation = flag;
	}
	
	private boolean isSkipLocalComputation() {
		return skipLocalComputation;
	}
	
	@Override
	public void setBSPJob(BSPJob job) {
		this.job = job;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		Text.writeString(out, rawSplitClass);
		rawSplit.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		rawSplitClass = Text.readString(in);
		rawSplit.readFields(in);
	}
}
