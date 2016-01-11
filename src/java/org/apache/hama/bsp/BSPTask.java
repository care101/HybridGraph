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
	
	private String rootDir;
	private BytesWritable rawSplit = new BytesWritable();
	private String rawSplitClass;
	
	private MasterProtocol reportServer;
	private CommunicationServer<V, W, M, I> commServer;
	private GraphDataServer<V, W, M, I> graphDataServer;
	private MsgDataServer<V, W, M, I> msgDataServer;
	
	private int iteNum = 0;
	private boolean conExe;
	private BSP<V, W, M, I> bsp;
	private SuperStepCommand ssc;
	private TaskInformation taskInfo;
	private float jobAgg = 0.0f; //the global aggregator
	private float taskAgg = 0.0f; //the local aggregator
	
	private Counters counters; //count some variables
	private int fulLoad = 0; //its value is equal with #local buckets
	private int hasPro = 0; //its value is equal with #processed buckets
	//java estimated, just accurately at the sampling point.
	private float totalMem, usedMem; 
	//self-computed in bytes. maximum value. 
	//record the memUsage of previous iteration.
	private long memUsage; 
	private float last, cur;
	private TaskReportTimer trt;
	/** General Style of BSP, Push, Pull, or Hybrid, 
	 * static during iterations*/
	private int bspStyle;
	/** Style of each iteration when bspStyle=Hybrid, 
	 * updated at each iteration */
	private int preIteStyle; //previous ite
	private int curIteStyle; //current ite, at 1st iteration, they are equal.
	private boolean estimatePullByte;
	
	public BSPTask() {
		
	}
	
	public BSPTask(BSPJobID jobId, String jobFile, TaskAttemptID taskid,
			int parId, String splitClass, BytesWritable split) {
		this.jobId = jobId;
		this.jobFile = jobFile;
		this.taskId = taskid;
		this.parId = parId;
		this.rawSplitClass = splitClass;
		this.rawSplit = split;
	}

	@Override
	public BSPTaskRunner createRunner(GroomServer groom) {
		return new BSPTaskRunner(this, groom, this.job);
	}
	
	/**
	 * Get progress of this task, including progress and memory information.
	 * @return
	 * @throws Exception
	 */
	public TaskReportContainer getProgress() throws Exception {
		this.cur = iteNum==0? 
				graphDataServer.getProgress():(float)hasPro/fulLoad;
		if (last != cur) { 
			//update and send current progress only when the progress is changed
			TaskReportContainer taskRepCon = 
				new TaskReportContainer(cur, usedMem, totalMem);
			last = cur;
			return taskRepCon;
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
        this.totalMem = (float)memoryUsage.getMax() / (1024 * 1024);
        this.usedMem = (float)memoryUsage.getUsed() / (1024 * 1024);
	}

	/**
	 * Initialize all variables before executing the computation.
	 * @param job
	 * @param host
	 */
	private void initialize(BSPJob job, String hostName) throws Exception {
		this.job = job;
		this.job.set("host", hostName);
		this.bspStyle = this.job.getBspStyle();
		this.preIteStyle = this.job.getStartIteStyle();
		this.curIteStyle = this.preIteStyle;
		this.estimatePullByte = false;
		switch(this.bspStyle) {
		case Constants.STYLE.Push:
			LOG.info("initialize BspStyle = STYLE.Push, IteStyle = STYLE.Push");
			break;
		case Constants.STYLE.Pull:
			LOG.info("initialize BspStyle = STYLE.Pull, IteStyle = STYLE.Pull");
			break;
		case Constants.STYLE.Hybrid:
			if (this.preIteStyle == Constants.STYLE.Push) {
				LOG.info("initialize BspStyle = STYLE.Hybrid," +
						" IteStyle = STYLE.Push");
			} else {
				LOG.info("initialize BspStyle = STYLE.Hybrid," +
						" IteStyle = STYLE.Pull");
			}
			break;
		default:
			throw new Exception("invalid bspStyle=" + this.bspStyle);
		}
		
		this.rootDir = this.job.get("bsp.local.dir") + "/" + this.jobId.toString()
				+ "/task-" + this.parId;
		if (this.job.isGraphDataOnDisk()) {
			this.graphDataServer = 
				new GraphDataServerDisk<V, W, M, I>(this.parId, this.job, 
					this.rootDir+"/"+Constants.Graph_Dir);
		} else {
			this.graphDataServer = 
				new GraphDataServerMem<V, W, M, I>(this.parId, this.job, 
					this.rootDir+"/"+Constants.Graph_Dir);
		}
		
		this.msgDataServer = new MsgDataServer<V, W, M, I>();
		this.commServer = 
			new CommunicationServer<V, W, M, I>(this.job, this.parId, this.taskId);
		this.reportServer = (MasterProtocol) RPC.waitForProxy(
				MasterProtocol.class, MasterProtocol.versionID,
				BSPMaster.getAddress(this.job.getConf()), this.job.getConf());
		
		this.counters = new Counters();
		this.iteNum = 0;
		this.conExe = true;
		this.bsp = (BSP<V, W, M, I>) 
			ReflectionUtils.newInstance(this.job.getConf().getClass(
				"bsp.work.class", BSP.class), this.job.getConf());
		this.trt = new TaskReportTimer(this.jobId, this.taskId, this, 3000);
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
	private void buildRouteTable(BSPPeerProtocol umbilical) throws Exception {
		int verMinId = this.graphDataServer.getVerMinId(this.rawSplit, 
				this.rawSplitClass);
		this.taskInfo = new TaskInformation(this.parId, verMinId, 
				this.commServer.getPort(), this.commServer.getAddress(), 
				this.graphDataServer.getByteOfOneMessage(), 
				this.graphDataServer.isAccumulated());
		
		LOG.info("task enter the buildRouteTable() barrier");
		this.reportServer.buildRouteTable(this.jobId, this.taskInfo);
		this.commServer.barrier();
		LOG.info("task leave the buildRouteTable() barrier");
		
		this.taskInfo.init(this.commServer.getJobInformation());
		this.fulLoad = this.taskInfo.getBlkNum();
		this.trt.setAgent(umbilical);
		this.trt.start();
	}
	
	/**
	 * Load data from HDFS, build VE-Block, 
	 * and then save them on the local disk.
	 * After that, begin to register to the {@link JobInProgress} to 
	 * report {@link TaskInformation}.
	 * The report information includes: #edges, 
	 * the relationship among virtual buckets.
	 */
	private void loadData() throws Exception {
		this.graphDataServer.initialize(this.taskInfo, 
				this.commServer.getCommRouteTable());
		this.graphDataServer.initMemOrDiskMetaData();
		this.graphDataServer.loadGraphData(taskInfo, this.rawSplit, 
				this.rawSplitClass);
		
		this.msgDataServer.init(job, taskInfo.getBlkLen(), taskInfo.getBlkNum(), 
				this.graphDataServer.getLocBucMinIds(), 
				this.parId, this.rootDir + "/" + Constants.Graph_Dir);
		this.commServer.bindMsgDataServer(msgDataServer);
		this.commServer.bindGraphData(graphDataServer, this.taskInfo.getBlkNum());
		
		LOG.info("task enter the registerTask() barrier");
		this.reportServer.registerTask(this.jobId, this.taskInfo);
		this.commServer.barrier();
		LOG.info("task leave the registerTask() barrier");
	}
	
	/**
	 * Do some preparetion work at the beginning of a new SuperStep.
	 * Do not report or get any information.
	 */
	private void beginIteration() throws Exception {
		LOG.info("==================== Begin SuperStep-" + this.iteNum 
			  + " ====================");
		
		this.counters.clearValues();
		this.taskAgg = 0.0f; //clear the local aggregator
		this.hasPro = 0; // clear load
		this.memUsage = this.graphDataServer.getAndClearMemUsage();
		this.memUsage += this.msgDataServer.getAndClearMemUsage();
		
		this.graphDataServer.clearBefIte(iteNum, this.preIteStyle, this.curIteStyle, 
				this.estimatePullByte);
		this.graphDataServer.clearBefIteMemOrDisk(iteNum);
		this.commServer.clearBefIte(iteNum, this.curIteStyle);
		this.msgDataServer.clearBefIte(iteNum, this.preIteStyle, this.curIteStyle);
		this.taskInfo.clear();
		if (iteNum%2 == 0) {
			updateMemInfo();
		}
		
		LOG.info("task enter the beginSuperStep() barrier");
		this.reportServer.beginSuperStep(this.jobId, this.parId);
		this.commServer.barrier();
		LOG.info("task leave the beginSuperStep() barrier");
	}
	
	/**
	 * Collect the task's information, update {@link SuperStepReport}, 
	 * and then send it to {@link JobInProgress}.
	 * After that, the local task will block itself 
	 * and wait for {@link SuperStepCommand} from 
	 * {@link JobInProgress} for the next SuperStep.
	 */
	private void finishIteration() throws Exception {
		this.graphDataServer.clearAftIte(iteNum);
		SuperStepReport ssr = new SuperStepReport(); //collect local information
		
		ssr.setCounters(this.counters);
		ssr.setTaskAgg(this.taskAgg);
		ssr.setActVerNumBucs(this.taskInfo.getRespondVerNumBlks());
		LOG.info("the local information is as follows:\n" + ssr.toString());
		
		LOG.info("task enter the finishSuperStep() barrier");
		this.reportServer.finishSuperStep(this.jobId, this.parId, ssr);
		this.commServer.barrier();
		LOG.info("task leave the finishSuperStep() barrier");
		
		// Get the command from JobInProgress.
		this.ssc = this.commServer.getNextSuperStepCommand();
		this.jobAgg = this.ssc.getJobAgg();
		
		this.preIteStyle = this.curIteStyle;
		this.curIteStyle = this.ssc.getIteStyle();
		this.estimatePullByte = this.ssc.isEstimatePullByte();
		if (this.curIteStyle!=Constants.STYLE.Push &&
				this.curIteStyle!=Constants.STYLE.Pull) {
			throw new Exception("invalid curIteStyle=" + this.curIteStyle);
		}
		switch (this.ssc.getCommandType()) {
	  	case START:
	  	case CHECKPOINT:
	  	case RECOVERY:
	  		this.conExe = true;
	  		break;
	  	case STOP:
	  		this.conExe = false;
	  		break;
	  	default:
	  		throw new Exception("[Invalid Command Type] " 
	  				+ this.ssc.getCommandType());
		}
		
		LOG.info("the command information of superstep-" + (iteNum+1) 
				+ "\n" + this.ssc.toString());
		LOG.info("==================== End SuperStep-" + iteNum
				+ " ====================\n");
	}
	
	/**
	 * Save local results onto distributed file system, such as HDFS.
	 * @throws Exception
	 */
	private void saveResult() throws Exception {
		int num = this.graphDataServer.saveAll(taskId, iteNum);
        LOG.info("task enter the saveResultOver() barrier");
		this.reportServer.saveResultOver(jobId, parId, num);
		LOG.info("task leave the saveResultOver() barrier");
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
						this.commServer.pushMsgData(msgs, iteNum);
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
		
		this.trt.force();
		this.bsp.superstepSetup(context);
		
		hbInfo.setLength(0);
		hbInfo.append("begin the calculation of superstep-" + iteNum);
		iteStaTime = System.currentTimeMillis();
		for (int bucketId = 0; bucketId < bucNum; bucketId++) {
			hbInfo.append("\nVBlockId=" + bucketId); //loop real buckets
			this.msgDataServer.clearBefBucket(); //prepare to collect msgs
			msgTime = 0L;
			switch(this.bspStyle) {
			case Constants.STYLE.Push: 
				msgTime += this.msgDataServer.pullMsgFromLocal(bucketId, iteNum);
				break;
			case Constants.STYLE.Pull:
				msgTime += this.commServer.pullMsgFromSource(parId, bucketId, iteNum);
				break;
			case Constants.STYLE.Hybrid:
				if (this.preIteStyle == Constants.STYLE.Push) {
					msgTime += this.msgDataServer.pullMsgFromLocal(bucketId, iteNum);
				} else if (this.preIteStyle == Constants.STYLE.Pull) {
					msgTime += this.commServer.pullMsgFromSource(parId, bucketId, iteNum);
				} else {
					throw new Exception("invalid preIteStyle=" + this.preIteStyle);
				}
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
			this.hasPro++;
			this.msgDataServer.clearAftBucket();
			this.bsp.vBlockCleanup(context);
		}
		
		this.taskInfo.setRespondVerNumBlks(this.graphDataServer.getRespondVerNumOfBlks());
		
		/** switch from Pull to Push in auto-version: first pull, and then push */
		if (this.preIteStyle==Constants.STYLE.Pull &&
				this.curIteStyle==Constants.STYLE.Push) {
			this.reportServer.sync(jobId, parId);
			this.commServer.barrier(); //ensure all tasks complete Pull
			this.runIterationOnlyForPush();
		}
		
		if (this.curIteStyle == Constants.STYLE.Push) {
			this.commServer.pushFlushMsgData(iteNum);
		}
		
		iteEndTime = System.currentTimeMillis();
		this.counters.addCounter(COUNTER.Time_Pull, totalMsgTime);
		this.counters.addCounter(COUNTER.Time_Ite, (iteEndTime-iteStaTime));
		this.bsp.superstepCleanup(context);
		LOG.info(hbInfo.toString());
		LOG.info("complete the calculation of superstep-" + iteNum);
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
		this.graphDataServer.clearAftIte(iteNum);
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
						this.commServer.pushMsgData(msgs, iteNum);
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
	public void run(BSPJob job, Task task, BSPPeerProtocol umbilical, String host) {
		Exception exception = null;
		try {
			initialize(job, host);
			buildRouteTable(umbilical); //get the locMinVerId of each task
			loadData();
			
			GraphContext<V, W, M, I> context = 
				new GraphContext<V, W, M, I>(this.parId, job, -1, -1, 
						this.commServer.getCommRouteTable());
			this.bsp.taskSetup(context);
			this.iteNum = 1; //#iteration starts from 1, not 0.
			
			/** run the job iteration by iteration */
			while (this.conExe) {
				beginIteration(); //preprocess before starting one iteration
				runIteration(); //run one iteration
				
				updateCounters();
				umbilical.increaseSuperStep(jobId, taskId);
				finishIteration(); //syn, report counters and get the next command
				iteNum++;
			}
			
			saveResult(); //save results and prepare to end
			umbilical.clear(this.jobId, this.taskId);
			LOG.info("task is completed successfully!");
		} catch (Exception e) {
			exception = e;
			LOG.error("task is failed!", e);
		} finally {
			GraphContext<V, W, M, I> context = 
				new GraphContext<V, W, M, I>(this.parId, job, -1, -1, 
						this.commServer.getCommRouteTable());
			this.bsp.taskCleanup(context);
			//umbilical.clear(this.jobId, this.taskId);
			try {
				clear();
				done(umbilical);
			} catch (Exception e) {
				//do nothing
			}
			umbilical.reportException(jobId, taskId, exception);
		}
	}
	
	private void updateCounters() throws Exception {
		/** actual I/O bytes */
		this.counters.addCounter(COUNTER.Byte_Actual, 
				this.commServer.getIOByte());
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
		} else {
			this.counters.addCounter(COUNTER.Byte_Pull, 
				this.commServer.getIOByte());
			this.counters.addCounter(COUNTER.Byte_Pull, 
				this.graphDataServer.getLocInfoIOByte());
			this.counters.addCounter(COUNTER.Byte_Pull, 
				this.graphDataServer.getLocAdjEdgeIOByte());
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
		this.trt.stop();
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
