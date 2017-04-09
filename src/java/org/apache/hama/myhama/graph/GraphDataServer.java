package org.apache.hama.myhama.graph;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.monitor.TaskInformation;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.api.UserTool;
import org.apache.hama.myhama.comm.CommRouteTable;
import org.apache.hama.myhama.comm.MsgPack;
import org.apache.hama.myhama.io.InputFormat;
import org.apache.hama.myhama.io.RecordReader;

/**
 * GraphDataServer used to manage graph data. 
 * It is implemented by {@link GraphDataServerMem} for memory 
 * and {@link GraphDataServerDisk} for disk.
 *
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public abstract class GraphDataServer<V, W, M, I> {
	private static final Log LOG = LogFactory.getLog(GraphDataServer.class);
	
	protected BSPJob job;
	protected RecordReader<?,?> input; //used to load graph data
	protected UserTool<V, W, M, I> userTool;
	protected BSP<V, W, M, I> bsp;
	/** messages are accumulated or not? */
	protected boolean isAccumulated;
	protected CommRouteTable<V, W, M, I> commRT;
	protected int taskId;
	
	/** the model of BSP implementation: b-pull, push, or hybrid */
	protected int bspStyle;
	/** the actual model of the previous superstep */
	protected int preIteStyle;
	/** the actual model of the current superstep */
	protected int curIteStyle;
	/** edges in the adjacency list is required at this superstep? */
	protected boolean loadAdjEdge;
	protected boolean loadGraphInfo;
	
	protected VerBlockMgr verBlkMgr; //local VBlock manager
	protected EdgeHashBucMgr edgeBlkMgr; //local EBlock manager
	/** 
	 *  Active-flag, accessed by a single thread. 
	 *  True as default.
	 *  True: this source vertex needs to be updated/computed 
	 *  */
	protected boolean[] actFlag;
	/** 
	 * Responding-flag, accessed by two threads. 
	 * Flags at superstep t are read by getMsg(), i.e., pull-respond(), 
	 * Flags at superstep t+1 are modified by saveGraphRecord(), 
	 *   i.e., pull-request().
	 * All flags are false by default.
	 * True: this source vertex must send messages to its neighbors. */
	protected boolean[][] resFlag;
	
	protected GraphRecord<V, W, M, I> graph_rw;
	protected Long read_adj_edge; //read edges in the adjacency list
	protected Long io_byte_ver; //io cost of updating vertex values
	protected Long io_byte_info; //io cost of reading graphInfo
	protected Long io_byte_adj; //io cost of reading edges in adjacency list
	protected int[] locMinVerIds;
	
	protected boolean estimatePullByteFlag;
	protected long estimatePullByte = 0L;
	protected long estimatePushByte = 0L;
	
	//message buffer for per target task.
	protected ArrayList<ByteArrayOutputStream>[] msgBuf;
	protected ArrayList<Integer>[] msgBufLen;
	protected boolean[] proMsgOver;
	protected int[] packageVersion;
	/** memory usage */
	protected long memUsedByMetaData = 0L; //including VBlocks and EBlocks
	//the maximal memory usage for messages in b-pull
	protected long[] memUsedByMsgPull; 
	
	protected CheckPointManager ckpMgr; //checkpoint
	
	protected int uncompletedIteration; //location where failures happen
	protected MsgDataServer msgDataServer;
	
	@SuppressWarnings("unchecked")
	public GraphDataServer(int _taskId, BSPJob _job) {
		taskId = _taskId;
		job = _job;
		loadAdjEdge = false;
		loadGraphInfo = false;
		bspStyle = _job.getBspStyle();
		
		userTool = 
	    	(UserTool<V, W, M, I>) ReflectionUtils.newInstance(
	    			job.getConf().getClass(Constants.USER_JOB_TOOL_CLASS, 
	    					UserTool.class), job.getConf());
		bsp = 
			(BSP<V, W, M, I>) ReflectionUtils.newInstance(
					this.job.getConf().getClass("bsp.work.class", 
							BSP.class), this.job.getConf());
	    isAccumulated = userTool.isAccumulated();
	    graph_rw = userTool.getGraphRecord();
	}
	
	/**
	 * Initialize the {@link RecordReader} of reading raw graph data on HDFS.
	 * @param rawSplit
	 * @param rawSplitClass
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	protected void initInputSplit(BytesWritable rawSplit, 
			String rawSplitClass) throws Exception {
		DataInputBuffer splitBuffer = new DataInputBuffer();
	    splitBuffer.reset(rawSplit.getBytes(), 0, rawSplit.getLength());
	    SerializationFactory factory = new SerializationFactory(job.getConf());
		
	    Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit>
	      deserializer = 
	        (Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit>) 
	        factory.getDeserializer(job.getConf().getClassByName(rawSplitClass));
	    deserializer.open(splitBuffer);
	    InputSplit split = deserializer.deserialize(null);
	    
		InputFormat inputformat = 
			(InputFormat) ReflectionUtils.newInstance(
					this.job.getConf().getClass( 
							Constants.USER_JOB_INPUT_FORMAT_CLASS, 
                InputFormat.class), this.job.getConf());
		inputformat.initialize(this.job.getConf());
		this.input = inputformat.createRecordReader(split, this.job);
		this.input.initialize(split, job.getConf());
	}
	
	/**
	 * Initialize some variables the {@link GraphDataServer} object 
	 * after that {@link CommRouteTable} has been initialized in 
	 * {@link BSPTask}.buildRouteTable() 
	 * and the number of Vblocks has been calculated/initialized 
	 * by {@link JobInProgress}/the user-specified parameter. 
	 * 
	 * @param taskInfo
	 * @param _commRT
	 * @throws Exception
	 */
	public void initialize(TaskInformation taskInfo, 
			final CommRouteTable<V, W, M, I> _commRT, 
			TaskAttemptID taskAttId) throws Exception {
		commRT = _commRT;
		locMinVerIds = new int[taskInfo.getBlkNum()];
		setUncompletedIteration(-1);
		
		int[] blkNumTask = commRT.getJobInformation().getBlkNumOfTasks();
		int taskNum = commRT.getTaskNum();
		verBlkMgr = new VerBlockMgr(taskInfo.getVerMinId(), 
				taskInfo.getVerMaxId(), 
				taskInfo.getBlkNum(), taskInfo.getBlkLen(), 
				taskNum, blkNumTask, bspStyle);
		
		//int blkNumJob = this.commRT.getJobInformation().getBlkNumOfJob();
		
		int verNum = this.verBlkMgr.getVerNum();
		actFlag = new boolean[verNum]; Arrays.fill(actFlag, true);
		resFlag = new boolean[2][];
		resFlag[0] = new boolean[verNum]; Arrays.fill(resFlag[0], false);
		resFlag[1] = new boolean[verNum]; Arrays.fill(resFlag[1], false);
		
		this.io_byte_ver = 0L;
		this.io_byte_info = 0L;
		this.io_byte_adj = 0L;
		this.read_adj_edge = 0L;
		this.memUsedByMsgPull = new long[taskNum];
		
		/** only used in pull or hybrid */
		if (this.bspStyle != Constants.STYLE.Push) {
			this.edgeBlkMgr = new EdgeHashBucMgr(taskNum, blkNumTask);
			int sumOfBlkNum = 0;
			for (int blk: blkNumTask) {
				sumOfBlkNum += blk;
			}
			this.packageVersion = new int[sumOfBlkNum];
			this.proMsgOver = new boolean[taskNum];
			this.msgBufLen = new ArrayList[taskNum];
			this.msgBuf = new ArrayList[taskNum];
			for (int i = 0; i < taskNum; i++) {
				this.msgBufLen[i] = new ArrayList<Integer>();
				this.msgBuf[i] = new ArrayList<ByteArrayOutputStream>();
			}
		}
		
		ckpMgr = new CheckPointManager(this.job, taskAttId, 
				this.commRT.getCheckPointDirForJob());
	}
	
	public void bindMsgDataServer(MsgDataServer _msgDataServer) {
		msgDataServer = _msgDataServer;
	}
	
	/**
	 * Get the minimum vertex Id by reading 
	 * the first graph record (adjacency list).
	 * 
	 * @return vertexid int
	 * @throws Exception 
	 */
	public int getVerMinId(BytesWritable rawSplit, 
			String rawSplitClass) throws Exception {
		initInputSplit(rawSplit, rawSplitClass);
		int id = 0;
		GraphRecord<V, W, M, I> record = userTool.getGraphRecord();
		if(input.nextKeyValue()) {
			record.parseGraphData(input.getCurrentKey().toString(), 
					input.getCurrentValue().toString());
			id = record.getVerId();
		} else {
			id = -1;
		}
		return id;
	}
	
	/**
	 * Get an integer array indicating the min vertex ids 
	 * of each local VBlocks (i.e., buckets).
	 * 
	 * @return
	 */
	public int[] getLocBucMinIds() {
		int bucNum = this.verBlkMgr.getBlkNum();
		this.locMinVerIds = new int[bucNum];
		for (int i = 0; i < bucNum; i++) {
			this.locMinVerIds[i] = 
				this.verBlkMgr.getVerBlkBeta(i).getVerMinId();
		}
		
		return this.locMinVerIds;
	}
	
	/**
	 * Get the number of responding source vertices of each VBlock.  
	 * 
	 * @return
	 */
	public int[] getRespondVerNumOfBlks() {		
		int[] resVerNumBlks = new int[this.verBlkMgr.getBlkNum()];
		for (int i = 0; i < this.verBlkMgr.getBlkNum(); i++) {
			resVerNumBlks[i] = 
				this.verBlkMgr.getVerBlkBeta(i).getRespondVerNum();
		}
		return resVerNumBlks;
	}
	
	/**
	 * Get the user-defined byte size of one message.
	 * 
	 * @return int
	 */
	public int getByteOfOneMessage() {
		return this.userTool.getMsgRecord().getMsgByte();
	}
	
	/**
	 * Is the message value combined? 
	 * 1) True:   messages sent to the same target vertex 
	 *            will be combined into a single one;
	 * 2) False:  messages sent te same target vertex 
	 *            will be concatenated to share the target vertex id.
	 * Note: 
	 * 1) Combining/concatenating only works for b-pull.
	 * 2) Like Giraph/GPS, combining/cancatenating is disabled at the 
	 *    sender side for push. 
	 * 
	 * @return
	 */
	public boolean isAccumulated() {
		return this.userTool.isAccumulated();
	}
	
	/**
	 * Report the progress of loading data 
	 * by invoking getProgress() of RecordReader in HDFS.
	 * 
	 * @return float [0.0, 1.0]
	 * @throws Exception
	 */
	public float getProgress() throws Exception {
		return this.input.getProgress();
	}
	
	/**
	 * Return local io_bytes of updating vertex values.
	 * 
	 * @return
	 */
	public long getLocVerIOByte() {
		return this.io_byte_ver;
	}
	
	/**
	 * Return local io_bytes of reading graphInfo, used for Pull.
	 * @return
	 */
	public long getLocInfoIOByte() {
		return this.io_byte_info;
	}
	
	/**
	 * Return local io_bytes of reading edges in the adjacency list.
	 * 
	 * @return
	 */
	public long getLocAdjEdgeIOByte() {
		return this.io_byte_adj;
	}
	
	/**
	 * Return #edges in the adjacency list.
	 * 
	 * @return
	 */
	public long getLocReadAdjEdgeNum() {
		return this.read_adj_edge;
	}
	
	/**
	 * Estimate bytes of style.Push.
	 * Only edges in the adjacency list are considered, 
	 * since vertex id and value have been counted 
	 * and messages will be evaluated by {@link JobInProgress}.
	 * 
	 * @param iteNum
	 * @return
	 */
	public long getEstimatedPushBytes(int iteNum) {
		return this.estimatePushByte;
	}
	
	/**
	 * Estimate bytes of style.Pull. 
	 * I/O costs include bytes of graphInfo, edges in the adjacency list, 
	 * edges in fragments and the associated data. 
	 * Note that vertices have been accurately counted.
	 * 
	 * @param iteNum
	 * @return
	 * @throws Exception
	 */
	public long getEstimatedPullBytes(int iteNum) throws Exception {
		return this.estimatePullByte;
	}
	
	/**
	 * Only return the bytes of reading source vertices if pulling messages. 
	 * 
	 * @param iteNum
	 * @return
	 */
	public long getEstimatePullVertBytes(int iteNum) {
		return 0;
	}
	
	/** 
	 * Memory usage of the previous superstep.
	 * Note that the usage of superstep t is available in superstep (t+1).
	 * Thus, this function should be invoked by 
	 * {@link BSPTask}.beginIteration().
	 * 
	 * @return
	 */
	public long getAndClearMemUsage() {
		long size = 0; 
		if (this.isAccumulated) {
			for (long t: this.memUsedByMsgPull) {
				size += t;
			}
		}//otherwise, messages will be generated and sent using the ck mechanism.
		Arrays.fill(this.memUsedByMsgPull, 0);
		
		return (size+this.memUsedByMetaData);
	}
	
	/**
	 * Do some work when this bucket should be skipped.
	 * Including rename the local value file and setDefFlagBuc().
	 * 
	 * @param _parId
	 * @param _bid
	 * @param _iteNum
	 */
	public void skipBucket(int _parId, int _bid, int _iteNum) {
		setDefFlagBuc(_bid, _iteNum);
	}
	
	/**
	 * If this bucket will be skipped, it's related flag 
	 * must be set as default value.
	 * Note that the default value is false, for acFlag, upFlag and upFlagBuc.
	 * @param _iteNum
	 * @param _bid
	 */
	private void setDefFlagBuc(int _bid, int _iteNum) {
		int min = this.verBlkMgr.getVerBlkBeta(_bid).getVerMinId();
		int num = this.verBlkMgr.getVerBlkBeta(_bid).getVerNum();
		int type = (_iteNum+1)%2;
		int index = min - this.verBlkMgr.getVerMinId();
		Arrays.fill(this.actFlag, index, (index+num), false);
		Arrays.fill(this.resFlag[type], index, (index+num), false);
		this.verBlkMgr.setBlkRespond(type, _bid, false);
	}
	
	/**
	 * Initialize metadata used by in memory/disk server in 
	 * the {@link GraphDataServer} object 
	 * after that {@link CommRouteTable} has been initialized in 
	 * {@link BSPTask}.buildRouteTable() 
	 * and the number of Vblocks has been calculated/initialized 
	 * by {@link JobInProgress}/the user-specified parameter. 
	 * 
	 * @throws Exception
	 */
	public abstract void initMemOrDiskMetaData() throws Exception;
	
	/**
	 * Read data from HDFS, create {@link GraphRecord} and then save data 
	 * into the local task.
	 * The original {@link GraphRecord} will be decomposed by invoking 
	 * user-defined function 
	 * into vertex data and outgoing edge data. 
	 * 
	 * Note: 
	 * 1) for b-pull, vertex/edge data are organized in VBlocks/EBlocks;
	 * 2) for push, vertex data are also managed in VBlocks, 
	 *    but edge data are presented in the adjacency list. 
	 * 3) for hybrid, 
	 *    vertex data ---- VBlocks
	 *    edge data ---- EBlocks and adjacency list.
	 */
	public abstract void loadGraphData(TaskInformation taskInfo, 
			BytesWritable rawSplit, String rawSplitClass) throws Exception;
	
	/**
	 * Get {@link MsgRecord}s based on the outbound edges in local task.
	 * This function should be invoked by RPC to pull messages.
	 * @param _toTaskId id of task to which messages are sent
	 * @param _toBlkId local id of {@link VBlock} to which messages are sent 
	 * @param _iteNum
	 * @return
	 * @throws IOException
	 */
	public abstract MsgPack<V, W, M, I> getMsg(int _toTaskId, int _toBlkId, int _iteNum);
	
	/**
	 * Do preprocessing work before launching a new iteration, 
	 * such as deleting files out of date 
	 * and clearing counters in {@link VerBlockMgr}.
	 * Note that the latter can make sure the correct of 
	 * the function hasNextGraphRecord().
	 * 
	 * @param _iteNum
	 */
	public void clearBefIte(int _iteNum, int _preIteStyle, int _curIteStyle, 
			boolean estimate) throws Exception {
		this.preIteStyle = _preIteStyle;
		this.curIteStyle = _curIteStyle;
		this.estimatePullByteFlag = estimate;
		this.estimatePullByte = 0L;
		this.estimatePushByte = 0L;
		if (_curIteStyle == Constants.STYLE.Push) {
			if (_preIteStyle == Constants.STYLE.Pull) {
				//pre=pull&&cur=push.
				this.loadAdjEdge = this.job.isUseAdjEdgeInUpdate();
				this.loadGraphInfo = this.job.isUseGraphInfoInUpdate();
			} else {
				//pre=push&&cur=push.
				this.loadAdjEdge = true;
				this.loadGraphInfo = false;
			}
		} else if (_curIteStyle == Constants.STYLE.Pull) {
			//pre=pull&&cur=pull, or pre=push&&cur=pull.
			this.loadAdjEdge = this.job.isUseAdjEdgeInUpdate();
			this.loadGraphInfo = this.job.isUseGraphInfoInUpdate();
		} else {
			throw new Exception("invalid _curIteStyle=" 
					+ _curIteStyle + " at iteNum=" + _iteNum);
		}
		String pre = this.preIteStyle==Constants.STYLE.Pull? "pull":"push";
		String cur = this.curIteStyle==Constants.STYLE.Pull? "pull":"push";
		LOG.info("\nIteStyle=[pre:" + pre + " cur:" + cur 
				+ "], loadAdjEdge=" + this.loadAdjEdge 
				+ ", loadGraphInfo=" + this.loadGraphInfo);
		
		/** clear the message buffer used in pull @getMsg */
		if (this.bspStyle != Constants.STYLE.Push) {
			Arrays.fill(packageVersion, 0);
			Arrays.fill(proMsgOver, false);
			for (int i = 0; i < this.commRT.getTaskNum(); i++) {
				this.msgBuf[i].clear();
				this.msgBufLen[i].clear();
			}
		}
		
		this.verBlkMgr.clearBefIte(_iteNum);
		this.io_byte_ver = 0L;
		this.io_byte_info = 0L;
		this.io_byte_adj = 0L;
		this.read_adj_edge = 0L;
	}
	
	/** 
	 * :)
	 **/
	public abstract void clearBefIteMemOrDisk(int _iteNum) throws Exception;
	
	/**
	 * Do some work after the workload of one iteration on the local task 
	 * is completed. 
	 * 
	 * @param _iteNum
	 */
	public void clearAftIte(int _iteNum, int flagOpt) throws Exception {
		this.verBlkMgr.clearAftIte(_iteNum, flagOpt);
	}
	
	/** 
	 * Just used by {@link BSPTask.runInterationOnlyForPush()} 
	 * 
	 **/
	public void clearOnlyForPush(int _iteNum) {
		this.verBlkMgr.clearBefIte(_iteNum);
	}
	
	public void setUncompletedIteration(int location) {
		uncompletedIteration = location;
	}
	
	/**
	 * The returned value is not -1 only when: 
	 * (1) this is a surviving task, and 
	 * (2) {@link SuperStepCommand}.CommandType is RECOVER.
	 * @return
	 */
	public int getUncompletedIteration() {
		return uncompletedIteration;
	}
	
	/** 
	 * Only open vertex value file and adj edge file, read-only.
	 * Just used by {@link BSPTask}.runIterationOnlyForPush
	 * 
	 * */
	public abstract void openGraphDataStreamOnlyForPush(int _parId, int _bid, 
			int _iteNum) throws Exception;
	
	/** 
	 * Just used by {@link BSPTask}.runIterationOnlyForPush(). 
	 **/
	public abstract void closeGraphDataStreamOnlyForPush(int _parId, int _bid, 
			int _iteNum) throws Exception;
	
	/**
	 * Just used by {@link BSPTask}.runIterationOnlyForPush().
	 * @param _bid
	 * @return
	 * @throws Exception
	 */
	public abstract GraphRecord<V, W, M, I> getNextGraphRecordOnlyForPush(int _bid) 
			throws Exception;
	
	/**
	 * Initialize the file variables according to the bucketId.
	 * Before read vertexData, this function must be invoked.
	 * 
	 * @param String bucketDirName
	 * @param String bucketFileName
	 */
	public abstract void openGraphDataStream(int _parId, int _bid, int _iteNum) 
		throws Exception;
	
	/**
	 * Close the read stream.
	 * This function must be invoked after finishing reading.
	 */
	public abstract void closeGraphDataStream(int _parId, int _bid, int _iteNum) 
		throws Exception;
	
	/**
	 * If the next {@link GraphRecord} exists, return true, else return false.
	 * 
	 * @return
	 */
	public boolean hasNextGraphRecord(int _bid) {
		return this.verBlkMgr.getVerBlkBeta(_bid).hasNext();
	}
	
	/**
	 * Get the active status of the current vertex.
	 * 
	 * @param _vid
	 * @return
	 */
	public boolean isActive(int _vid) {
		int index = _vid - this.verBlkMgr.getVerMinId(); //global index
		return actFlag[index];
	}
	
	/**
	 * Get the next {@link GraphRecord} from the local disk.
	 * 
	 * @return
	 */
	public abstract GraphRecord<V, W, M, I> getNextGraphRecord(int _bid) 
			throws Exception;
	
	/**
	 * Write a {@link GraphRecord} onto the local disk 
	 * and update the corresponding flag.
	 * 
	 */
	public abstract void saveGraphRecord(int _bid, int _iteNum, 
			boolean _acFlag, boolean _upFlag) throws Exception;
	
	/** 
	 * Whether skip this bucket or not. If no vertices are updated, false, 
	 * otherwise true.
	 * Just used by {@link BSPTask.runSimIterationOnlyForPush()} 
	 * 
	 **/
	public boolean isDoOnlyForPush(int bid, int iteNum) {
		VerBlockBeta vHbb = this.verBlkMgr.getVerBlkBeta(bid);
		
		int type = iteNum % 2;
		return vHbb.isRespond(type);
	}
	
	/** 
	 * Is this vertex is updated?
	 * Just used by {@link BSPTask.runIterationOnlyForPush()} 
	 * 
	 **/
	public boolean isUpdatedOnlyForPush(int bid, int vid, int iteNum) {
		return resFlag[iteNum%2][vid-this.verBlkMgr.getVerMinId()];
	}
	
	/**
	 * Save all final results onto the distributed file system, 
	 * now the default is HDFS.
	 * Note that only vertex id and value of a {@link GraphRecord} is saved.
	 * 
	 * @param taskId
	 * @param _iteNum
	 * @return
	 * @throws Exception
	 */
	public abstract int saveAll(TaskAttemptID taskId, int _iteNum) 
			throws Exception;
	
	/**
	 * Archive checkpoint.
	 * 
	 * @param _version
	 * @param _iteNum
	 * @return Number of archived vertices
	 * @throws Exception
	 */
	public int archiveCheckPoint(int _version, int _iteNum) 
			throws Exception {
		return 0;
	}
	
	/**
	 * Load the most recent available checkpoint.
	 * Now it is implemented in {@link GraphDataServerDisk} only. 
	 * Thus, now HybridGraph can tolerate failures only for disk-based 
	 * computations. 
	 * @param iteNum restarting iteration counter
	 * @param ckpVersion the most recent available checkpoint version
	 * @return Number of vertices loaded from checkpoint 
	 * (0 if no checkpoint is available)
	 * @throws Exception
	 */
	public int loadCheckPoint(int iteNum, int ckpVersion) 
			throws Exception {
		return 0;
	}
	
	public void close() {
		
	}
}
