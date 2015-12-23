package org.apache.hama.myhama.graph;

import java.io.ByteArrayOutputStream;
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
import org.apache.hama.monitor.LocalStatistics;
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
	protected RecordReader<?,?> input; //load graph data
	protected UserTool<V, W, M, I> userTool;
	protected int parId;
	protected boolean isAccumulated;
	protected CommRouteTable<V, W, M, I> commRT;
	protected int taskNum;
	protected int verMinId, verMaxId, verNum;
	
	protected int bspStyle;
	protected int preIteStyle;
	protected int curIteStyle;
	protected boolean useGraphInfo;
	
	protected VerHashBucMgr verBucMgr; //local vertex hash bucket manager
	protected EdgeHashBucMgr edgeBucMgr; //local edge hash bucket manager
	protected boolean[] acFlag; //active-flag/vertex, single thread, true as default
	protected boolean[][] upFlag; //update-flag/vertex, two threads, false as default
	//private int bufSize; //final
	protected long[][] locMatrix; //collect edge info. for LocalStatistics
	
	protected GraphRecord<V, W, M, I> graph_rw;
	protected Long read_edge; //read edges, used in push, i.e. GraphInfo
	protected Long io_byte_ver; //io cost of updating vertex values
	protected Long io_byte_edge; //io cost of reading edges, used in style.Push
	protected int[] locMinVerIds;
	
	protected boolean estimatePullByte;
	protected long estimatePushByte = 0L;
	
	//message buffer for per target task.
	protected ArrayList<ByteArrayOutputStream>[] msgBuf;
	protected ArrayList<Integer>[] msgBufLen;
	protected boolean[] proMsgOver;
	/** memory usage */
	protected long memUsedByMetaData = 0L; //used when pulling messages
	protected long[] memUsedByMsgPull; //the maximal memory used when pulling messages
	
	@SuppressWarnings("unchecked")
	public GraphDataServer(int _parId, BSPJob _job) {
		parId = _parId;
		job = _job;
		this.useGraphInfo = _job.isUseGraphInfo();
		this.bspStyle = _job.getBspStyle();
		
		userTool = 
	    	(UserTool<V, W, M, I>) ReflectionUtils.newInstance(job.getConf().getClass(
	    			Constants.USER_JOB_TOOL_CLASS, UserTool.class), job.getConf());
	    this.isAccumulated = userTool.isAccumulated();
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
			(InputFormat) ReflectionUtils.newInstance(this.job.getConf().getClass(
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
	 * @param lStatis
	 * @param _commRT
	 * @throws Exception
	 */
	public void initialize(LocalStatistics lStatis, 
			final CommRouteTable<V, W, M, I> _commRT) throws Exception {
		commRT = _commRT;
		taskNum = commRT.getTaskNum();
		verMinId = lStatis.getVerMinId();
		verMaxId = lStatis.getVerMaxId();
		verNum = lStatis.getVerNum();
		locMinVerIds = new int[lStatis.getBucNum()];
		
		int[] bucNumTask = commRT.getGlobalSketchGraph().getBucNumTask();
		this.verBucMgr = new VerHashBucMgr(lStatis.getVerMinId(), lStatis.getVerMaxId(), 
				lStatis.getBucNum(), lStatis.getBucLen(), 
				taskNum, bucNumTask, this.userTool, this.bspStyle);
		
		int bucNumJob = commRT.getGlobalSketchGraph().getBucNumJob();
		this.locMatrix = new long[lStatis.getBucNum()][];
		for (int i = 0; i < lStatis.getBucNum(); i++) {
			this.locMatrix[i] = new long[bucNumJob];
		}
		
		acFlag = new boolean[verNum]; Arrays.fill(acFlag, true);
		upFlag = new boolean[2][];
		upFlag[0] = new boolean[verNum]; Arrays.fill(upFlag[0], false);
		upFlag[1] = new boolean[verNum]; Arrays.fill(upFlag[1], false);
		
		//this.bufSize = this.commRT.getGlobalStatis().getCachePerTask();
		this.io_byte_ver = 0L;
		this.io_byte_edge = 0L;
		this.read_edge = 0L;
		this.memUsedByMsgPull = new long[this.taskNum];
		
		/** only used in pull or hybrid */
		if (this.bspStyle != Constants.STYLE.Push) {
			this.edgeBucMgr = new EdgeHashBucMgr(taskNum, bucNumTask);
			this.proMsgOver = new boolean[this.taskNum];
			this.msgBufLen = new ArrayList[this.taskNum];
			this.msgBuf = new ArrayList[this.taskNum];
			for (int i = 0; i < this.taskNum; i++) {
				this.msgBufLen[i] = new ArrayList<Integer>();
				this.msgBuf[i] = new ArrayList<ByteArrayOutputStream>();
			}
		}
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
			record.initGraphData(input.getCurrentKey().toString(), 
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
		int bucNum = this.verBucMgr.getHashBucNum();
		this.locMinVerIds = new int[bucNum];
		for (int i = 0; i < bucNum; i++) {
			this.locMinVerIds[i] = this.verBucMgr.getVerHashBucBeta(i).getVerMinId();
		}
		
		return this.locMinVerIds;
	}
	
	/**
	 * Update local statistics information, 
	 * mainly including the number of active vertices  
	 * and the relationship among VBlocks.
	 * 
	 * @param lStatis
	 * @param iteNum
	 */
	public void updateLocalEdgeMatrix(LocalStatistics lStatis, int iteNum) {
		lStatis.updateLocalMatrix(this.locMatrix);
		
		int[] updVerNumBucs = new int[this.verBucMgr.getHashBucNum()];
		for (int i = 0; i < this.verBucMgr.getHashBucNum(); i++) {
			updVerNumBucs[i] = this.verBucMgr.getVerHashBucBeta(i).getUpdVerNum();
		}
		lStatis.setActVerNumBucs(updVerNumBucs);
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
	 * Return local io_bytes of reading edges, used in style.Push.
	 * 
	 * @return
	 */
	public long getLocEdgeIOByte() {
		return this.io_byte_edge;
	}
	
	/**
	 * Return edges accessed in stype.Push.
	 * 
	 * @return
	 */
	public long getLocReadEdgeNum() {
		return this.read_edge;
	}
	
	/**
	 * Estimate bytes of style.Push if pushing messages 
	 * from current active vertices (not updated). 
	 * Only considering bytes of graphInfo, since verValue has been counted.
	 * 
	 * @param iteNum
	 * @return
	 */
	public long getEstimatedPushBytes(int iteNum) {
		return this.estimatePushByte;
	}
	
	/**
	 * Estimate bytes of style.Pull if pulling messages from 
	 * source vertices updated in the previous iteration.
	 * 
	 * @param iteNum
	 * @return
	 * @throws Exception
	 */
	public long getEstimatedPullBytes(int iteNum) throws Exception {
		return 0L;
	}
	
	/** 
	 * Memory usage of current iteration is available in the next iteration.
	 * 
	 * @return
	 */
	public long getAndClearMemUsage() {
		long size = 0; 
		for (long t: this.memUsedByMsgPull) {
			size += t;
		}
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
		int min = this.verBucMgr.getVerHashBucBeta(_bid).getVerMinId();
		int num = this.verBucMgr.getVerHashBucBeta(_bid).getVerNum();
		int type = (_iteNum+1)%2;
		int index = min - this.verMinId;
		Arrays.fill(this.acFlag, index, (index+num), false);
		Arrays.fill(this.upFlag[type], index, (index+num), false);
		this.verBucMgr.setBucUpdated(type, _bid, false);
	}
	
	/**
	 * Initialize metadata used by in memory/disk server in 
	 * the {@link GraphDataServer} object 
	 * after that {@link CommRouteTable} has been initialized in 
	 * {@link BSPTask}.buildRouteTable() 
	 * and the number of Vblocks has been calculated/initialized 
	 * by {@link JobInProgress}/the user-specified parameter. 
	 * 
	 * @param lStatis
	 * @param _commRT
	 * @throws Exception
	 */
	public abstract void initMemOrDiskMetaData(LocalStatistics lStatis, 
			final CommRouteTable<V, W, M, I> _commRT) throws Exception;
	
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
	public abstract void loadGraphData(LocalStatistics lStatis, 
			BytesWritable rawSplit, String rawSplitClass) throws Exception;
	
	/**
	 * Get {@link MsgRecord}s based on the outbound edges in local task.
	 * This function should be invoked by RPC to pull messages.
	 * 
	 * @param _tid
	 * @param _bid
	 * @param _iteNum
	 * @return
	 * @throws Exception
	 */
	public abstract MsgPack<V, W, M, I> getMsg(int _tid, int _bid, int _iteNum) 
			throws Exception;
	
	/**
	 * Do preprocessing work before launching a new iteration, 
	 * such as deleting files out of date 
	 * and clearing counters in {@link VerHashBucMgr}.
	 * Note that the latter can make sure the correct of 
	 * the function hasNextGraphRecord().
	 * 
	 * @param _iteNum
	 */
	public void clearBefIte(int _iteNum, int _preIteStyle, int _curIteStyle, 
			boolean estimate) throws Exception {
		this.preIteStyle = _preIteStyle;
		this.curIteStyle = _curIteStyle;
		this.estimatePullByte = estimate;
		this.estimatePushByte = 0L;
		if (this.bspStyle == Constants.STYLE.Hybrid) {
			if (_curIteStyle == Constants.STYLE.Push) {
				this.useGraphInfo = true;
			} else if (_curIteStyle == Constants.STYLE.Pull) {
				this.useGraphInfo = false;
			} else {
				throw new Exception("invalid _curIteStyle=" 
						+ _curIteStyle + " at iteNum=" + _iteNum);
			}
		} //so, pagerank, cannot use Hybrid, since its Pull cannot use GraphInfo.
		LOG.info("IteStyle=[pre:" + this.preIteStyle 
				+ " cur:" + this.curIteStyle 
				+ "], useGraphInfo=" + this.useGraphInfo);
		
		/** clear the message buffer used in pull @getMsg */
		if (this.bspStyle != Constants.STYLE.Push) {
			for (int i = 0; i < this.taskNum; i++) {
				if (this.msgBuf[i].size() > 0) {
					this.msgBuf[i].clear();
					this.msgBufLen[i].clear();
				}
				this.proMsgOver[i] = false;
			}
		}
		
		this.verBucMgr.clearBefIte(_iteNum);
		this.io_byte_ver = 0L;
		this.io_byte_edge = 0L;
		this.read_edge = 0L;
		
		/** only used in evaluate the maximum allocated memory */
		for (int i = 0; i < this.verBucMgr.getHashBucNum(); i++) {
			Arrays.fill(this.locMatrix[i], 0);
		}
	}
	
	/** 
	 * :)
	 **/
	public abstract void clearBefIteMemOrDisk(int _iteNum);
	
	/**
	 * Do some work after the workload of one iteration on the local task 
	 * is completed. 
	 * 
	 * @param _iteNum
	 */
	public void clearAftIte(int _iteNum) {
		this.verBucMgr.clearAftIte(_iteNum);
	}
	
	/** 
	 * Just used by {@link BSPTask.runInterationOnlyForPush()} 
	 * 
	 **/
	public void clearOnlyForPush(int _iteNum) {
		this.verBucMgr.clearBefIte(_iteNum);
	}
	
	/** 
	 * Only open vertex value file and graphInfo file, read-only.
	 * Just used by {@link BSPTask.runIterationOnlyForPush()}
	 * 
	 * */
	public abstract void openGraphDataStreamOnlyForPush(int _parId, int _bid, 
			int _iteNum) throws Exception;
	
	/** 
	 * Just used by {@link BSPTask.runSimplePushIteration()} 
	 **/
	public abstract void closeGraphDataStreamOnlyForPush(int _parId, int _bid, 
			int _iteNum) throws Exception;
	
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
		return this.verBucMgr.getVerHashBucBeta(_bid).hasNext();
	}
	
	/**
	 * Get the active status of the current vertex.
	 * 
	 * @param _vid
	 * @return
	 */
	public boolean isActive(int _vid) {
		int index = _vid - this.verMinId; //global index
		return acFlag[index];
	}
	
	/**
	 * Get the next {@link GraphRecord} from the local disk.
	 * 
	 * @return
	 */
	public abstract GraphRecord<V, W, M, I> getNextGraphRecord(int _bid) throws Exception;
	
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
		VerHashBucBeta vHbb = this.verBucMgr.getVerHashBucBeta(bid);
		
		int type = iteNum % 2;
		return vHbb.isUpdated(type);
	}
	
	/** 
	 * Is this vertex is updated?
	 * Just used by {@link BSPTask.runIterationOnlyForPush()} 
	 * 
	 **/
	public boolean isUpdatedOnlyForPush(int bid, int vid, int iteNum) {
		int type = iteNum % 2;
		return upFlag[type][vid-this.verMinId];
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
	public abstract int saveAll(TaskAttemptID taskId, int _iteNum) throws Exception;
	
	public void close() {
		
	}
}
