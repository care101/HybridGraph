package org.apache.hama.myhama.graph;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
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
import org.apache.hama.myhama.io.OutputFormat;
import org.apache.hama.myhama.io.RecordReader;
import org.apache.hama.myhama.io.RecordWriter;

/**
 * GraphData is used for operating graph data.
 * 
 * Store vertex value and graphInfo of local vertices.
 * They are stored in two seperate files: value and info.
 * File "vale" is dynamically updated (rw), but "info" is static (only-read).
 *             rootDir         (rootDir:jobId/taskId/graph)
 *                |
 *              verDir         (verDir:jobId/taskId/graph/vertex)
 *            /   |   \
 *        lbd1  lbd2   lbd3    (lbd1:local bucket dir 1, name:locbuc-1)
 *        /  \   ...   /  \
 *    value info             (file name: value, info)
 * 
 * Store edges of graph data, they are static (only-read).
 * The store framework is as follows:
 *             rootDir         (rootDir:jobId/taskId/graph)
 *                |
 *             edgeDir         (edgeDir:jobId/taskId/graph/edge)
 *            /   |   \
 *         td1   td2   td3     (td1:taskdir-1, name:task-1)
 *         / \   / \   / \
 *      bf1  bf2 ...  bf1 bf2  (bf1:bucketfile-1, name:buc-1)
 * 
 * The store format of each bucket file is:
 * <code>source vertex id</code><code>#edges</code>
 * <code>the list of edge id and weight</code>
 * For example, "2->{(3,0.2),(4,0.6)}" should be writen as "2230.240.6".
 * 
 * @author 
 * @version 0.1
 */
public class GraphDataServer {
	private static final Log LOG = LogFactory.getLog(GraphDataServer.class);
	
	public static int VERTEX_ID_BYTE = 4, Buf_Size = 5000;
	private static String Ver_Dir_Buc_Pre = "locbuc-";
	private static String Ver_File_Value = "value-";
	private static String Ver_File_Info = "info";
	private static String Edge_Dir_Task_Pre = "task-";
	private static String Edge_File_Buc_Pre = "buc-";
	
	private RecordReader input; //load graph data
	private CommRouteTable commRT;
	private UserTool userTool;
	
	private BSPJob job;
	private int parId;
	private int taskNum, verMinId, verMaxId, verNum;
	
	/** directory and managers for graph data */
	private File verDir, edgeDir;
	private VerHashBucMgr verBucMgr; //local vertex hash bucket manager
	private EdgeHashBucMgr edgeBucMgr; //local edge hash bucket manager
	
	/** buffer for loading graph data, will be cleared after loading graph */
	private GraphRecord[] verBuf; //graph record
	private int verBufLen = 0;
	private long valBufByte = 0;
	private long infoBufByte = 0;
	private GraphRecord[][][] edgeBuf; //[TaskId][BucId]:graph record
	private int[][] edgeBufLen;
	private long[][] edgeBufByte;
	private ExecutorService spillVerTh;
	private ExecutorService spillEdgeTh;
	private Future<Boolean> spillVerThRe;
	private Future<Boolean> spillEdgeThRe;
	private long loadByte = 0L;
	
	/** used to read or write graph data during iteration computation */
	private class VBlockFileHandler {
		private RandomAccessFile raf_v_r, raf_info_r, raf_v_w;
		private FileChannel fc_v_r, fc_info_r, fc_v_w;
		private MappedByteBuffer mbb_v_r, mbb_info_r, mbb_v_w;
				
		public VBlockFileHandler() { }
		
		public void openVerReadHandler(File f_v_r) throws Exception {
			raf_v_r = new RandomAccessFile(f_v_r, "r");
			fc_v_r = raf_v_r.getChannel();
			mbb_v_r = fc_v_r.map(FileChannel.MapMode.READ_ONLY, 0L, 
					fc_v_r.size());
		}
		
		public MappedByteBuffer getVerReadHandler() {
			return mbb_v_r;
		}
		
		public void closeVerReadHandler() throws Exception {
			fc_v_r.close(); raf_v_r.close();
		}
		
		public void openInfoReadHandler(File f_info_r) throws Exception {
			raf_info_r = new RandomAccessFile(f_info_r, "r");
			fc_info_r = raf_info_r.getChannel();
			mbb_info_r = fc_info_r.map(FileChannel.MapMode.READ_ONLY, 0L, 
					fc_info_r.size());
		}
		
		public MappedByteBuffer getInfoReadHandler() {
			return mbb_info_r;
		}
		
		public void closeInfoReadHandler() throws Exception {
			fc_info_r.close(); raf_info_r.close();
		}
		
		public void openVerWriteHandler(File f_v_w) throws Exception {
			raf_v_w = new RandomAccessFile(f_v_w, "rw");
			fc_v_w = raf_v_w.getChannel();
			mbb_v_w = fc_v_w.map(FileChannel.MapMode.READ_WRITE, 0, 
					fc_v_r.size());
		}
		
		public MappedByteBuffer getVerWriteHandler() {
			return mbb_v_w;
		}
		
		public void closeVerWriteHandler() throws Exception {
			fc_v_w.close(); raf_v_w.close();
		}
	}
	private VBlockFileHandler vbFile;
	private GraphRecord graph_rw;
	private boolean[] acFlag; //active-flag/vertex, single thread, original value is true
	private boolean[][] upFlag; //update-flag/vertex, two threads, original value is false
	private Long io_byte_ver; //io cost for updating vertex values
	private Long io_byte_edge; //io cost for reading edges, used in style.Push
	private Long read_edge; //read edges, used in push, i.e. GraphInfo
	
	private int bufSize; //final
	private long[][] locMatrix; //collect edge info. for LocalStatistics
	
	/** memory usage */
	private long memUsedByMetaData = 0L; //used when pulling messages
	private long[] memUsedByMsgPull; //the maximal memory used when pulling messages
	
	/** used to read VBlockFile and EBlockFile during responding pulling requests */
	private class VEBlockFileHandler{
		private int tid, reqBid, resBid; //requested task_id, block_id; respond block_id
		private RandomAccessFile raf_e, raf_v;
		private FileChannel fc_e, fc_v;
		private MappedByteBuffer mbb_e, mbb_v;
		
		private boolean ck; //has been ck, true
		private int curLocVerId, resMiniBid, counter;
		
		public VEBlockFileHandler(int _tid) { 
			tid = _tid; 
			reqBid = -1;
			resBid = -1;
			resetCheckPoint();
		}
		
		private boolean hasOpenEdgeFile(int _bid) {
			if (_bid == reqBid) {
				return true;
			} else {
				reqBid = _bid;
				return false;
			}
		}
		
		private boolean hasOpenVerFile(int _bid) {
			if (_bid == resBid) {
				return true;
			} else {
				resBid = _bid;
				return false;
			}
		}
		
		public void openEdgeHandler(int _bid) throws Exception {
			if (hasOpenEdgeFile(_bid)) { return; }
			
			File file = getEdgeFile(getEdgeDir(tid), reqBid);
			raf_e = new RandomAccessFile(file, "r");
			fc_e = raf_e.getChannel();
			mbb_e = fc_e.map(FileChannel.MapMode.READ_ONLY, 0, fc_e.size());
		}
		
		public MappedByteBuffer getEdgeHandler() { return mbb_e; }
		
		public void closeEdgeHandler() throws Exception { 
			fc_e.close(); raf_e.close(); 
			reqBid = -1;
			resBid = -1;
			resetCheckPoint();
		}
		
		public void openVerHandler(int _bid, int _iteNum) throws Exception {
			if (hasOpenVerFile(_bid)) { return; }
			
			File file = new File(getVerDir(resBid), Ver_File_Value + _iteNum);
			raf_v = new RandomAccessFile(file, "r");
			fc_v = raf_v.getChannel();
			mbb_v = fc_v.map(FileChannel.MapMode.READ_ONLY, 0, fc_v.size());
		}
		
		public MappedByteBuffer getVerHandler() { return mbb_v; }
		
		public void closeVerHandler() throws Exception {
			fc_v.close(); raf_v.close();
		}
		
		public void setCheckPoint(int _resMiniBid, int _curLocVerId, 
				int _counter) {
			resMiniBid = _resMiniBid;
			curLocVerId = _curLocVerId;
			counter = _counter;
			ck = true;
		}
		
		public boolean hasCheckPoint() {
			return ck;
		}
		
		public int getResMiniBid() {
			return resMiniBid;
		}
		
		public int getCurLocVerId() {
			return curLocVerId;
		}
		
		public int getCounter() {
			return counter;
		}
		
		public void resetCheckPoint() {
			resMiniBid = -1;
			curLocVerId = -1;
			counter = -1;
			ck = false;
		}
		
		public int getResBid() {
			return resBid<0? 0:resBid;
		}
	}
	private VEBlockFileHandler[] vebFile; //per requested task
	private ArrayList<ByteArrayOutputStream>[] msgBuf; //message buffer for per target task.
	private ArrayList<Integer>[] msgBufLen;
	private boolean[] proMsgOver;
	private boolean isAccumulated;
	
	private boolean useGraphInfo;
	private int bspStyle;
	private int preIteStyle;
	private int curIteStyle;
	private boolean estimatePullByte;
	private boolean[][] hitFlag; //to compute bytes of style.Pull when running style.Push
	private long[] numOfReadVal; //to compute bytes of style.Pull when running style.Push
	private long estimatePushByte = 0L;
	
	private int[] locMinVerIds;
	
	/**
	 * Spill vertex data onto local disk.
	 * 
	 * @author 
	 * @version 0.1
	 */
	public class SpillVertexThread implements Callable<Boolean> {
		private int bid;
		private int writeLen;
		private GraphRecord[] gBuf;
		private long writeValByte;
		private long writeInfoByte;
		
		public SpillVertexThread(int _bid, int _verBufLen, GraphRecord[] _verBuf, 
				long _valBufByte, long _infoBufByte) {
			this.bid = _bid;
			this.writeLen = _verBufLen;
			this.gBuf = _verBuf;
			this.writeValByte = _valBufByte;
			this.writeInfoByte = _infoBufByte;
			loadByte += (_valBufByte + _infoBufByte);
		}
		
		@Override
		public Boolean call() throws IOException {
			File dir = getVerDir(this.bid);
			File f_v = new File(dir, Ver_File_Value + "1");
			RandomAccessFile raf_v = new RandomAccessFile(f_v, "rw");
			FileChannel fc_v = raf_v.getChannel();
			MappedByteBuffer mbb_v = 
				fc_v.map(FileChannel.MapMode.READ_WRITE, fc_v.size(), this.writeValByte);
			
			File f_info;
			RandomAccessFile raf_info = null;
			FileChannel fc_info = null;
			MappedByteBuffer mbb_info = null;
			
			if (useGraphInfo) {
				f_info = new File(dir, Ver_File_Info);
				raf_info = new RandomAccessFile(f_info, "rw");
				fc_info = raf_info.getChannel();
				mbb_info = 
					fc_info.map(FileChannel.MapMode.READ_WRITE, 
							fc_info.size(), this.writeInfoByte);
			}
			
			for (int i = 0; i < this.writeLen; i++) {
				this.gBuf[i].serVerValue(mbb_v);
				if (useGraphInfo) {
					this.gBuf[i].serGrapnInfo(mbb_info);
				}
			}
			
			fc_v.close();
			raf_v.close();
			if (useGraphInfo) {
				fc_info.close(); 
				raf_info.close();
			}
			return true;
		}
	}
	
	/**
	 * Spill edge data onto local disk.
	 * 
	 * @author 
	 * @version 0.1
	 */
	public class SpillEdgeThread implements Callable<Boolean> {
		private int tid;
		private int bid;
		private int writeLen;
		private long writeByte;
		private GraphRecord[] gBuf;
		
		public SpillEdgeThread(int _tid, int _bid, 
				int _edgeBufLen, long _edgeBufByte, GraphRecord[] _edgeBuf) {
			this.tid = _tid;
			this.bid = _bid;
			this.writeLen = _edgeBufLen;
			this.writeByte = _edgeBufByte;
			this.gBuf = _edgeBuf;
			loadByte += _edgeBufByte;
		}
		
		@Override
		public Boolean call() throws IOException {
			File dir = getEdgeDir(this.tid);
			File file = getEdgeFile(dir, this.bid);
			RandomAccessFile raf = new RandomAccessFile(file, "rw");
			FileChannel fc = raf.getChannel();
			MappedByteBuffer mbb = 
				fc.map(FileChannel.MapMode.READ_WRITE, fc.size(), this.writeByte);
			
			int vid = 0;
			for (int i = 0; i < this.writeLen; i++) {
				this.gBuf[i].serVerId(mbb);
				this.gBuf[i].serEdges(mbb);
				vid = this.gBuf[i].getVerId();
				verBucMgr.updateBucEVidEdgeLen(this.gBuf[i].getSrcBucId(), 
						this.tid, this.bid, 
						vid, VERTEX_ID_BYTE+this.gBuf[i].getEdgeByte());
				verBucMgr.updateBucEVerNum(this.gBuf[i].getSrcBucId(), 
						this.tid, this.bid, vid, 1);
				edgeBucMgr.updateBucNum(this.tid, this.bid, 1, this.gBuf[i].getEdgeNum());
				for (int eid: this.gBuf[i].getEdgeIds()) {
					edgeBucMgr.updateBucEdgeIdBound(this.tid, this.bid, eid);
				}
			}
			
			fc.close(); raf.close();
			return true;
		}
	}
	
	public GraphDataServer(int _parId, BSPJob _job, String _rootDir) {
		parId = _parId;
		job = _job;
		this.useGraphInfo = _job.isUseGraphInfo();
		LOG.info("set useGraphInfo=" + this.useGraphInfo + " when loading graph data");
		this.bspStyle = _job.getBspStyle();
		createDir(_rootDir);
		
		userTool = 
	    	(UserTool) ReflectionUtils.newInstance(job.getConf().getClass(
	    			Constants.USER_JOB_TOOL_CLASS, UserTool.class), job.getConf());
	    this.isAccumulated = userTool.isAccumulated();
	    graph_rw = userTool.getGraphRecord();
	}
	
	private void createDir(String _rootDir) {
		File rootDir = new File(_rootDir);
		if (!rootDir.exists()) {
			rootDir.mkdirs();
		}
		
		this.verDir = new File(rootDir, Constants.Graph_Ver_Dir);
		if(!this.verDir.exists()) {
			this.verDir.mkdirs();
		}
		
		this.edgeDir = new File(rootDir, Constants.Graph_Edge_Dir);
		if(!this.edgeDir.exists()) {
			this.edgeDir.mkdirs();
		}
	}
	
	/**
	 * Get the handler of reading raw graph data on HDFS.
	 * @param rawSplit
	 * @param rawSplitClass
	 * @return
	 * @throws Exception
	 */
	private void initInputSplit(BytesWritable rawSplit, 
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
	
	public void initialize(LocalStatistics lStatis, final CommRouteTable _commRT) 
		throws Exception {
		commRT = _commRT;
		taskNum = commRT.getTaskNum();
		verMinId = lStatis.getVerMinId();
		verMaxId = lStatis.getVerMaxId();
		verNum = lStatis.getVerNum();
		
		verBuf = new GraphRecord[Buf_Size];
		spillVerTh = Executors.newSingleThreadExecutor();
		spillVerThRe = null;
		
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
		this.vbFile = new VBlockFileHandler();
		
		this.bufSize = this.commRT.getGlobalStatis().getCachePerTask();
		
		this.io_byte_ver = 0L;
		this.io_byte_edge = 0L;
		this.read_edge = 0L;
		this.memUsedByMsgPull = new long[this.taskNum];
		
		this.hitFlag = new boolean[this.taskNum][this.commRT.getLocBucNum()];
		this.numOfReadVal = new long[]{0L, 0L};
		
		/** only used in pull or hybrid */
		if (this.bspStyle != Constants.STYLE.Push) {
			edgeBuf = new GraphRecord[taskNum][][];
			edgeBufLen = new int[taskNum][];
			edgeBufByte = new long[taskNum][];
			
			spillEdgeTh = Executors.newSingleThreadExecutor();
			spillEdgeThRe = null;
			
			this.edgeBucMgr = new EdgeHashBucMgr(taskNum, bucNumTask);
			for (int i = 0; i < taskNum; i++) {
				edgeBuf[i] = new GraphRecord[bucNumTask[i]][];
				edgeBufLen[i] = new int[bucNumTask[i]];
				edgeBufByte[i] = new long[bucNumTask[i]];
				for (int j = 0; j < bucNumTask[i]; j++) {
					edgeBuf[i][j] = new GraphRecord[Buf_Size];
				}
			}
		
			this.vebFile = new VEBlockFileHandler[this.taskNum];
			this.proMsgOver = new boolean[this.taskNum];
			this.msgBufLen = new ArrayList[this.taskNum];
			this.msgBuf = new ArrayList[this.taskNum];
			for (int i = 0; i < this.taskNum; i++) {
				this.msgBufLen[i] = new ArrayList<Integer>();
				this.msgBuf[i] = new ArrayList<ByteArrayOutputStream>();
				this.vebFile[i] = new VEBlockFileHandler(i);
			}
		}
	}
	
	/**
	 * Get the minimum vertex Id by reading the first graph record (adjacency list).
	 * After that, the {@link RecordReader} will be re-initialize by 
	 * invoking the function {@link initializeInputSplit}.
	 * @return
	 * @throws Exception 
	 */
	public int getVerMinId(BytesWritable rawSplit, 
			String rawSplitClass) throws Exception {
		initInputSplit(rawSplit, rawSplitClass);
		int id = 0;
		GraphRecord record = userTool.getGraphRecord();
		if(input.nextKeyValue()) {
			record.initGraphData(input.getCurrentKey().toString(), 
					input.getCurrentValue().toString());
			id = record.getVerId();
		} else {
			id = -1;
		}
		return id;
	}
	
	public int getByteOfOneMessage() {
		return this.userTool.getMsgRecord().getMsgByte();
	}
	
	public boolean isAccumulated() {
		return this.userTool.isAccumulated();
	}
	
	private void putIntoEdgeBuf(ArrayList<GraphRecord> graphs) throws Exception {
		int tid = 0, bid = 0;
		for (GraphRecord graph: graphs) {
			tid = graph.getDstParId();
			bid = graph.getDstBucId();
			edgeBuf[tid][bid][edgeBufLen[tid][bid]] = graph;
			edgeBufLen[tid][bid]++;
			edgeBufByte[tid][bid] += (VERTEX_ID_BYTE + graph.getEdgeByte()); 
			
			if (edgeBufLen[tid][bid] >= Buf_Size) {
				if (this.spillEdgeThRe != null && this.spillEdgeThRe.isDone()) {
					this.spillEdgeThRe.get();
				}
				
				this.spillEdgeThRe = 
					this.spillEdgeTh.submit(new SpillEdgeThread(
							tid, bid, edgeBufLen[tid][bid], 
							edgeBufByte[tid][bid], edgeBuf[tid][bid]));
				
				edgeBuf[tid][bid] = new GraphRecord[Buf_Size];
				edgeBufLen[tid][bid] = 0;
				edgeBufByte[tid][bid] = 0L;
			}
		}
	}
	
	private File getEdgeDir(int _tid) {
		File dir = new File(this.edgeDir, Edge_Dir_Task_Pre+Integer.toString(_tid));
		if (!dir.exists()) {
			dir.mkdirs();
		}
		return dir;
	}
	
	private File getEdgeFile(File dir, int _bid) {
		return new File(dir, Edge_File_Buc_Pre+Integer.toString(_bid));
	}
	
	private void clearEdgeBuf() throws Exception {
		int taskNum = commRT.getTaskNum();
		int[] bucNumTask = commRT.getGlobalSketchGraph().getBucNumTask();
		for (int i = 0; i < taskNum; i++) {
			for (int j = 0; j < bucNumTask[i]; j++) {
				if (edgeBufLen[i][j] > 0) {
					if (this.spillEdgeThRe != null && this.spillEdgeThRe.isDone()) {
						this.spillEdgeThRe.get();
					}
					
					this.spillEdgeThRe = 
						this.spillEdgeTh.submit(new SpillEdgeThread(
								i, j, edgeBufLen[i][j], edgeBufByte[i][j], edgeBuf[i][j]));
					this.spillEdgeThRe.get();
				}
			}
		}
		edgeBuf = null;
		edgeBufLen = null;
		edgeBufByte = null;
		this.spillEdgeTh.shutdown();
		this.spillEdgeThRe = null;
	}
	
	private void putIntoVerBuf(GraphRecord graph, int _bid) throws Exception {
		verBuf[verBufLen] = graph;
		verBufLen++;
		valBufByte += graph.getVerByte();
		infoBufByte += graph.getGraphInfoByte();
		
		if (verBufLen >= Buf_Size) {
			if (this.spillVerThRe != null && this.spillVerThRe.isDone()) {
				this.spillVerThRe.get();
			}
			
			this.spillVerThRe = 
				this.spillVerTh.submit(
						new SpillVertexThread(_bid, verBufLen, verBuf, 
								valBufByte, infoBufByte));
			
			verBuf = new GraphRecord[Buf_Size];
			verBufLen = 0;
			valBufByte = 0;
			infoBufByte = 0;
		}
	}
	
	private File getVerDir(int _bid) {
		File dir = new File(this.verDir, Ver_Dir_Buc_Pre+Integer.toString(_bid));
		if (!dir.exists()) {
			dir.mkdirs();
		}
		return dir;
	}
	
	private void flushVerBuf(int _bid) throws Exception {
		if (verBufLen > 0) {
			if (this.spillVerThRe != null && this.spillVerThRe.isDone()) {
				this.spillVerThRe.get();
			}
			
			this.spillVerThRe = 
				this.spillVerTh.submit(
						new SpillVertexThread(_bid, verBufLen, verBuf, 
								valBufByte, infoBufByte));
			this.spillVerThRe.get();
		}
		verBuf = new GraphRecord[Buf_Size];
		verBufLen = 0;
		valBufByte = 0;
		infoBufByte = 0;
	}
	
	private void clearVerBuf(int _bid) throws Exception {
		if (verBufLen > 0) {
			flushVerBuf(_bid);
		}
		
		verBuf = null;
		verBufLen = 0;
		valBufByte = 0;
		infoBufByte = 0;
		this.spillVerTh.shutdown();
		this.spillVerThRe = null;
	}
	
	/**
	 * Read data from HDFS, create the {@link GraphRecord} and then write it 
	 * onto the local disk in bytes.
	 * Original {@link GraphRecord} will be decomposed by invoking user-defined function 
	 * into vertex data and out bound edge data.
	 */
	public void loadGraphData(LocalStatistics lStatis, BytesWritable rawSplit, 
			String rawSplitClass) throws Exception {
		long startTime = System.currentTimeMillis();
		long edgeNum = 0L;
		initInputSplit(rawSplit, rawSplitClass);
		
		int curBid = 0, bid = -1, vid = 0;
		while (input.nextKeyValue()) {
			GraphRecord graph = this.userTool.getGraphRecord();
			graph.initGraphData(input.getCurrentKey().toString(), 
					input.getCurrentValue().toString());
			edgeNum += graph.getEdgeNum();
			vid = graph.getVerId();
			curBid = commRT.getDstBucId(parId, vid);
			bid = bid<0? curBid:bid;
			graph.setSrcBucId(curBid);
			this.verBucMgr.updateBucEdgeNum(curBid, vid, graph.getEdgeNum());
			this.verBucMgr.updateBucVerInfoLen(curBid, vid, 
					graph.getVerByte(), graph.getGraphInfoByte());
			
			if (bid != curBid) {
				flushVerBuf(bid);
				bid = curBid;
			}
			putIntoVerBuf(graph, curBid);
			
			if (this.bspStyle != Constants.STYLE.Push) {
				ArrayList<GraphRecord> graphs = graph.decompose(commRT, lStatis);
				putIntoEdgeBuf(graphs);
			}
		}
		clearVerBuf(curBid);
		if (this.bspStyle != Constants.STYLE.Push) {
			clearEdgeBuf();
		}
		this.verBucMgr.loadOver(this.bspStyle);
		
		
		int[] verNumBucs = new int[this.verBucMgr.getHashBucNum()];
		for (int i = 0; i < this.verBucMgr.getHashBucNum(); i++) {
			verNumBucs[i] = this.verBucMgr.getVerHashBucBeta(i).getVerNum();
		}
		int[] actVerNumBucs = new int[this.verBucMgr.getHashBucNum()];
		Arrays.fill(actVerNumBucs, 0);
		lStatis.setVerNumBucs(verNumBucs);
		lStatis.setActVerNumBucs(actVerNumBucs);
		lStatis.setEdgeNum(edgeNum);
		lStatis.setLoadByte(this.loadByte);
		this.memUsedByMetaData = this.verBucMgr.getMemUsage();
		if (this.bspStyle != Constants.STYLE.Push) {
			this.memUsedByMetaData += this.edgeBucMgr.getMemUsage();
		}
		
		long endTime = System.currentTimeMillis();
		LOG.info("load graph from HDFS, costTime=" 
				+ (endTime-startTime)/1000.0 + " seconds");
	}
	
	public int[] getLocBucMinIds() {
		int bucNum = this.verBucMgr.getHashBucNum();
		this.locMinVerIds = new int[bucNum];
		for (int i = 0; i < bucNum; i++) {
			this.locMinVerIds[i] = this.verBucMgr.getVerHashBucBeta(i).getVerMinId();
		}
		
		return this.locMinVerIds;
	}
	
	public float getProgress() throws Exception {
		return this.input.getProgress();
	}
	
	/**
	 * Estimate bytes of style.Push if pushing messages 
	 * from current active vertices (not updated). 
	 * Only considering bytes of graphInfo, since verValue has been counted.
	 * @param iteNum
	 * @return
	 */
	public long getEstimatedPushBytes(int iteNum) {
		return this.estimatePushByte;
	}
	
	/**
	 * Estimate bytes of style.Pull if pulling messages from 
	 * source vertices updated in the previous iteration.
	 * @param iteNum
	 * @return
	 * @throws Exception
	 */
	public long getEstimatedPullBytes(int iteNum) throws Exception {
		int type = iteNum % 2;
		long bytes = 0L;
		
		if (this.estimatePullByte) {
			int[] bucNumTask = this.verBucMgr.getBucNumTasks();
			for (int dstTid = 0; dstTid < this.taskNum; dstTid++) {
				for (int dstBid = 0; dstBid < bucNumTask[dstTid]; dstBid++) {
					for (int srcBid = 0; 
							srcBid < this.verBucMgr.getHashBucNum(); srcBid++) {
						VerHashBucBeta vHbb = this.verBucMgr.getVerHashBucBeta(srcBid);
						if (!vHbb.isUpdated(type) ||
								vHbb.getEVerNum(dstTid, dstBid)==0) {continue;} //skip
						bytes += vHbb.getEVidEdgeLen(dstTid, dstBid);
					}
				}
			}
			
			bytes += this.numOfReadVal[type] * this.graph_rw.getVerByte();
		} 
		
		this.numOfReadVal[type] = 0L;
		
		return bytes;
	}
	
	/**
	 * Get {@link MsgRecord} based on the outbound edges in local task.
	 * This function should be invoked by RPC to pull messages.
	 * @param _tid
	 * @param _bid
	 * @param _iteNum
	 * @return
	 * @throws Exception
	 */
	public MsgPack getMsg(int _tid, int _bid, int _iteNum) throws Exception {
		if (this.proMsgOver[_tid]) {
			MsgPack msgPack = new MsgPack(userTool); //message pack
			msgPack.setEdgeInfo(0L, 0L, 0L);
			
			if (this.msgBuf[_tid].size() > 0) {
				msgPack.setRemote(this.msgBuf[_tid].remove(0), 
						this.msgBufLen[_tid].remove(0), 0L, 0L);
			}
			
			if (this.msgBuf[_tid].size() == 0) {
				msgPack.setOver();
				this.proMsgOver[_tid] = false;
			}
			
			return msgPack;
		}
		
		int dstVerMinId = this.edgeBucMgr.getBucEdgeMinId(_tid, _bid);
		int dstVerMaxId = this.edgeBucMgr.getBucEdgeMaxId(_tid, _bid);
		int srcVerNum = this.edgeBucMgr.getBucVerNum(_tid, _bid);
		int type = _iteNum % 2; //compute the type to read upFlag and upFlagBuc
		if (srcVerNum == 0) {
			return new MsgPack(this.userTool); //there is no edge target to _tid
		}
		/** create cache whose capacity = the number of destination vertices */
		MsgRecord[] cache = new MsgRecord[dstVerMaxId-dstVerMinId+1];
		//io, edge_read, fragment_read, msg_pro, msg_rec, dstVerHasMsg.
		long[] statis = new long[6];
		int resBid = this.vebFile[_tid].getResBid();
		this.vebFile[_tid].openEdgeHandler(_bid);
		for (; resBid < this.verBucMgr.getHashBucNum(); resBid++) {
			VerHashBucBeta vHbb = this.verBucMgr.getVerHashBucBeta(resBid);
			VerMiniBucMgr vMbMgr = vHbb.getVerMiniBucMgr();
			
			if (!vHbb.isUpdated(type) || (vHbb.getEVerNum(_tid, _bid)==0)) {
				continue; //skip the whole hash bucket
			}
			
			this.vebFile[_tid].openVerHandler(resBid, _iteNum);
			//vMbMgr and cache: pass-by-reference
			this.getMsgMiniBucket(statis, vMbMgr, 
					this.vebFile[_tid].getVerHandler(), 
					this.vebFile[_tid].getEdgeHandler(), 
					type, _tid, _bid, cache, dstVerMinId);
			if (this.vebFile[_tid].hasCheckPoint()) {
				break;
			}
			this.vebFile[_tid].closeVerHandler();
		}
		if (!this.vebFile[_tid].hasCheckPoint()) {
			this.vebFile[_tid].closeEdgeHandler();
		}
		
		MsgPack msgPack = packMsg(_tid, cache, statis);
		
		return msgPack;
	}
	
	private MsgPack packMsg(int reqTid, MsgRecord[] cache, long[] _statis) 
		throws Exception{
		MsgPack msgPack = new MsgPack(userTool); //message pack
		msgPack.setEdgeInfo(_statis[0], _statis[1], _statis[2]);
		long memUsage = 0L;
		
		if (_statis[5] > 0) {
			/** msg for local task, send all messages by one pack. */
			if (reqTid == this.parId) {
				MsgRecord[] tmp = new MsgRecord[(int)_statis[5]];
				int vCounter = 0;
				for (MsgRecord msg: cache) {
					if (msg != null) {
						tmp[vCounter++] = msg;
						memUsage += msg.getMsgByte();
						msg = null;
					}
				}
				cache = null;
				msgPack.setLocal(tmp, vCounter, _statis[3], _statis[5]); //now, we use #dstVert as #recMsg
				if (!this.vebFile[reqTid].hasCheckPoint()) {
					this.proMsgOver[reqTid] = true;
					msgPack.setOver();
					this.proMsgOver[reqTid] = false; //pull local msgs only once!
				}
			} else {
				/** msg for remote task, send them by several packs. */
				int vCounter = 0, mCounter = 0, packSize = this.job.getMsgPackSize();
				if (this.vebFile[reqTid].hasCheckPoint()) {
					packSize = Integer.MAX_VALUE;
				}
				ByteArrayOutputStream bytes = 
					new ByteArrayOutputStream(this.job.getMsgPackSize());
				DataOutputStream stream = new DataOutputStream(bytes);
				for (MsgRecord msg: cache) {
					if (msg == null) continue;
					
					msg.serialize(stream);
					vCounter++;
					mCounter += msg.getNumOfMsgValues(); //mCounter >= vCounter
					msg = null;
					
					if (mCounter == packSize) {
						stream.close();	bytes.close();
						this.msgBuf[reqTid].add(bytes);
						this.msgBufLen[reqTid].add(vCounter);
						vCounter = 0; mCounter = 0;
						memUsage += stream.size();
						
						bytes = 
							new ByteArrayOutputStream(this.job.getMsgPackSize());
						stream = new DataOutputStream(bytes);
					} //pack
				} //loop all messages
				cache = null;
				
				if (vCounter > 0) {
					stream.close();
					bytes.close();
					this.msgBuf[reqTid].add(bytes);
					this.msgBufLen[reqTid].add(vCounter);
					memUsage += stream.size();
				}
				
				if (!this.vebFile[reqTid].hasCheckPoint()) {
					this.proMsgOver[reqTid] = true;
				}
				if (this.msgBuf[reqTid].size() > 0) {
					msgPack.setRemote(this.msgBuf[reqTid].remove(0), 
							this.msgBufLen[reqTid].remove(0), _statis[3], _statis[5]); //now, we use #dstVert as #recMsg
					if (this.msgBuf[reqTid].size()==0 
							&& !this.vebFile[reqTid].hasCheckPoint()) {
						msgPack.setOver();
						this.proMsgOver[reqTid] = false; //prepare for the next bucket
					}
				}
				
			}
		} else {
			msgPack.setOver();
		}
		
		this.memUsedByMsgPull[reqTid] = Math.max(this.memUsedByMsgPull[reqTid], memUsage);
		
		return msgPack;
	}
	
	/**
	 * Get {@link MsgRecord}s for each {@link VerMiniBucBeta} bucket.
	 * @param vMbMgr
	 * @param mbb_s
	 * @param mbb
	 * @param type
	 * @param _tid
	 * @param _bid
	 * @param cache
	 * @param dstVerMinId
	 * @return
	 * @throws Exception
	 */
	private void getMsgMiniBucket(long[] statis, VerMiniBucMgr vMbMgr, 
			MappedByteBuffer mbb_v, MappedByteBuffer mbb_e, 
			int type, int _tid, int _bid, 
			MsgRecord[] cache, 
			int dstVerMinId) throws Exception {
		int resMiniBid = 0, curLocVerId = 0, counter = 0; 
		int skip = 0, curLocVerPos = 0;
		int dstBucIndex = this.commRT.getGlobalSketchGraph().getGlobalBucIndex(_tid, _bid);
		GraphRecord graph = this.userTool.getGraphRecord();
		
		/** recover the scenario */
		if (this.vebFile[_tid].hasCheckPoint()) {
			resMiniBid = this.vebFile[_tid].getResMiniBid();
			curLocVerId = this.vebFile[_tid].getCurLocVerId();
			counter = this.vebFile[_tid].getCounter() + 1;
		}
		for (; resMiniBid < vMbMgr.getMiniBucNum(); resMiniBid++) {
			VerMiniBucBeta vMbb = vMbMgr.getVerMiniBucBeta(resMiniBid);
			if (!vMbb.isUpdated(type) || (vMbb.getEVerNum(_tid, _bid)==0)){
				continue; //skip this mini bucket
			}
			
			if (!this.vebFile[_tid].hasCheckPoint()) {
				mbb_v.position((int)vMbb.getVerStart());
				mbb_e.position((int)vMbb.getEVidEdgeStart(_tid, _bid)); //re-position
				curLocVerId = vMbb.getVerMinId();
				statis[0] += vMbb.getEVidEdgeLen(_tid, _bid);
			} else {
				this.vebFile[_tid].resetCheckPoint();
			}
			
			int i = 0;
			for (; counter < vMbb.getEVerNum(_tid, _bid); counter++) {
				skip = 0;
				graph.deserVerId(mbb_e); 
				graph.deserEdges(mbb_e); //deserialize edges 
				statis[1] += graph.getEdgeNum(); //edge_read
				statis[2]++; //fragment_read
				
				while((graph.getVerId()>curLocVerId)) {
					skip++; curLocVerId++; //find local vid based on srcVerId of edges
				}
				
				if (skip > 0) {//re-position of the local sendValue file
					curLocVerPos = mbb_v.position();
					mbb_v.position(curLocVerPos + skip*graph.getVerByte());
				}
				
				if (!upFlag[type][curLocVerId-this.verMinId]) {
					curLocVerPos = mbb_v.position();
					mbb_v.position(curLocVerPos + graph.getVerByte());
					curLocVerId++;
					continue; //skip if vertex isn't updated at the previous iteration
				} else {
					curLocVerId++;
				}
				
				graph.deserVerValue(mbb_v); //deserialize value
				statis[0] += graph.getVerByte(); //io for value.
				MsgRecord[] msgs = graph.getMsg(this.preIteStyle);				
				
				this.locMatrix[vMbMgr.getBucId()][dstBucIndex] += msgs.length;
				statis[3] += msgs.length; //msg_pro
				for (MsgRecord msg: msgs) {
					int index = msg.getDstVerId() - dstVerMinId;
					if (cache[index] == null) {
						cache[index] = msg;
						statis[4]++; //msg_rec
						statis[5]++; //dstVerHasMsg
					} else {
						cache[index].collect(msg);
						if (!this.isAccumulated) {
							statis[4]++; //msg_rec
						}
					}
				}
				
				if (!this.isAccumulated && statis[4]>this.job.getMsgPackSize()) {
					this.vebFile[_tid].setCheckPoint(resMiniBid, curLocVerId, counter);
					break;
				}
			}
			if (this.vebFile[_tid].hasCheckPoint()) {
				break;
			}
		}
	}
	
	public void updateLocalEdgeMatrix(LocalStatistics lStatis, int iteNum) {
		lStatis.updateLocalMatrix(this.locMatrix);
		
		int[] updVerNumBucs = new int[this.verBucMgr.getHashBucNum()];
		for (int i = 0; i < this.verBucMgr.getHashBucNum(); i++) {
			updVerNumBucs[i] = this.verBucMgr.getVerHashBucBeta(i).getUpdVerNum();
		}
		lStatis.setActVerNumBucs(updVerNumBucs);
	}
	
	/**
	 * Return io_bytes of data during loading subgraph onto local disk.
	 * @return
	 */
	private long getLoadByte() {
		return this.loadByte;
	}
	
	/**
	 * Return local io_bytes of updating vertex values.
	 * @return
	 */
	public long getLocVerIOByte() {
		return this.io_byte_ver;
	}
	
	/**
	 * Return local io_bytes of reading edges, used in style.Push.
	 * @return
	 */
	public long getLocEdgeIOByte() {
		return this.io_byte_edge;
	}
	
	/**
	 * Now, it is always zero.
	 * @return
	 */
	public long getLocReadEdgeNum() {
		return this.read_edge;
	}
	
	/** 
	 * Memory usage of current iteration is available in the next iteration.
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
	
	/** clear or rename value files based on the update_flag */
	private void switchAndClearGraphFile(int _iteNum) {
		int type = _iteNum % 2;
		for (int bid = 0; bid < this.verBucMgr.getHashBucNum(); bid++) {
			File dir = getVerDir(bid);
			VerHashBucBeta vHbb = this.verBucMgr.getVerHashBucBeta(bid);
			/** if it is updated, new file (_iteNum) should exist, 
			 * then the old one (_iteNum-1) is useless. 
			 * So, we delete it right now to save the disk space */
			if (vHbb.isUpdated(type)) {
				File f = new File(dir, Ver_File_Value + (_iteNum-1));
				f.delete();
			} else {
				/**
				 * if it is not updated in the previous iteration, 
				 * we should change its name from (_iteNum-1) to _iteNum.
				 * 
				 * this process must be done here, instead of @skipBucket, 
				 * otherwise, some @getMsg in (_iteNum-1) can not find 
				 * the related value-file
				 */
				File f_v_r = new File(dir, Ver_File_Value + (_iteNum-1));
				File f_v_w = new File(dir, Ver_File_Value + _iteNum);
				f_v_r.renameTo(f_v_w);
			}
		}
	}
	
	/**
	 * Do preprocess work before a new iteration computation.
	 * Now, it includes deleting files out of date 
	 * and clear counters in {@link VerHashBucMgr}.
	 * Note that the latter can make sure the correct of 
	 * the function hasNextGraphRecord().
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
		
		switchAndClearGraphFile(_iteNum);
		
		this.verBucMgr.clearBefIte(_iteNum);
		this.io_byte_ver = 0L;
		this.io_byte_edge = 0L;
		this.read_edge = 0L;
		
		for (int i = 0; i < this.taskNum; i++) {
			Arrays.fill(hitFlag[i], false);
		}
		
		/** only used in evaluate the maximum allocated memory */
		for (int i = 0; i < this.verBucMgr.getHashBucNum(); i++) {
			Arrays.fill(this.locMatrix[i], 0);
		}
	}
	
	public void clearAftIte(int _iteNum) {
		this.verBucMgr.clearAftIte(_iteNum);
	}
	
	public void incUpdVerNumBuc(int bid) {
		this.verBucMgr.incUpdVerNumBuc(bid);
	}
	
	/** Just used by {@link BSPTask.runInterationOnlyForPush()} */
	public void clearOnlyForPush(int _iteNum) {
		this.verBucMgr.clearBefIte(_iteNum);
	}
	
	/** 
	 * Whether skip this bucket or not. If no vertices are updated, false, 
	 * otherwise true.
	 * Just used by {@link BSPTask.runSimIterationOnlyForPush()} 
	 **/
	public boolean isDoOnlyForPush(int bid, int iteNum) {
		VerHashBucBeta vHbb = this.verBucMgr.getVerHashBucBeta(bid);
		VerMiniBucMgr vMbMgr = vHbb.getVerMiniBucMgr();
		
		int type = iteNum % 2;
		return vHbb.isUpdated(type);
	}
	
	/** 
	 * Is this vertex is updated?
	 * Just used by {@link BSPTask.runIterationOnlyForPush()} 
	 **/
	public boolean isUpdatedOnlyForPush(int bid, int vid, int iteNum) {
		int type = iteNum % 2;
		return upFlag[type][vid-this.verMinId];
	}
	
	/**
	 * Just used by verifying the correctness.
	 * @param bid
	 * @param iteNum
	 * @return
	 */
	public int getUpdatedNum(int bid, int iteNum) {
		int type = iteNum % 2;
		int begin = this.locMinVerIds[bid]-this.locMinVerIds[0];
		int end = 0;
		if ((bid+1) < this.locMinVerIds.length) {
			end = this.locMinVerIds[bid+1]-this.locMinVerIds[0]-1;
		} else {
			end = this.upFlag[type].length - 1;
		}
		
		int c = 0;
		
		for (int i = begin; i <= end; i++) {
			if (this.upFlag[type][i]) {c++;}
		}
		return c;
	}
	
	/** 
	 * Only open vertex value file and graphInfo file, read-only.
	 * Just used by {@link BSPTask.runIterationOnlyForPush()}
	 * */
	public void openGraphDataStreamOnlyForPush(int _parId, int _bid, int _iteNum) 
		throws Exception {
		File dir = getVerDir(_bid);
		File f_v_r = new File(dir, Ver_File_Value + _iteNum);
		this.vbFile.openVerReadHandler(f_v_r);
		
		if (this.useGraphInfo) {
			File f_info_r = new File(dir, Ver_File_Info);
			this.vbFile.openInfoReadHandler(f_info_r);
		}
	}
	
	/** Just used by {@link BSPTask.runSimplePushIteration()} */
	public void closeGraphDataStreamOnlyForPush(int _parId, int _bid, int _iteNum) 
		throws Exception {
		this.vbFile.closeVerReadHandler();
		if (useGraphInfo) {
			this.vbFile.closeInfoReadHandler();
		}
	}
	
	/**
	 * Initialize the file variables according to the bucketId.
	 * Before read vertexData, this function must be invoked.
	 * 
	 * @param String bucketDirName
	 * @param String bucketFileName
	 */
	public void openGraphDataStream(int _parId, int _bid, int _iteNum) throws Exception {
		File dir = getVerDir(_bid);
		File f_v_r = new File(dir, Ver_File_Value + _iteNum);
		this.vbFile.openVerReadHandler(f_v_r);
		
		if (this.useGraphInfo) {
			File f_info_r = new File(dir, Ver_File_Info);
			this.vbFile.openInfoReadHandler(f_info_r);
		} else {
			this.estimatePushByte += 
				this.verBucMgr.getVerHashBucBeta(_bid).getInfoLen();
		}
		
		File f_v_w = new File(dir, Ver_File_Value + (_iteNum+1));
		this.vbFile.openVerWriteHandler(f_v_w);
	}
	
	/**
	 * Close the read stream.
	 * This function must be invoked after finishing reading.
	 */
	public void closeGraphDataStream(int _parId, int _bid, int _iteNum) throws Exception {
		this.vbFile.closeVerReadHandler();
		if (useGraphInfo) {
			this.vbFile.closeInfoReadHandler();
		}
		
		this.vbFile.closeVerWriteHandler();
	}
	
	/**
	 * Do some work when this bucket should be skipped.
	 * Including rename the local value file and setDefFlagBuc().
	 * @param _parId
	 * @param _bid
	 * @param _iteNum
	 */
	public void skipBucket(int _parId, int _bid, int _iteNum) {
		setDefFlagBuc(_bid, _iteNum);
	}
	
	/**
	 * If this bucket will be skipped, it's related flag must be set as default value.
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
	 * If the next {@link GraphRecord} exists, return true, else return false.
	 * @return
	 */
	public boolean hasNextGraphRecord(int _bid) {
		return this.verBucMgr.getVerHashBucBeta(_bid).hasNext();
	}
	
	/**
	 * Get the next {@link GraphRecord} from the local disk.
	 * @return
	 */
	public GraphRecord getNextGraphRecord(int _bid) throws Exception {
		graph_rw.setVerId(this.verBucMgr.getVerHashBucBeta(_bid).getVerId());
		graph_rw.deserVerValue(this.vbFile.getVerReadHandler());
		io_byte_ver += (VERTEX_ID_BYTE + graph_rw.getVerByte());
		
		if (this.useGraphInfo) {
			graph_rw.deserGraphInfo(this.vbFile.getInfoReadHandler()); //read-only
			io_byte_edge += graph_rw.getGraphInfoByte();
			read_edge += graph_rw.getEdgeNum(); 
		}
		return graph_rw;
	}
	
	/**
	 * Write a {@link GraphRecord} onto the local disk 
	 * and update the corresponding flag.
	 */
	public void saveGraphRecord(int _bid, int _iteNum, 
			boolean _acFlag, boolean _upFlag) throws Exception {
		int index = graph_rw.getVerId() - this.verMinId; //global index
		int type = (_iteNum+1)%2;
		acFlag[index] = _acFlag;
		upFlag[type][index] = _upFlag;
		if (_upFlag) {
			this.verBucMgr.setBucUpdated(type, _bid, graph_rw.getVerId(), _upFlag);
			incUpdVerNumBuc(_bid);
			if (this.estimatePullByte) {
				this.numOfReadVal[type] += 
					this.graph_rw.getNumOfFragments(this.curIteStyle,
						this.commRT, this.hitFlag);
			}
		}
		
		graph_rw.serVerValue(this.vbFile.getVerWriteHandler());
		io_byte_ver += (graph_rw.getVerByte()); //only write value
	}
	
	/**
	 * Get the active status of the current vertex.
	 * @param _vid
	 * @return
	 */
	public boolean isActive(int _vid) {
		int index = _vid - this.verMinId; //global index
		return acFlag[index];
	}
	
	/**
	 * Save all final results onto the distributed file system, now the default is HDFS.
	 * Note that only vertex id and value of a {@link GraphRecord} is saved.
	 * @param taskId
	 * @param _iteNum
	 * @return
	 * @throws Exception
	 */
	public int saveAll(TaskAttemptID taskId, int _iteNum) throws Exception {
		switchAndClearGraphFile(_iteNum);
		
		OutputFormat outputformat = 
        	(OutputFormat) ReflectionUtils.newInstance(job.getOutputFormatClass(), 
        		job.getConf());
        outputformat.initialize(job.getConf());
        RecordWriter output = outputformat.getRecordWriter(job, taskId);
        int saveNum = 0;
        
        for (int bid = 0; bid < this.verBucMgr.getHashBucNum(); bid++) {
        	File dir = getVerDir(bid);
    		File f_v_r = new File(dir, Ver_File_Value + _iteNum);
    		this.vbFile.openVerReadHandler(f_v_r);
    		
    		if (this.useGraphInfo) {
    			File f_info_r = new File(dir, Ver_File_Info);
        		this.vbFile.openInfoReadHandler(f_info_r);
    		}
        	
    		int bucVerNum = this.verBucMgr.getVerHashBucBeta(bid).getVerNum();
    		int min = this.verBucMgr.getVerHashBucBeta(bid).getVerMinId();
            
    		for (int idx = 0; idx < bucVerNum; idx++) {
    			graph_rw.deserVerValue(this.vbFile.getVerReadHandler());
    			if (this.useGraphInfo) {
    				graph_rw.deserGraphInfo(this.vbFile.getInfoReadHandler());
    			}
    			output.write(new Text(Integer.toString(min+idx)), 
    					new Text(graph_rw.getFinalValue().toString()));
    		}
    		this.vbFile.closeVerReadHandler();
    		if (this.useGraphInfo) {
    			this.vbFile.closeInfoReadHandler();
    		}
    		saveNum += bucVerNum;
        }
        
		output.close(job);
		return saveNum;
	}
	
	public void close() {
		
	}
}
