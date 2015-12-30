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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.monitor.TaskInformation;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.comm.MsgPack;
import org.apache.hama.myhama.io.OutputFormat;
import org.apache.hama.myhama.io.RecordWriter;
import org.apache.hama.myhama.util.GraphContext;

/**
 * GraphDataDisk manages graph data on local disks.
 * 
 * VBlocks: variables in one VBlock are stored in two files 
 * for values (file "value") and graphInfos (file "info"). 
 * File "vale" is read-write, but "info" is read-only.
 *              rootDir           (rootDir:jobId/taskId/graph)
 *                 |
 *               verDir           (verDir:rootDir/vertex)
 *            /    |    \
 *       lbd1     lbd2    lbd3    (lbdx:one VBlock dir x, name:locbuc-1)
 *      /  |  \    ...   /  |  \
 * value info [edge]              (file name: value, info, [edge])
 * In addition, if bspStyle={@link Constants}.STYLE.Push or Hybrid, 
 * edges should be stored in the adjacency list.   
 *    
 * 
 * EBlocks: edges linking to target vertices in one VBlock of a task 
 * are stored into one disk file.
 * In one disk file, edges of the same source vertices 
 * are clustered in a fragment.
 * Edge files are read-only.
 * The storage hierarchy is as follows:
 *             rootDir         (rootDir:jobId/taskId/graph)
 *                |
 *             edgeDir         (edgeDir:rootDir/edge)
 *            /   |   \
 *         td1   td2   td3     (td1:taskdir-1, name:task-1)
 *         / \   / \   / \
 *      bf1  bf2 ...  bf1 bf2  (bf1:bucketfile-1, name:buc-1)
 * 
 * The storage format of each bucket file is:
 * <code>source vertex id</code><code>#edges</code>
 * <code>the list of edge ids and weights</code>
 * For example, "2->{(3,0.2),(4,0.6)}" should be writen as "2230.240.6".
 * 
 * @author 
 * @version 0.1
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class GraphDataServerDisk<V, W, M, I> extends GraphDataServer<V, W, M, I> {
	private static final Log LOG = LogFactory.getLog(GraphDataServerDisk.class);
	
	public static int VERTEX_ID_BYTE = 4, Buf_Size = 5000;
	private static String Ver_Dir_Buc_Pre = "locbuc-";
	private static String Ver_File_Value = "value-";
	private static String Ver_File_Info = "info";
	private static String Ver_File_Adj = "edge_adj"; //edges in adjacency list used by Push.
	private static String Edge_Dir_Task_Pre = "task-"; //edges in fragments usecd by Pull
	private static String Edge_File_Buc_Pre = "buc-";
	
	/** directory and managers for graph data */
	private File verDir, edgeDir;
	
	/** buffer for loading graph data, will be cleared after loading graph */
	private GraphRecord<V, W, M, I>[] verBuf; //graph record
	private int verBufLen = 0;
	private long valBufByte = 0;
	private long infoBufByte = 0;
	private long adjBufByte = 0; //edges in the adjacency list
	private GraphRecord<V, W, M, I>[][][] edgeBuf; //[TaskId][BucId]:graph record
	private int[][] edgeBufLen;
	private long[][] edgeBufByte;
	private ExecutorService spillVerTh;
	private ExecutorService spillEdgeTh;
	private Future<Boolean> spillVerThRe;
	private Future<Boolean> spillEdgeThRe;
	private long loadByte = 0L;
	
	/** used to read or write graph data during iteration computation */
	private class VBlockFileHandler {
		private RandomAccessFile raf_v_r, raf_v_w, raf_info, raf_adj;
		private FileChannel fc_v_r, fc_v_w, fc_info, fc_adj;
		private MappedByteBuffer mbb_v_r, mbb_v_w, mbb_info, mbb_adj;
				
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
		
		public void openInfoReadHandler(File f_info) throws Exception {
			raf_info = new RandomAccessFile(f_info, "r");
			fc_info = raf_info.getChannel();
			mbb_info = fc_info.map(FileChannel.MapMode.READ_ONLY, 0L, 
					fc_info.size());
		}
		
		public MappedByteBuffer getInfoReadHandler() {
			return mbb_info;
		}
		
		public void closeInfoReadHandler() throws Exception {
			fc_info.close(); raf_info.close();
		}
		
		public void openAdjReadHandler(File f_adj) throws Exception {
			raf_adj = new RandomAccessFile(f_adj, "r");
			fc_adj = raf_adj.getChannel();
			mbb_adj = fc_adj.map(FileChannel.MapMode.READ_ONLY, 0L, 
					fc_adj.size());
		}
		
		public MappedByteBuffer getAdjReadHandler() {
			return mbb_adj;
		}
		
		public void closeAdjReadHandler() throws Exception {
			fc_adj.close(); raf_adj.close();
		}
	}
	private VBlockFileHandler vbFile;
	
	/** used to read VBlockFile and EBlockFile during responding pulling requests */
	private class VEBlockFileHandler<V, W, M, I> {
		private int tid, reqBid, resBid; //requested task_id, block_id; respond block_id
		private RandomAccessFile raf_e, raf_v;
		private FileChannel fc_e, fc_v;
		private MappedByteBuffer mbb_e, mbb_v;
		
		private boolean ck; //has been ck, true
		private int curLocVerId, counter;
		
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
		
		public void setCheckPoint(int _curLocVerId, 
				int _counter) {
			curLocVerId = _curLocVerId;
			counter = _counter;
			ck = true;
		}
		
		public boolean hasCheckPoint() {
			return ck;
		}
		
		public int getCurLocVerId() {
			return curLocVerId;
		}
		
		public int getCounter() {
			return counter;
		}
		
		public void resetCheckPoint() {
			curLocVerId = -1;
			counter = -1;
			ck = false;
		}
		
		public int getResBid() {
			return resBid<0? 0:resBid;
		}
	}
	private VEBlockFileHandler<V, W, M, I>[] vebFile; //per requested task
	private boolean[][] hitFlag; //used to estimate #fragments in pull when running push
	private long fragNumOfPull = 0L;
	
	/**
	 * Spill vertex data onto local disk.
	 * 
	 * @author 
	 * @version 0.1
	 */
	public class SpillVertexThread implements Callable<Boolean> {
		private int bid;
		private int writeLen;
		private GraphRecord<V, W, M, I>[] gBuf;
		private long writeValByte;
		private long writeInfoByte;
		private long writeAdjByte;
		
		public SpillVertexThread(int _bid, int _verBufLen, 
				GraphRecord<V, W, M, I>[] _verBuf, 
				long _valBufByte, long _infoBufByte, long _adjBufByte) {
			this.bid = _bid;
			this.writeLen = _verBufLen;
			this.gBuf = _verBuf;
			this.writeValByte = _valBufByte;
			this.writeInfoByte = _infoBufByte;
			this.writeAdjByte = _adjBufByte;
			loadByte += (_valBufByte + _infoBufByte + _adjBufByte);
		}
		
		@Override
		public Boolean call() throws IOException {
			File dir = getVerDir(this.bid);
			File f_v = new File(dir, Ver_File_Value + "1");
			RandomAccessFile raf_v = new RandomAccessFile(f_v, "rw");
			FileChannel fc_v = raf_v.getChannel();
			MappedByteBuffer mbb_v = 
				fc_v.map(FileChannel.MapMode.READ_WRITE, fc_v.size(), this.writeValByte);
			//graphInfo.
			File f_info;
			RandomAccessFile raf_info = null;
			FileChannel fc_info = null;
			MappedByteBuffer mbb_info = null;
			//edges in the adjacnecy list
			File f_adj;
			RandomAccessFile raf_adj = null;
			FileChannel fc_adj = null;
			MappedByteBuffer mbb_adj = null;
			
			boolean useGraphInfo = job.isUseGraphInfoInUpdate();
			boolean storeAdjEdge = job.isStoreAdjEdge();
			
			if (useGraphInfo) {
				f_info = new File(dir, Ver_File_Info);
				raf_info = new RandomAccessFile(f_info, "rw");
				fc_info = raf_info.getChannel();
				mbb_info = 
					fc_info.map(FileChannel.MapMode.READ_WRITE, 
							fc_info.size(), this.writeInfoByte);
			}
			
			if (storeAdjEdge) {
				f_adj = new File(dir, Ver_File_Adj);
				raf_adj = new RandomAccessFile(f_adj, "rw");
				fc_adj = raf_adj.getChannel();
				mbb_adj = 
					fc_adj.map(FileChannel.MapMode.READ_WRITE, 
							fc_adj.size(), this.writeAdjByte);
			}
			
			for (int i = 0; i < this.writeLen; i++) {
				this.gBuf[i].serVerValue(mbb_v);
				if (useGraphInfo) {
					this.gBuf[i].serGrapnInfo(mbb_info);
				}
				if (storeAdjEdge) {
					this.gBuf[i].serEdges(mbb_adj);
				}
			}
			
			fc_v.close();
			raf_v.close();
			if (useGraphInfo) {
				fc_info.close(); 
				raf_info.close();
			}
			if (storeAdjEdge) {
				fc_adj.close();
				raf_adj.close();
			}
			return true;
		}
	}
	
	/**
	 * Spill edge data in EBlock onto local disk.
	 * 
	 * @author 
	 * @version 0.1
	 */
	public class SpillEdgeThread implements Callable<Boolean> {
		private int tid;
		private int bid;
		private int writeLen;
		private long writeByte;
		private GraphRecord<V, W, M, I>[] gBuf;
		
		public SpillEdgeThread(int _tid, int _bid, 
				int _edgeBufLen, long _edgeBufByte, 
				GraphRecord<V, W, M, I>[] _edgeBuf) {
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
			
			for (int i = 0; i < this.writeLen; i++) {
				this.gBuf[i].serVerId(mbb);
				this.gBuf[i].serEdges(mbb);
			}
			
			fc.close(); raf.close();
			return true;
		}
	}
	
	/**
	 * Constructing the GraphDataServer object.
	 * 
	 * @param _parId
	 * @param _job
	 * @param _rootDir
	 */
	public GraphDataServerDisk(int _parId, BSPJob _job, String _rootDir) {
		super(_parId, _job);
		StringBuffer sb = new StringBuffer();
		sb.append("\n initialize graph data server in disk version;");
		sb.append("\n   storeAdjEdge="); sb.append(_job.isStoreAdjEdge());
		sb.append("\n   useAdjEdgeInUpdate="); sb.append(_job.isUseAdjEdgeInUpdate());
		sb.append("\n   useGraphInfoInUpdate="); sb.append(_job.isUseGraphInfoInUpdate());
		LOG.info(sb.toString());
	    createDir(_rootDir);
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
	
	@Override
	public void initMemOrDiskMetaData() throws Exception {
		vbFile = new VBlockFileHandler();
		verBuf = (GraphRecord<V, W, M, I>[]) new GraphRecord[Buf_Size];
		spillVerTh = Executors.newSingleThreadExecutor();
		spillVerThRe = null;
		
		int taskNum = this.commRT.getTaskNum();
		
		/** only used in pull or hybrid */
		if (this.bspStyle != Constants.STYLE.Push) {
			edgeBuf = 
				(GraphRecord<V, W, M, I>[][][]) new GraphRecord[taskNum][][];
			edgeBufLen = new int[taskNum][];
			edgeBufByte = new long[taskNum][];
			
			spillEdgeTh = Executors.newSingleThreadExecutor();
			spillEdgeThRe = null;
			
			this.hitFlag = new boolean[taskNum][];
			int[] bucNumTask = commRT.getJobInformation().getBlkNumOfTasks();
			for (int i = 0; i < taskNum; i++) {
				edgeBuf[i] = 
					(GraphRecord<V, W, M, I>[][]) new GraphRecord[bucNumTask[i]][];
				edgeBufLen[i] = new int[bucNumTask[i]];
				edgeBufByte[i] = new long[bucNumTask[i]];
				for (int j = 0; j < bucNumTask[i]; j++) {
					edgeBuf[i][j] = 
						(GraphRecord<V, W, M, I>[]) new GraphRecord[Buf_Size];
				}
				
				this.hitFlag[i] = new boolean[bucNumTask[i]];
			}
		
			this.vebFile = 
				(VEBlockFileHandler<V, W, M, I>[]) 
				new VEBlockFileHandler[taskNum];
			for (int i = 0; i < taskNum; i++) {
				this.vebFile[i] = new VEBlockFileHandler<V, W, M, I>(i);
			}
		}
	}
	
	private void putIntoEdgeBuf(ArrayList<EdgeFragmentEntry<V,W,M,I>> frags) 
			throws Exception {
		int vid = 0, tid = 0, bid = 0;
		for (EdgeFragmentEntry<V,W,M,I> frag: frags) {
			vid = frag.getVerId();
			tid = frag.getDstTid();
			bid = frag.getDstBid();
			GraphRecord<V,W,M,I> graph = this.userTool.getGraphRecord();
			graph.initialize(frag);
			
			edgeBuf[tid][bid][edgeBufLen[tid][bid]] = graph;
			edgeBufLen[tid][bid]++;
			edgeBufByte[tid][bid] += (VERTEX_ID_BYTE + graph.getEdgeByte()); 
			
			
			verBlkMgr.updateBlkFragmentLenAndNum(frag.getSrcBid(), 
					tid, bid, vid, VERTEX_ID_BYTE+graph.getEdgeByte());
			edgeBlkMgr.updateBucNum(tid, bid, 1, graph.getEdgeNum());
			for (int eid: graph.getEdgeIds()) {
				edgeBlkMgr.updateBucEdgeIdBound(tid, bid, eid);
			}
			
			if (edgeBufLen[tid][bid] >= Buf_Size) {
				if (this.spillEdgeThRe!=null 
						&& this.spillEdgeThRe.isDone()) {
					this.spillEdgeThRe.get();
				}
				
				this.spillEdgeThRe = 
					this.spillEdgeTh.submit(new SpillEdgeThread(
							tid, bid, edgeBufLen[tid][bid], 
							edgeBufByte[tid][bid], edgeBuf[tid][bid]));
				
				edgeBuf[tid][bid] = 
					(GraphRecord<V, W, M, I>[]) new GraphRecord[Buf_Size];
				edgeBufLen[tid][bid] = 0;
				edgeBufByte[tid][bid] = 0L;
			}
		}
	}
	
	private File getEdgeDir(int _tid) {
		File dir = 
			new File(this.edgeDir, Edge_Dir_Task_Pre+Integer.toString(_tid));
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
		int[] bucNumTask = commRT.getJobInformation().getBlkNumOfTasks();
		for (int i = 0; i < taskNum; i++) {
			for (int j = 0; j < bucNumTask[i]; j++) {
				if (edgeBufLen[i][j] > 0) {
					if (this.spillEdgeThRe!=null 
							&& this.spillEdgeThRe.isDone()) {
						this.spillEdgeThRe.get();
					}
					
					this.spillEdgeThRe = 
						this.spillEdgeTh.submit(new SpillEdgeThread(i, j, 
								edgeBufLen[i][j], edgeBufByte[i][j], edgeBuf[i][j]));
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
	
	private void putIntoVerBuf(GraphRecord<V, W, M, I> graph, int _bid) 
			throws Exception {
		verBuf[verBufLen] = graph;
		verBufLen++;
		valBufByte += graph.getVerByte();
		infoBufByte += graph.getGraphInfoByte();
		adjBufByte += graph.getEdgeByte();
		
		if (verBufLen >= Buf_Size) {
			if (this.spillVerThRe != null && this.spillVerThRe.isDone()) {
				this.spillVerThRe.get();
			}
			
			this.spillVerThRe = 
				this.spillVerTh.submit(
						new SpillVertexThread(_bid, verBufLen, verBuf, 
								valBufByte, infoBufByte, adjBufByte));
			
			verBuf = (GraphRecord<V, W, M, I>[]) new GraphRecord[Buf_Size];
			verBufLen = 0;
			valBufByte = 0;
			infoBufByte = 0;
			adjBufByte = 0;
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
								valBufByte, infoBufByte, adjBufByte));
			this.spillVerThRe.get();
		}
		verBuf = (GraphRecord<V, W, M, I>[]) new GraphRecord[Buf_Size];
		verBufLen = 0;
		valBufByte = 0;
		infoBufByte = 0;
		adjBufByte = 0;
	}
	
	private void clearVerBuf(int _bid) throws Exception {
		if (verBufLen > 0) {
			flushVerBuf(_bid);
		}
		
		verBuf = null;
		verBufLen = 0;
		valBufByte = 0;
		infoBufByte = 0;
		adjBufByte = 0;
		this.spillVerTh.shutdown();
		this.spillVerThRe = null;
	}
	
	@Override
	public void loadGraphData(TaskInformation taskInfo, BytesWritable rawSplit, 
			String rawSplitClass) throws Exception {
		long startTime = System.currentTimeMillis();
		long edgeNum = 0L;
		initInputSplit(rawSplit, rawSplitClass);
		
		int curBid = 0, bid = -1, vid = 0;
		while (input.nextKeyValue()) {
			GraphRecord<V, W, M, I> graph = this.userTool.getGraphRecord();
			graph.parseGraphData(input.getCurrentKey().toString(), 
					input.getCurrentValue().toString());
			edgeNum += graph.getEdgeNum();
			vid = graph.getVerId();
			curBid = commRT.getDstLocalBlkIdx(taskId, vid);
			bid = bid<0? curBid:bid;
			graph.setSrcBlkId(curBid);
			
			if (bid != curBid) {
				flushVerBuf(bid);
				bid = curBid;
			}
			putIntoVerBuf(graph, curBid);
			
			if (this.bspStyle != Constants.STYLE.Push) {
				ArrayList<EdgeFragmentEntry<V,W,M,I>> frags = 
					graph.decompose(commRT, taskInfo);
				putIntoEdgeBuf(frags);
			}
		}
		clearVerBuf(curBid);
		if (this.bspStyle != Constants.STYLE.Push) {
			clearEdgeBuf();
		}
		this.verBlkMgr.setEdgeNum(edgeNum);
		this.verBlkMgr.loadOver(this.bspStyle, this.commRT.getTaskNum(), 
				this.commRT.getJobInformation().getBlkNumOfTasks());
		
		
		int[] verNumBlks = new int[this.verBlkMgr.getBlkNum()];
		for (int i = 0; i < this.verBlkMgr.getBlkNum(); i++) {
			verNumBlks[i] = this.verBlkMgr.getVerBlkBeta(i).getVerNum();
		}
		int[] resVerNumBlks = new int[this.verBlkMgr.getBlkNum()];
		Arrays.fill(resVerNumBlks, 0);
		taskInfo.setVerNumBlks(verNumBlks);
		taskInfo.setRespondVerNumBlks(resVerNumBlks);
		taskInfo.setEdgeNum(edgeNum);
		taskInfo.setLoadByte(this.loadByte);
		this.memUsedByMetaData = this.verBlkMgr.getMemUsage();
		if (this.bspStyle != Constants.STYLE.Push) {
			this.memUsedByMetaData += this.edgeBlkMgr.getMemUsage();
		}
		
		long endTime = System.currentTimeMillis();
		LOG.info("load graph from HDFS, costTime=" 
				+ (endTime-startTime)/1000.0 + " seconds");
	}
	
	@Override
	public long getEstimatedPullBytes(int iteNum) throws Exception {
		int type = iteNum % 2; //not (iteNum+1)%2, should be equal with "type" in getMsg();
		long bytes = 0L;
		
		if (this.estimatePullByteFlag) {
			int[] blkNumOfTasks = this.commRT.getJobInformation().getBlkNumOfTasks();
			for (int dstTid = 0; dstTid < this.commRT.getTaskNum(); dstTid++) {
				for (int dstBid = 0; dstBid < blkNumOfTasks[dstTid]; dstBid++) {
					for (int srcBid = 0; 
							srcBid < this.verBlkMgr.getBlkNum(); srcBid++) {
						VerBlockBeta beta = this.verBlkMgr.getVerBlkBeta(srcBid);
						if (!beta.isRespond(type) ||
								beta.getFragmentNum(dstTid, dstBid)==0) {
							continue;
						}//skip
						bytes += beta.getFragmentLen(dstTid, dstBid);
					}
				}
			}//calculate bytes of fragments (svid, #edges, edges)
			
			//associated data, i.e., vertex values associated with fragments.
			bytes += this.fragNumOfPull * this.graph_rw.getVerByte();
			bytes += this.estimatePullByte; //actual info&adj-edge
		} 
		this.fragNumOfPull = 0L;
		
		return bytes;
	}
	
	@Override
	public MsgPack<V, W, M, I> getMsg(int _tid, int _bid, int _iteNum) 
			throws Exception {
		if (this.proMsgOver[_tid]) {
			MsgPack<V, W, M, I> msgPack = 
				new MsgPack<V, W, M, I>(userTool);
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
		
		int dstVerMinId = this.edgeBlkMgr.getBucEdgeMinId(_tid, _bid);
		int dstVerMaxId = this.edgeBlkMgr.getBucEdgeMaxId(_tid, _bid);
		int srcVerNum = this.edgeBlkMgr.getBucVerNum(_tid, _bid);
		int type = _iteNum % 2; //compute the type to read upFlag and upFlagBuc
		if (srcVerNum == 0) {
			return new MsgPack<V, W, M, I>(this.userTool); 
			//there is no edge target to _tid
		}
		/** create cache whose capacity = the number of destination vertices */
		MsgRecord<M>[] cache = 
			(MsgRecord<M>[]) new MsgRecord[dstVerMaxId-dstVerMinId+1];
		//io, edge_read, fragment_read, msg_pro, msg_rec, dstVerHasMsg.
		long[] statis = new long[6];
		int resBid = this.vebFile[_tid].getResBid();
		this.vebFile[_tid].openEdgeHandler(_bid);
		for (; resBid < this.verBlkMgr.getBlkNum(); resBid++) {
			VerBlockBeta vHbb = this.verBlkMgr.getVerBlkBeta(resBid);			
			if (!vHbb.isRespond(type) || (vHbb.getFragmentNum(_tid, _bid)==0)) {
				continue; //skip the whole hash bucket
			}
			
			this.vebFile[_tid].openVerHandler(resBid, _iteNum);
			//cache: pass-by-reference
			this.getMsgFromOneVBlock(statis, resBid, 
					this.vebFile[_tid].getVerHandler(), 
					this.vebFile[_tid].getEdgeHandler(), 
					type, _tid, _bid, _iteNum, cache, dstVerMinId);
			if (this.vebFile[_tid].hasCheckPoint()) {
				break;
			}
			this.vebFile[_tid].closeVerHandler();
		}
		if (!this.vebFile[_tid].hasCheckPoint()) {
			this.vebFile[_tid].closeEdgeHandler();
		}
		
		MsgPack<V, W, M, I> msgPack = packMsg(_tid, cache, statis);
		
		return msgPack;
	}
	
	private MsgPack<V, W, M, I> packMsg(int reqTid, MsgRecord<M>[] cache, long[] _statis) 
		throws Exception{
		MsgPack<V, W, M, I> msgPack = new MsgPack<V, W, M, I>(userTool); //message pack
		msgPack.setEdgeInfo(_statis[0], _statis[1], _statis[2]);
		long memUsage = 0L;
		
		if (_statis[5] > 0) {
			/** msg for local task, send all messages by one pack. */
			if (reqTid == this.taskId) {
				MsgRecord<M>[] tmp = 
					(MsgRecord<M>[]) new MsgRecord[(int)_statis[5]];
				int vCounter = 0;
				for (MsgRecord<M> msg: cache) {
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
				for (MsgRecord<M> msg: cache) {
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
							this.msgBufLen[reqTid].remove(0), _statis[3], _statis[5]); 
					//now, we use #dstVert as #recMsg
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
	 * @param resBid
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
	private void getMsgFromOneVBlock(long[] statis, int resBid, 
			MappedByteBuffer mbb_v, MappedByteBuffer mbb_e, 
			int type, int _tid, int _bid, int _iteNum, 
			MsgRecord<M>[] cache, int dstVerMinId) throws Exception {
		int curLocVerId = 0, counter = 0; 
		int skip = 0, curLocVerPos = 0;
		int verMinId = this.verBlkMgr.getVerMinId();
		GraphContext<V, W, M, I> context = 
			new GraphContext<V, W, M, I>(this.taskId, this.job, 
					_iteNum, this.preIteStyle);
		GraphRecord<V, W, M, I> graph = this.userTool.getGraphRecord();
		
		/** recover the scenario */
		if (this.vebFile[_tid].hasCheckPoint()) {
			curLocVerId = this.vebFile[_tid].getCurLocVerId();
			counter = this.vebFile[_tid].getCounter() + 1;
		}
		
		VerBlockBeta vBeta = this.verBlkMgr.getVerBlkBeta(resBid);		
		if (!this.vebFile[_tid].hasCheckPoint()) {
			mbb_e.position((int)vBeta.getFragmentStart(_tid, _bid)); //re-position
			curLocVerId = vBeta.getVerMinId();
			statis[0] += vBeta.getFragmentLen(_tid, _bid);
		} else {
			this.vebFile[_tid].resetCheckPoint();
		}
			
		int fragNum = vBeta.getFragmentNum(_tid, _bid);
		for (; counter < fragNum; counter++) {
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
				
			if (!resFlag[type][curLocVerId-verMinId]) {
				curLocVerPos = mbb_v.position();
				mbb_v.position(curLocVerPos + graph.getVerByte());
				curLocVerId++;
				continue; //skip if vertex isn't updated at the previous iteration
			} else {
				curLocVerId++;
			}
				
			graph.deserVerValue(mbb_v); //deserialize value
			statis[0] += graph.getVerByte(); //io for value.
			context.reset();
			context.initialize(graph, null, 0.0f, true);
			MsgRecord<M>[] msgs = this.bsp.getMessages(context);
			
			statis[3] += msgs.length; //msg_pro
			for (MsgRecord<M> msg: msgs) {
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
				this.vebFile[_tid].setCheckPoint(curLocVerId, counter);
				break;
			}
		}
	}
	
	@Override
	public void clearBefIteMemOrDisk(int _iteNum) {
		int type = _iteNum % 2;
		for (int bid = 0; bid < this.verBlkMgr.getBlkNum(); bid++) {
			File dir = getVerDir(bid);
			VerBlockBeta vHbb = this.verBlkMgr.getVerBlkBeta(bid);
			/** if it is updated, new file (_iteNum) should exist, 
			 * then the old one (_iteNum-1) is useless. 
			 * So, we delete it right now to save the disk space */
			if (vHbb.isRespond(type)) {
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
	
	@Override
	public void openGraphDataStreamOnlyForPush(int _parId, int _bid, int _iteNum) 
			throws Exception {
		File dir = getVerDir(_bid);
		File f_v_r = new File(dir, Ver_File_Value + _iteNum);
		this.vbFile.openVerReadHandler(f_v_r);
		
		File f_adj = new File(dir, Ver_File_Adj);
		this.vbFile.openAdjReadHandler(f_adj);
		this.io_byte_adj += f_adj.length();
	}
	
	@Override
	public void closeGraphDataStreamOnlyForPush(int _parId, int _bid, int _iteNum) 
			throws Exception {
		this.vbFile.closeVerReadHandler();
		this.vbFile.closeAdjReadHandler();
	}
	
	@Override
	public GraphRecord<V, W, M, I> getNextGraphRecordOnlyForPush(int _bid) 
			throws Exception {
		graph_rw.setVerId(this.verBlkMgr.getVerBlkBeta(_bid).getVerId());
		graph_rw.deserVerValue(this.vbFile.getVerReadHandler());
		io_byte_ver += (VERTEX_ID_BYTE + graph_rw.getVerByte());
		
		graph_rw.deserEdges(this.vbFile.getAdjReadHandler()); //read-only
		read_adj_edge += graph_rw.getEdgeNum(); 
		
		return graph_rw;
	}
	
	@Override
	public void openGraphDataStream(int _parId, int _bid, int _iteNum) 
			throws Exception {
		File dir = getVerDir(_bid);
		File f_v_r = new File(dir, Ver_File_Value + _iteNum);
		this.vbFile.openVerReadHandler(f_v_r);
		
		File f_info = new File(dir, Ver_File_Info);
		if (this.loadGraphInfo) {
			//curIteStyle=Pull and graphInfo is required
			this.vbFile.openInfoReadHandler(f_info);
			io_byte_info += f_info.length();
		}
		
		File f_adj = new File(dir, Ver_File_Adj);
		if (this.loadAdjEdge) {
			this.vbFile.openAdjReadHandler(f_adj);
			this.io_byte_adj += f_adj.length();
		}
		
		File f_v_w = new File(dir, Ver_File_Value + (_iteNum+1));
		this.vbFile.openVerWriteHandler(f_v_w);
		
		if (this.estimatePullByteFlag) {
			//curIteStyle=Push
			if (this.job.isUseGraphInfoInUpdate()) {
				//if graphInfo is required by Pull
				this.estimatePullByte += f_info.length();
			}
			if (this.job.isUseAdjEdgeInUpdate()) {
				//if adj is required by Pull
				this.estimatePullByte += f_adj.length();
			}
		} else {
			//curIteStyle=Pull, adj must be read by Push.
			this.estimatePushByte += f_adj.length();
		}
	}
	
	@Override
	public void closeGraphDataStream(int _parId, int _bid, int _iteNum) 
			throws Exception {
		this.vbFile.closeVerReadHandler();
		if (this.loadGraphInfo) {
			this.vbFile.closeInfoReadHandler();
		}
		if (this.loadAdjEdge) {
			this.vbFile.closeAdjReadHandler();
		}
		this.vbFile.closeVerWriteHandler();
	}
	
	@Override
	public GraphRecord<V, W, M, I> getNextGraphRecord(int _bid) throws Exception {
		graph_rw.setVerId(this.verBlkMgr.getVerBlkBeta(_bid).getVerId());
		graph_rw.deserVerValue(this.vbFile.getVerReadHandler());
		io_byte_ver += (VERTEX_ID_BYTE + graph_rw.getVerByte());
		
		if (this.loadGraphInfo) {
			graph_rw.deserGraphInfo(this.vbFile.getInfoReadHandler()); //read-only
		}
		if (this.loadAdjEdge) {
			graph_rw.deserEdges(this.vbFile.getAdjReadHandler()); //read-only
			read_adj_edge += graph_rw.getEdgeNum(); 
		}
		
		return graph_rw;
	}
	
	@Override
	public void saveGraphRecord(int _bid, int _iteNum, 
			boolean _acFlag, boolean _resFlag) throws Exception {
		int index = graph_rw.getVerId() - this.verBlkMgr.getVerMinId(); //global index
		int type = (_iteNum+1)%2;
		actFlag[index] = _acFlag;
		resFlag[type][index] = _resFlag;
		if (_resFlag) {
			this.verBlkMgr.setBlkRespond(type, _bid, _resFlag);
			this.verBlkMgr.incRespondVerNum(_bid);
			
			if (this.estimatePullByteFlag) {
				this.fragNumOfPull += graph_rw.getFragmentNum(commRT, hitFlag);
			}
		}
		
		graph_rw.serVerValue(this.vbFile.getVerWriteHandler());
		io_byte_ver += (graph_rw.getVerByte()); //only write value
	}
	
	@Override
	public int saveAll(TaskAttemptID taskId, int _iteNum) throws Exception {
		clearBefIteMemOrDisk(_iteNum);
		
		OutputFormat outputformat = 
        	(OutputFormat) ReflectionUtils.newInstance(job.getOutputFormatClass(), 
        		job.getConf());
        outputformat.initialize(job.getConf());
        RecordWriter output = outputformat.getRecordWriter(job, taskId);
        int saveNum = 0;
        
        for (int bid = 0; bid < this.verBlkMgr.getBlkNum(); bid++) {
        	File dir = getVerDir(bid);
    		File f_v_r = new File(dir, Ver_File_Value + _iteNum);
    		this.vbFile.openVerReadHandler(f_v_r);
    		
    		if (this.job.isUseGraphInfoInUpdate()) {
    			File f_info = new File(dir, Ver_File_Info);
        		this.vbFile.openInfoReadHandler(f_info);
    		}
    		if (this.job.isUseAdjEdgeInUpdate()) {
    			File f_adj = new File(dir, Ver_File_Adj);
    			this.vbFile.openAdjReadHandler(f_adj);
    		}
        	
    		int bucVerNum = this.verBlkMgr.getVerBlkBeta(bid).getVerNum();
    		int min = this.verBlkMgr.getVerBlkBeta(bid).getVerMinId();
            
    		for (int idx = 0; idx < bucVerNum; idx++) {
    			graph_rw.deserVerValue(this.vbFile.getVerReadHandler());
    			if (this.job.isUseGraphInfoInUpdate()) {
    				graph_rw.deserGraphInfo(this.vbFile.getInfoReadHandler());
    			}
    			if (this.job.isUseAdjEdgeInUpdate()) {
    				graph_rw.deserEdges(this.vbFile.getAdjReadHandler());
    			}
    			output.write(new Text(Integer.toString(min+idx)), 
    					new Text(graph_rw.getFinalValue().toString()));
    		}
    		this.vbFile.closeVerReadHandler();
    		if (this.job.isUseGraphInfoInUpdate()) {
    			this.vbFile.closeInfoReadHandler();
    		}
    		if (this.job.isUseAdjEdgeInUpdate()) {
    			this.vbFile.closeAdjReadHandler();
    		}
    		saveNum += bucVerNum;
        }
        
		output.close(job);
		return saveNum;
	}
}
