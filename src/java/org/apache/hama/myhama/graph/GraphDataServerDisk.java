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
import org.apache.hama.myhama.io.RecordReader;
import org.apache.hama.myhama.io.RecordWriter;
import org.apache.hama.myhama.util.GraphContext;

/**
 * GraphDataServerDisk.java manages graph data on local disks.
 * 
 * ====================<<<<<<<<<< VBlocks >>>>>>>>>>====================
 * VBlocks: variables in one VBlock are stored in three separate files: 
 * values (file "value"), graph information (file "info"), and responding 
 * flags (file "res"). Among of the three read-only files, new "value" 
 * and "res" files will be created per superstep.
 * 
 *              rootDir            (rootDir:jobId/taskId/graph)
 *                 |
 *             vertexDir           (vertexDir:rootDir/vertex)
 *            /    |    \
 *     srcBktDir-1 ...  ...        (source VBlock dir x, name:srcbkt-1)
 *     /   |   |  \    
 * value info res [edge]           (file name: value, info, res, [edge])
 * 
 * In addition, if bspStyle={@link Constants}.STYLE.Push or Hybrid, 
 * edges should be stored in the adjacency list.
 * 
 * "res" is used by the fault-tolerance component (March, 2017).
 * Usually, the file is named after "res-x", where "x" is the superstep 
 * counter. In particular, if {@link resFlags} of all source vertices 
 * are marked as TRUE, a special suffix "all" is added, i.e., "res-x-all", 
 * and then a flag specific to each source vertex is ignored to reduce the 
 * disk storage space and hence I/O costs. In contrast, in the "all-false" 
 * case, the file will not be created. During failure recovery, if "res-x" 
 * does not exist, nothing is done in {@link getMsgFromOneVBlock}. 
 * Otherwise, {@link getMsgFromOneVBlock} returns messages by reading the 
 * "value-y" file with the smallest y that is greater than or equal to x 
 * in "res-x". 
 *  
 * 
 * ====================<<<<<<<<<< EBlocks >>>>>>>>>>====================
 * EBlocks: edges linking to target vertices in one VBlock on some task 
 * are stored into a single disk file. Further, in a disk file, edges 
 * with the same source vertices are clustered in a fragment.
 * All edge files are read-only.
 *             rootDir           (rootDir:jobId/taskId/graph)
 *                |
 *             edgeDir           (edgeDir:rootDir/edge)
 *            /   |   \
 *         td1   td2   td3       (td1:taskdir-1, name:task-1)
 *         / \   / \   / \
 *     tbf1  tbf2 ... tbf1 tbf2  (tbf1:targetbucketfile-1, name:tarbkt-1)
 * 
 * Data in each bucket file (tarbkt-x) is formatted as follows:
 * <code>source vertex id</code><code>#edges</code>
 * <code>the list of edge ids and weights</code>
 * For example, "2->{(3,0.2),(4,0.6)}" should be writen as "2230.240.6".
 * Note that "x" in "tarbkt-x" indicates the index of a target vertex bucket 
 * kept on some target task, instead of local source vertex bucket index. 
 * Further, for each "tarbkt-x" file, the offset of edges associated with a 
 * local source vertex bucket is kept in metadata. In this way, if source 
 * vertices in some local bucket do not need to respond pull requests, their 
 * outgoing edges in "tarbkt-x" can be skipped when generating messages. 
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
	private static String Vert_Dir_Bkt_Prefix = "srcbkt-"; //local source vert bucket
	private static String Vert_File_Value_Prefix = "value-";
	private static String Vert_File_Info = "info";
	private static String Vert_File_ActFlag_Prefix = "act-";
	private static String Vert_File_ResFlag_Prefix = "res-";
	private static String Vert_File_Flag_Suffix = "-alltrue";
	private static String Vert_File_Adj = "edge_adj"; //edges in adjacency list (Push)
	private static String Edge_Dir_Task_Prefix = "task-"; //edges in fragments (Pull)
	private static String Edge_File_Bkt_Prefix = "tarbkt-"; //target vert bucket
	
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
	
	private BitBytes bitBytes;
	private long rwResTime = 0L;
	
	/** used to read or write graph data during iteration computation */
	private class VBlockFileHandler {
		private RandomAccessFile raf_v_r, raf_v_w, raf_info, raf_adj;
		private FileChannel fc_v_r, fc_v_w, fc_info, fc_adj;
		private MappedByteBuffer mbb_v_r, mbb_v_w, mbb_info, mbb_adj;
				
		public VBlockFileHandler() { }
		
		public void clearBefIte() throws IOException {
			clear(fc_v_r, raf_v_r);
			clear(fc_v_w, raf_v_w);
			clear(fc_info, raf_info);
			clear(fc_adj, raf_adj);
		}
		
		public void clearAftIte() throws IOException {
		}
		
		private void clear(FileChannel fc, RandomAccessFile raf) 
				throws IOException {
			if (fc!=null && fc.isOpen()) {
				fc.close(); raf.close();
			}
		}
		
		public void openVerReadHandler(File f_v_r) throws IOException {
			raf_v_r = new RandomAccessFile(f_v_r, "r");
			fc_v_r = raf_v_r.getChannel();
			mbb_v_r = fc_v_r.map(FileChannel.MapMode.READ_ONLY, 0L, 
					fc_v_r.size());
		}
		
		public MappedByteBuffer getVerReadHandler() {
			return mbb_v_r;
		}
		
		public void closeVerReadHandler() throws IOException {
			fc_v_r.close(); raf_v_r.close();
		}
		
		public void openVerWriteHandler(File f_v_w) throws IOException {
			if (f_v_w.exists()) {
				f_v_w.delete();
			}
			raf_v_w = new RandomAccessFile(f_v_w, "rw");
			fc_v_w = raf_v_w.getChannel();
			mbb_v_w = fc_v_w.map(FileChannel.MapMode.READ_WRITE, 0, 
					fc_v_r.size());
		}
		
		public void openVerWriteHandler(File f_v_w, long bytes) throws IOException {
			raf_v_w = new RandomAccessFile(f_v_w, "rw");
			fc_v_w = raf_v_w.getChannel();
			mbb_v_w = fc_v_w.map(FileChannel.MapMode.READ_WRITE, 0, bytes);
		}
		
		public MappedByteBuffer getVerWriteHandler() {
			return mbb_v_w;
		}
		
		public void closeVerWriteHandler() throws IOException {
			fc_v_w.close(); raf_v_w.close();
		}
		
		public void openInfoReadHandler(File f_info) throws IOException {
			raf_info = new RandomAccessFile(f_info, "r");
			fc_info = raf_info.getChannel();
			mbb_info = fc_info.map(FileChannel.MapMode.READ_ONLY, 0L, 
					fc_info.size());
		}
		
		public MappedByteBuffer getInfoReadHandler() {
			return mbb_info;
		}
		
		public void closeInfoReadHandler() throws IOException {
			fc_info.close(); raf_info.close();
		}
		
		public void openAdjReadHandler(File f_adj) throws IOException {
			raf_adj = new RandomAccessFile(f_adj, "r");
			fc_adj = raf_adj.getChannel();
			mbb_adj = fc_adj.map(FileChannel.MapMode.READ_ONLY, 0L, 
					fc_adj.size());
		}
		
		public MappedByteBuffer getAdjReadHandler() {
			return mbb_adj;
		}
		
		public void closeAdjReadHandler() throws IOException {
			fc_adj.close(); raf_adj.close();
		}
	}
	private VBlockFileHandler vbFile;
	
	/** 
	 * Used to read VBlockFile and EBlockFile when responding pull requests.
	 * */
	private class VEBlockFileHandler<V, W, M, I> {
		//requested task_id and block_id; respond block_id
		private int tid, reqBid, resBid; 
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
		
		public void clearBefIte() throws IOException {
			//channel can only be safely closed before an iteration
			clear(fc_e, raf_e);
			clear(fc_v, raf_v);
			reqBid = -1;
			resBid = -1;
			resetCheckPoint();
		}
		
		public void clearAftIte() throws IOException {
			//do not close any file channel because the channel may be 
			//used by other tasks.
		}
		
		private void clear(FileChannel fc, RandomAccessFile raf) 
				throws IOException {
			if (fc!=null && fc.isOpen()) {
				fc.close(); raf.close();
			}
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
		
		public void openEdgeHandler(int _bid) throws IOException {
			if (hasOpenEdgeFile(_bid)) { return; }
			
			File file = getEdgeFile(getEdgeDir(tid), reqBid);
			raf_e = new RandomAccessFile(file, "r");
			fc_e = raf_e.getChannel();
			mbb_e = fc_e.map(FileChannel.MapMode.READ_ONLY, 0, fc_e.size());
		}
		
		public MappedByteBuffer getEdgeHandler() { return mbb_e; }
		
		public void closeEdgeHandler() throws IOException { 
			fc_e.close(); raf_e.close(); 
			reqBid = -1;
			resBid = -1;
			resetCheckPoint();
		}
		
		/**
		 * Open a channel to read the given vertex value file.
		 * @param _bid
		 * @param _iteNum
		 * @return true if successful, false otherwise (file does not exist)
		 * @throws IOException
		 */
		public boolean openVerHandler(int _bid, int _iteNum) throws IOException {
			File file = 
				new File(getVerDir(_bid), Vert_File_Value_Prefix + _iteNum);
			//LOG.warn(file);
			if (!file.exists()) {
				return false;
			}
			
			if (hasOpenVerFile(_bid)) { return true; }
			
			file = 
				new File(getVerDir(resBid), Vert_File_Value_Prefix + _iteNum);
			raf_v = new RandomAccessFile(file, "r");
			fc_v = raf_v.getChannel();
			mbb_v = fc_v.map(FileChannel.MapMode.READ_ONLY, 0, fc_v.size());
			return true;
		}
		
		public MappedByteBuffer getVerHandler() { return mbb_v; }
		
		public void closeVerHandler() throws IOException {
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
	private VEBlockFileHandler<V, W, M, I>[] vebFile;//per requested task
	/** used to estimate #fragments in pull when running push */
	private boolean[][] hitFlag; 
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
			File f_v = new File(dir, Vert_File_Value_Prefix + "1");
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
				f_info = new File(dir, Vert_File_Info);
				raf_info = new RandomAccessFile(f_info, "rw");
				fc_info = raf_info.getChannel();
				mbb_info = 
					fc_info.map(FileChannel.MapMode.READ_WRITE, 
							fc_info.size(), this.writeInfoByte);
			}
			
			if (storeAdjEdge) {
				f_adj = new File(dir, Vert_File_Adj);
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
		sb.append("\ninitialize graph data server (disk version)");
		sb.append("\n  1) store edges in the adjacency list: "); 
		sb.append(_job.isStoreAdjEdge());
		sb.append("\n  2) use adjacency-based edges in update(): "); 
		sb.append(_job.isUseAdjEdgeInUpdate());
		sb.append("\n  3) use graph information in update(): "); 
		sb.append(_job.isUseGraphInfoInUpdate());
		LOG.info(sb.toString());
	    createDir(_rootDir);
	    
	    bitBytes = new BitBytes();
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
			new File(this.edgeDir, Edge_Dir_Task_Prefix+Integer.toString(_tid));
		if (!dir.exists()) {
			dir.mkdirs();
		}
		return dir;
	}
	
	private File getEdgeFile(File dir, int _bid) {
		return new File(dir, Edge_File_Bkt_Prefix+Integer.toString(_bid));
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
								edgeBufLen[i][j], edgeBufByte[i][j], 
								edgeBuf[i][j]));
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
		File dir = 
			new File(this.verDir, Vert_Dir_Bkt_Prefix+Integer.toString(_bid));
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
		LOG.info("load graph, " 
				+ (endTime-startTime)/1000.0 + " seconds");
	}
	
	@Override
	public long getEstimatedPullBytes(int iteNum) throws Exception {
		//not (iteNum+1)%2, should be equal with "type" in getMsg();
		int type = iteNum % 2; 
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
		
		return bytes;
	}
	
	public long getEstimatePullVertBytes(int iteNum) {
		long bytes = this.fragNumOfPull * this.graph_rw.getVerByte();
		this.fragNumOfPull = 0L;
		return bytes;
	}
	
	@Override
	public MsgPack<V, W, M, I> getMsg(int _toTaskId, int _toBlkId, int _iteNum) {
		if (this.proMsgOver[_toTaskId]) {
			MsgPack<V, W, M, I> msgPack = 
				new MsgPack<V, W, M, I>(userTool);
			msgPack.setEdgeInfo(0L, 0L, 0L, 0L);
			
			if (this.msgBuf[_toTaskId].size() > 0) {
				msgPack.setRemote(this.msgBuf[_toTaskId].remove(0), 
						this.msgBufLen[_toTaskId].remove(0), 0L, 0L, 0L);
			}
			
			if (this.msgBuf[_toTaskId].size() == 0) {
				msgPack.setOver();
				this.proMsgOver[_toTaskId] = false;
			}
			
			return msgPack;
		}
		
		try {
			if ((job.getCheckPointPolicy()
					==Constants.CheckPoint.Policy.ConfinedRecoveryLogMsg) 
					&& (getUncompletedIteration()!=-1)) {
				return packMsg(_toTaskId, _toBlkId);
			}
			
			int toVerMinId = this.edgeBlkMgr.getBucEdgeMinId(_toTaskId, _toBlkId);
			int toVerMaxId = this.edgeBlkMgr.getBucEdgeMaxId(_toTaskId, _toBlkId);
			int fromVerNum = this.edgeBlkMgr.getBucVerNum(_toTaskId, _toBlkId);
			int type = _iteNum % 2; //compute the type to read upFlag and upFlagBuc
			if (fromVerNum == 0) {
				MsgPack pack = new MsgPack<V, W, M, I>(this.userTool); 
				pack.setOver();
				return pack;
				//there is no edge target to _tid
			}
			//io, edge_read, fragment_read, msg_pro, msg_rec, dstVerHasMsg, io_vert.
			long[] statis = new long[7];
			/** create cache whose capacity = the number of destination vertices */
			MsgRecord<M>[] cache = 
				(MsgRecord<M>[]) new MsgRecord[toVerMaxId-toVerMinId+1];
			int fromBlkId = this.vebFile[_toTaskId].getResBid();
			this.vebFile[_toTaskId].openEdgeHandler(_toBlkId);
			for (; fromBlkId < this.verBlkMgr.getBlkNum(); fromBlkId++) {
				VerBlockBeta vHbb = this.verBlkMgr.getVerBlkBeta(fromBlkId);
				if (!vHbb.isRespond(type) 
						|| (vHbb.getFragmentNum(_toTaskId, _toBlkId)==0)) {
					continue; //skip the whole hash bucket
				}
				
				int attemptedIteNum = _iteNum;
				while (!this.vebFile[_toTaskId].openVerHandler(fromBlkId, attemptedIteNum)) {
					attemptedIteNum++;
					//LOG.warn("iteNum=" + _iteNum + ", attemptedIteNum=" + attemptedIteNum);
					if (attemptedIteNum > getUncompletedIteration()) {
						File f = 
							new File(getVerDir(fromBlkId), Vert_File_Value_Prefix + _iteNum);
						throw new Exception("Fail to find file:\n" + f.toString() 
								+ ", attemptedIteNum=" + attemptedIteNum);
					}
				}
				
				//cache: pass-by-reference
				this.getMsgFromOneVBlock(statis, fromBlkId, 
						this.vebFile[_toTaskId].getVerHandler(), 
						this.vebFile[_toTaskId].getEdgeHandler(), 
						type, _toTaskId, _toBlkId, _iteNum, cache, toVerMinId);
				if (this.vebFile[_toTaskId].hasCheckPoint()) {
					break;
				}
				this.vebFile[_toTaskId].closeVerHandler();
			}
			if (!this.vebFile[_toTaskId].hasCheckPoint()) {
				this.vebFile[_toTaskId].closeEdgeHandler();
			}
			
			return packMsg(_toTaskId, _toBlkId, cache, statis);
		} catch (Exception e) {
			LOG.error("getMsg", e);
			return null;
		}
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
			MsgRecord<M>[] cache, int dstVerMinId) throws IOException {
		int curLocVerId = 0, counter = 0; 
		int skip = 0, curLocVerPos = 0;
		int verMinId = this.verBlkMgr.getVerMinId();
		GraphContext<V, W, M, I> context = 
			new GraphContext<V, W, M, I>(this.taskId, this.job, 
					_iteNum, this.preIteStyle, this.commRT);
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
			statis[0] += graph.getVerByte(); //io for value
			statis[6] += graph.getVerByte(); 
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
	
	/**
	 * For confined recovery with logged messages, directly read messages.
	 * @param toTaskId
	 * @param toBlkId
	 * @return
	 * @throws IOException
	 */
	private MsgPack<V, W, M, I> packMsg(int toTaskId, int toBlkId) 
			throws IOException {
		//load logged outgoing messages
		MsgPack<V, W, M, I> msgPack = new MsgPack<V, W, M, I>(userTool);
		//io, edge_read, fragment_read, msg_pro, msg_rec, dstVerHasMsg, io_vert.
		long[] statis = new long[7];
		
		int toGlobalBlkIdx = 
			this.commRT.getJobInformation().getGlobalBlkIdx(toTaskId, toBlkId);
		int numOfLoggedPacks = msgDataServer.getNumberOfMsgPacks(toGlobalBlkIdx);
		if (numOfLoggedPacks == 0) {
			msgPack.setOver();
		} else {
			for (int version = 0; version < numOfLoggedPacks; version++) {
				ByteArrayOutputStream messages = 
					new ByteArrayOutputStream(this.job.getMsgPackSize());
				int vCounter = 
					msgDataServer.loadOutgoingMsg(messages, toGlobalBlkIdx, version, statis);
				this.msgBuf[toTaskId].add(messages);
				this.msgBufLen[toTaskId].add(vCounter);
			}  
		}
		proMsgOver[toTaskId] = true;
		msgPack.setEdgeInfo(statis[0], statis[6], statis[1], statis[2]);
		
		if (this.msgBuf[toTaskId].size() > 0) {
			msgPack.setRemote(this.msgBuf[toTaskId].remove(0), 
				this.msgBufLen[toTaskId].remove(0), statis[3], statis[5], 0L); 
			//now, we use #dstVert as #recMsg
			if (this.msgBuf[toTaskId].size() == 0) {
				msgPack.setOver();
				this.proMsgOver[toTaskId] = false; //prepare for the next bucket
			}
		}
		
		return msgPack;
	}
	
	private MsgPack<V, W, M, I> packMsg(int toTaskId, int toBlkId, 
			MsgRecord<M>[] cache, long[] _statis) throws IOException {
		MsgPack<V, W, M, I> msgPack = new MsgPack<V, W, M, I>(userTool); //message pack
		msgPack.setEdgeInfo(_statis[0], _statis[6], _statis[1], _statis[2]);
		long memUsage = 0L;
		long loggedBytes = 0L;
		int toGlobalBlkIdx = 
			this.commRT.getJobInformation().getGlobalBlkIdx(toTaskId, toBlkId);
	
		if (_statis[5] > 0) {
			/** msg for local task, send all messages by one pack. */
			if (toTaskId == this.taskId) {
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
				
				if ((job.getCheckPointPolicy()
						==Constants.CheckPoint.Policy.ConfinedRecoveryLogMsg) 
						&& (getUncompletedIteration()==-1)) {
					//log outgoing messages
					ByteArrayOutputStream bytes = 
						new ByteArrayOutputStream(this.job.getMsgPackSize());
					DataOutputStream stream = new DataOutputStream(bytes);
					for (MsgRecord<M> msg: tmp) {
						msg.serialize(stream);
					}
					stream.close();	bytes.close();
					this.packageVersion[toGlobalBlkIdx]++;
					int version = packageVersion[toGlobalBlkIdx] - 1;//starting from zero
					loggedBytes += msgDataServer.logOutgoingMsg(bytes, toGlobalBlkIdx, 
							version, _statis, vCounter);
				}
				
				cache = null;
				//now, we use #dstVert as #recMsg
				msgPack.setLocal(tmp, vCounter, _statis[3], _statis[5], loggedBytes); 
				if (!this.vebFile[toTaskId].hasCheckPoint()) {
					this.proMsgOver[toTaskId] = true;
					msgPack.setOver();
					this.proMsgOver[toTaskId] = false; //pull local msgs only once!
				}
			} else {
				/** msg for remote task, send them by several packs. */
				int vCounter = 0, mCounter = 0, packSize = this.job.getMsgPackSize();
				if (this.vebFile[toTaskId].hasCheckPoint()) {
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
						this.msgBuf[toTaskId].add(bytes);
						this.msgBufLen[toTaskId].add(vCounter);
						this.packageVersion[toGlobalBlkIdx]++;
						memUsage += stream.size();
						
						if ((job.getCheckPointPolicy()
								==Constants.CheckPoint.Policy.ConfinedRecoveryLogMsg) 
								&& (getUncompletedIteration()==-1)) {
							//log outgoing messages
							int version = packageVersion[toGlobalBlkIdx] - 1;//starting from zero
							loggedBytes += msgDataServer.logOutgoingMsg(bytes, toGlobalBlkIdx, 
									version, _statis, vCounter);
						}
						
						vCounter = 0; mCounter = 0;
						bytes = 
							new ByteArrayOutputStream(this.job.getMsgPackSize());
						stream = new DataOutputStream(bytes);
					} //pack
				} //loop all messages
				cache = null;
			
				if (vCounter > 0) {
					stream.close();
					bytes.close();
					this.msgBuf[toTaskId].add(bytes);
					this.msgBufLen[toTaskId].add(vCounter);
					this.packageVersion[toGlobalBlkIdx]++;
					memUsage += stream.size();
					
					if ((job.getCheckPointPolicy()
							==Constants.CheckPoint.Policy.ConfinedRecoveryLogMsg)
							&& (getUncompletedIteration()==-1)) {
						//log outgoing messages
						int version = packageVersion[toGlobalBlkIdx] - 1;//starting from zero
						loggedBytes += msgDataServer.logOutgoingMsg(bytes, toGlobalBlkIdx, 
								version, _statis, vCounter);
					}
				}
			
				if (!this.vebFile[toTaskId].hasCheckPoint()) {
					this.proMsgOver[toTaskId] = true;
				}
				if (this.msgBuf[toTaskId].size() > 0) {
					msgPack.setRemote(this.msgBuf[toTaskId].remove(0), 
						this.msgBufLen[toTaskId].remove(0), _statis[3], _statis[5], loggedBytes); 
					//now, we use #dstVert as #recMsg
					if (this.msgBuf[toTaskId].size()==0 
							&& !this.vebFile[toTaskId].hasCheckPoint()) {
						msgPack.setOver();
						this.proMsgOver[toTaskId] = false; //prepare for the next bucket
					}
				}
			}
		} else {
			msgPack.setOver();
		}
	
		this.memUsedByMsgPull[toTaskId] = 
			Math.max(this.memUsedByMsgPull[toTaskId], memUsage);
	
		return msgPack;
	}
	
	@Override
	public void clearBefIteMemOrDisk(int _iteNum) throws Exception {
		vbFile.clearBefIte();
		for (VEBlockFileHandler veb: vebFile) {
			veb.clearBefIte();
		}
		int type = _iteNum % 2;
		for (int bid = 0; bid < this.verBlkMgr.getBlkNum(); bid++) {
			File dir = getVerDir(bid);
			VerBlockBeta vHbb = this.verBlkMgr.getVerBlkBeta(bid);
			/** 
			 * If this block was updated, a new file (_iteNum) would have been created. 
			 * Thus, the old file created at the (_iteNum-1)-th iteration is useless 
			 * during normal computations. Deleting these useless files can save disk 
			 * space, but here, they are preserved to provide fast fault-tolerance service. 
			 * */
			if (vHbb.isRespond(type)) {
				//File valFile = new File(dir, Vert_File_Value_Prefix + (_iteNum-1));
				//valFile.delete();
			} else {
				/**
				 * Otherwise, the old file name is directly changed from xx-(_iteNum-1) 
				 * to xx-(_iteNum).
				 * 
				 * Note that the renaming process must be performed here, instead of 
				 * @skipBucket. Otherwise, @getMsg called at the (_iteNum-1)-th iteration 
				 * may throw the "File not exist" exception.
				 * 
				 * No responding flag file is created.
				 * 
				 * When re-running the iteration where failures happen, the value file 
				 * at the (_iteNum-1) iteration may not exist because it has been renamed 
				 * by the function clearBefIteMemOrDisk() before failures happen.
				 */
				File f_v_r = new File(dir, Vert_File_Value_Prefix + (_iteNum-1));
				File f_v_w = new File(dir, Vert_File_Value_Prefix + _iteNum);
				if (f_v_r.exists()) {
					f_v_r.renameTo(f_v_w);
					//LOG.info(f_v_r + " to " + f_v_w);
				}
			}
		}
	}
	
	@Override
	public void clearAftIte(int _iteNum, int flagOpt) throws Exception {
		super.clearAftIte(_iteNum, flagOpt);
		vbFile.clearAftIte();
		for (VEBlockFileHandler veb: vebFile) {
			veb.clearAftIte();
		}
		
		if (flagOpt == 1) { 
			//log flags used at the next iteration,
			//if Constants.CheckPoint.Policy is ConfinedRecovery 
			//(LogVert or LogMsg)
			long start = System.currentTimeMillis();
			logFlags(_iteNum+1);
			rwResTime += (System.currentTimeMillis()-start);
		} else if (flagOpt == 2) {
			//confined recovery: load flags used at the next iteration
			long start = System.currentTimeMillis();
			loadFlags(_iteNum+1);
			rwResTime += (System.currentTimeMillis()-start);
		}
	}
	
	/**
	 * Log active and responding flags onto local disks. 
	 * Logged data can be used to perform confined recovery. 
	 * @param _iteNum
	 * @throws Exception
	 */
	private void logFlags(int _iteNum) throws Exception {
		int type = _iteNum % 2;
		for (int bid = 0; bid < this.verBlkMgr.getBlkNum(); bid++) {
			File dir = getVerDir(bid);
			VerBlockBeta vHbb = this.verBlkMgr.getVerBlkBeta(bid);
			int fromIdx = 
				vHbb.getVerMinId() - this.verBlkMgr.getVerMinId(); //inclusive
			int toIdx = vHbb.getVerNum() + fromIdx; //exclusive
			
			if (vHbb.isActive()) {
				String fstr = Vert_File_ActFlag_Prefix + _iteNum;
				serializeFlags(dir, fstr, vHbb.isAllActive(), 
						fromIdx, toIdx, actFlag);
			}
			
			if (vHbb.isRespond(type)) {
				String fstr = Vert_File_ResFlag_Prefix + _iteNum;
				serializeFlags(dir, fstr, vHbb.isAllRespond(type), 
						fromIdx, toIdx, resFlag[type]);
			}
		}
	}
	
	/**
	 * Serialize a flag array with the given beginindex (inclusive) and 
	 * endindex (exclusive).
	 * @param dir
	 * @param filename
	 * @param allTrue
	 * @param fromIdx
	 * @param toIdx
	 * @param flags
	 * @throws Exception
	 */
	private void serializeFlags(File dir, String filename, boolean allTrue, 
			int fromIdx, int toIdx, boolean[] flags) throws Exception {
		File flagFile = null;
		
		if (allTrue) {
			//marked by adding the suffix name
			flagFile = new File(dir, filename + Vert_File_Flag_Suffix);
			if (flagFile.exists()) {
				flagFile.delete();
			}
			if (!flagFile.createNewFile()) {
				LOG.error("fail to create flag file: " + flagFile.toString());
			}
		} else {
			//save the responding flag per vertex
			flagFile = new File(dir, filename);
			if (flagFile.exists()) {
				flagFile.delete();
			}
			bitBytes.serialize(flags, fromIdx, toIdx, flagFile, 
					this.verBlkMgr.getVerMinId());
		}
	}
	
	private void loadFlags(int _iteNum) throws Exception {
		if (_iteNum <= 1) {
			return;
		}
		
		//simulate computations at the current iteration
		int type = _iteNum % 2;
		verBlkMgr.clearBefIte(_iteNum-1);
		for (int bid = 0; bid < this.verBlkMgr.getBlkNum(); bid++) {
			File dir = getVerDir(bid);
			VerBlockBeta vHbb = this.verBlkMgr.getVerBlkBeta(bid);
			int fromIdx = 
				vHbb.getVerMinId() - this.verBlkMgr.getVerMinId(); //inclusive
			int toIdx = vHbb.getVerNum() + fromIdx; //exclusive
			
			String fstr = Vert_File_ActFlag_Prefix + _iteNum;
			deserialize(dir, fstr, fromIdx, toIdx, actFlag);
			
			fstr = Vert_File_ResFlag_Prefix + _iteNum;
			deserialize(dir, fstr, fromIdx, toIdx, resFlag[type]);
			
			for (int flagIdx = fromIdx; flagIdx < toIdx; flagIdx++) {
				if (actFlag[flagIdx]) {
					verBlkMgr.setBlkActive(bid, actFlag[flagIdx]);
					verBlkMgr.incActiveVerNum(bid);
				}
				if (resFlag[type][flagIdx]) {
					verBlkMgr.setBlkRespond(type, bid, resFlag[type][flagIdx]);
					verBlkMgr.incRespondVerNum(bid);
				}
			}
		}
	}
	
	/**
	 * Deserialize a flag array with the given beginindex (inclusive) and 
	 * endindex (exclusive).
	 * @param dir
	 * @param filename
	 * @param fromIdx
	 * @param toIdx
	 * @param flags
	 * @throws Exception
	 */
	private void deserialize(File dir, String filename, 
			int fromIdx, int toIdx, boolean[] flags) throws Exception {
		File f = new File(dir, filename);
		if (!f.exists()) {
			File ftrue = new File(dir, filename+Vert_File_Flag_Suffix);
			if (ftrue.exists()) {
				/** All flags should be set to true. */
				Arrays.fill(flags, fromIdx, toIdx, true);
			} else {
				/** None of flags is true. */
				Arrays.fill(flags, fromIdx, toIdx, false);
			}
		} else {
			/** Deserializing flags one by one. */
			bitBytes.deserialize(flags, fromIdx, toIdx, f, 
					this.verBlkMgr.getVerMinId());
		}
	}
	
	@Override
	public void openGraphDataStreamOnlyForPush(int _parId, int _bid, int _iteNum) 
			throws Exception {
		File dir = getVerDir(_bid);
		File f_v_r = new File(dir, Vert_File_Value_Prefix + _iteNum);
		this.vbFile.openVerReadHandler(f_v_r);
		
		File f_adj = new File(dir, Vert_File_Adj);
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
		File fvr = new File(dir, Vert_File_Value_Prefix + _iteNum);
		this.vbFile.openVerReadHandler(fvr);
		
		File finfo = new File(dir, Vert_File_Info);
		if (this.loadGraphInfo) {
			//curIteStyle=Pull and graphInfo is required
			this.vbFile.openInfoReadHandler(finfo);
			io_byte_info += finfo.length();
		}
		
		File fadj = new File(dir, Vert_File_Adj);
		if (this.loadAdjEdge) {
			this.vbFile.openAdjReadHandler(fadj);
			this.io_byte_adj += fadj.length();
		}
		
		File fvw = new File(dir, Vert_File_Value_Prefix + (_iteNum+1));
		this.vbFile.openVerWriteHandler(fvw);
		
		if (this.estimatePullByteFlag) {
			//curIteStyle=Push
			if (this.job.isUseGraphInfoInUpdate()) {
				//if graphInfo is required by Pull
				this.estimatePullByte += finfo.length();
			}
			if (this.job.isUseAdjEdgeInUpdate()) {
				//if adj is required by Pull
				this.estimatePullByte += fadj.length();
			}
		} else {
			//curIteStyle=Pull, adj must be read by Push.
			this.estimatePushByte += fadj.length();
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
		
		if (_acFlag) {
			this.verBlkMgr.setBlkActive(_bid, _acFlag);
			this.verBlkMgr.incActiveVerNum(_bid);
		}
		
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
		OutputFormat outputformat = 
        	(OutputFormat) ReflectionUtils.newInstance(job.getOutputFormatClass(), 
        		job.getConf());
        outputformat.initialize(job.getConf());
        RecordWriter output = outputformat.getRecordWriter(job, taskId);
        int saveNum = 0;
        
        for (int bid = 0; bid < verBlkMgr.getBlkNum(); bid++) {
        	File dir = getVerDir(bid);
    		File f_v_r = new File(dir, Vert_File_Value_Prefix + _iteNum);
    		vbFile.openVerReadHandler(f_v_r);
    		
    		if (job.isUseGraphInfoInUpdate()) {
    			File f_info = new File(dir, Vert_File_Info);
        		vbFile.openInfoReadHandler(f_info);
    		}
    		if (job.isUseAdjEdgeInUpdate()) {
    			File f_adj = new File(dir, Vert_File_Adj);
    			vbFile.openAdjReadHandler(f_adj);
    		}
        	
    		int blkVertNum = verBlkMgr.getVerBlkBeta(bid).getVerNum();
    		int blkMinId = verBlkMgr.getVerBlkBeta(bid).getVerMinId();
            
    		for (int idx = 0; idx < blkVertNum; idx++) {
    			graph_rw.deserVerValue(vbFile.getVerReadHandler());
    			if (job.isUseGraphInfoInUpdate()) {
    				graph_rw.deserGraphInfo(vbFile.getInfoReadHandler());
    			}
    			if (job.isUseAdjEdgeInUpdate()) {
    				graph_rw.deserEdges(vbFile.getAdjReadHandler());
    			}
    			output.write(new Text(Integer.toString(blkMinId+idx)), 
    					new Text(graph_rw.getFinalValue().toString()));
    		}
    		vbFile.closeVerReadHandler();
    		if (job.isUseGraphInfoInUpdate()) {
    			vbFile.closeInfoReadHandler();
    		}
    		if (job.isUseAdjEdgeInUpdate()) {
    			vbFile.closeAdjReadHandler();
    		}
    		saveNum += blkVertNum;
        }
        
		output.close(job);
		return saveNum;
	}
	
	@Override
	public int archiveCheckPoint(int _version, int _iteNum) throws Exception {
		long startTime = System.currentTimeMillis();

		this.ckpMgr.befArchive(_version);
		int ckpNum = 0;        
		int type = _iteNum % 2;
		int flagIdx = 0;
		int taskMinId = verBlkMgr.getVerMinId();
		StringBuffer sb = new StringBuffer();
		
        for (int bid = 0; bid < verBlkMgr.getBlkNum(); bid++) {
        	File dir = getVerDir(bid);
    		File f_v_r = new File(dir, Vert_File_Value_Prefix + _iteNum);
    		vbFile.openVerReadHandler(f_v_r);
        	
    		int blkVertNum = verBlkMgr.getVerBlkBeta(bid).getVerNum();
    		int blkMinId = verBlkMgr.getVerBlkBeta(bid).getVerMinId();
    		int vertId = 0;
    		for (int idx = 0; idx < blkVertNum; idx++) {
    			vertId = blkMinId + idx;
    			flagIdx = vertId - taskMinId;
    			graph_rw.deserVerValue(vbFile.getVerReadHandler());
    			sb.setLength(0);
    			sb.append(actFlag[flagIdx]? 1:0);
    			sb.append(resFlag[type][flagIdx]? 1:0);
    			sb.append(graph_rw.getVerValue().toString());
        		this.ckpMgr.archive(Integer.toString(vertId), sb.toString());
    		}
    		vbFile.closeVerReadHandler();
    		ckpNum += blkVertNum;
        }
        
        ckpMgr.aftArchive(_version);
        
        /**
         * Delete out-of-date local disk files with suffix id less 
         * than _iteNum. 
         */
        for (int delIdx = 1; delIdx < _iteNum; delIdx++) {
        	for (int bid = 0; bid < verBlkMgr.getBlkNum(); bid++) {
        		File dir = getVerDir(bid);
        		
        		deleteLocalFile(dir, Vert_File_Value_Prefix+delIdx);
        		deleteLocalFile(dir, Vert_File_ActFlag_Prefix+delIdx);
        		deleteLocalFile(dir, Vert_File_ResFlag_Prefix+delIdx);
        		deleteLocalFile(dir, Vert_File_ActFlag_Prefix+delIdx 
        				+Vert_File_Flag_Suffix);
        		deleteLocalFile(dir, Vert_File_ResFlag_Prefix+delIdx 
        				+Vert_File_Flag_Suffix);
        	}
        	msgDataServer.clearLoggedMsg(delIdx);
        }
        
        long endTime = System.currentTimeMillis();
		LOG.info("\narchive " + ckpNum + " vertices into checkpoint, " 
				+ (endTime-startTime)/1000.0 + " seconds, version-" + _version);
		return ckpNum;
	}
	
	private void deleteLocalFile(File dir, String filename) {
		File f = new File(dir, filename);
		if (f.exists()) {
			f.delete();
		}
	}
	
	@Override
	public int loadCheckPoint(int iteNum, int ckpVersion) throws Exception {
		long startTime = System.currentTimeMillis();
		RecordReader<Text,Text> reader = ckpMgr.befLoad(ckpVersion);
		int ckpNum = 0;
		if (reader == null) {
			return ckpNum;
		}
		
		int type = iteNum % 2;
		int bid = 0, vid = 0, vIdx = 0, flagIdx = 0;
		int blkSize = verBlkMgr.getVerBlkBeta(bid).getVerNum();
		GraphRecord[] buf = new GraphRecord[blkSize];
		long bytes = 0L;
		while (reader.nextKeyValue()) {
			buf[vIdx] = userTool.getGraphRecord();
			vid = Integer.parseInt(reader.getCurrentKey().toString());
			flagIdx = vid - verBlkMgr.getVerMinId();
			String ckpValue = reader.getCurrentValue().toString();
			actFlag[flagIdx] = ckpValue.charAt(0)=='1'? true:false;
			resFlag[type][flagIdx] = ckpValue.charAt(1)=='1'? true:false;
			buf[vIdx].parseVerValue(ckpValue.substring(2));
			bytes += buf[vIdx].getVerByte();
			if (actFlag[flagIdx]) {
				verBlkMgr.setBlkActive(bid, actFlag[flagIdx]);
				verBlkMgr.incActiveVerNum(bid);
			}
			if (resFlag[type][flagIdx]) {
				verBlkMgr.setBlkRespond(type, bid, resFlag[type][flagIdx]);
				verBlkMgr.incRespondVerNum(bid);
			}
			
			vIdx++;
			ckpNum++;
			if (vIdx == blkSize) {
				File dir = getVerDir(bid);
				File fvw = new File(dir, Vert_File_Value_Prefix + iteNum);
				if (fvw.exists()) {
					fvw.delete();
				}
				vbFile.openVerWriteHandler(fvw, bytes);
				for (GraphRecord record: buf) {
					record.serVerValue(vbFile.getVerWriteHandler());
				}
				vbFile.closeVerWriteHandler();
				
				bid++;
				vIdx = 0;
				bytes = 0L;
				if (bid < verBlkMgr.getBlkNum()) {
					blkSize = verBlkMgr.getVerBlkBeta(bid).getVerNum();
					buf = new GraphRecord[blkSize];
				}
			}
		}
		
		if (bid != verBlkMgr.getBlkNum()) {
			throw new Exception("verify error: bid=" + bid 
					+ ", but actual #blocks=" + verBlkMgr.getBlkNum());
		}
		
		ckpMgr.aftLoad();
		long endTime = System.currentTimeMillis();
		LOG.info("\nload " + ckpNum + " vertices from checkpoint, " 
				+ (endTime-startTime)/1000.0 + " seconds, version-" + ckpVersion);
		return ckpNum;
	}
	
	@Override
	public void close() {
		LOG.info("read/write flags: " 
				+ this.rwResTime/1000.0 + " seconds");
	}
}
