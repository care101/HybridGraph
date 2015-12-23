package org.apache.hama.myhama.graph;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.monitor.LocalStatistics;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.comm.CommRouteTable;
import org.apache.hama.myhama.comm.MsgPack;
import org.apache.hama.myhama.io.OutputFormat;
import org.apache.hama.myhama.io.RecordWriter;

/**
 * GraphDataServerMem manages graph data in memory.
 * The overall performance may be affected 
 * when starting Java GC or applying for more memory resources 
 * to increase the heap size of JVM.
 * Thus, we encourage users to set -Xmx=-Xms 
 * in $HOME/conf/termite-site.xml as follows:
 * 
 * <property>
 *   <name>bsp.child.java.opts</name>
 *   <value>-Xmx512m -Xms512m</value>
 * </property>
 * 
 * @author 
 * @version 0.1
 */
public class GraphDataServerMem<V, W, M, I> 
		extends GraphDataServer<V, W, M, I> {
	private static final Log LOG = LogFactory.getLog(GraphDataServerMem.class);
	public static int VERTEX_ID_BYTE = 4;
	/**
	 * A temporary byte buffer for (de)serializing a vertex value, 
	 * to separate the two values used in superstep t and t+1.
	 * Its capacity should be big enough to hold the maximum vertex value.
	 * The capacity is 500 (bytes) by default.
	 */
	private MappedByteBuffer byteBuf;
	
	/**
	 * A triple in VBlock.
	 */
	private class VertexTriple<V, W, M, I> {
		private int verId;
		private V curVerValue;
		private V nextVerValue;
		private I graphInfo;
		
		/**
		 * Initialize variables based on the given {@link GraphRecord}.
		 * 
		 * TODO Using byte array can reduce memory footprint, 
		 * while simultaneously increasing CPU costs. 
		 * Is it cost effectient?
		 * 
		 * @param record
		 */
		public VertexTriple(GraphRecord<V, W, M, I> record) {
			verId = record.getVerId();
			curVerValue = record.getVerValue();
			//nextVerValue will be initialized by putForUpdate() if necessary.
			nextVerValue = null;
			graphInfo = record.getGraphInfo();
		}
		
		/**
		 * Get the current vertex value required in 
		 * the user-defined {@link BSPInterface}.compute().
		 * VerValue in {@link GraphRecord} is read-write 
		 * and independent of curVerValue in this triple 
		 * through (de)serializing.
		 * 
		 * @param record
		 * @throws Exception
		 */
		public void getForUpdate(GraphRecord<V, W, M, I> record) 
				throws Exception {
			record.setVerId(verId);
			record.setVerValue(curVerValue);
			record.setGraphInfo(graphInfo);
			
			byteBuf.position(0);
			record.serVerValue(byteBuf); //serialize
			byteBuf.position(0);
			record.deserVerValue(byteBuf); //deserialize
		}
		
		/**
		 * Get the current vertex value required in 
		 * {@link GraphRecord}.getMsg(), 
		 * in order to respond pull requests from target vertices. 
		 * VerValue in {@link GraphRecord} is read-only.
		 * 
		 * @param record
		 */
		public void getForRespond(GraphRecord<V, W, M, I> record) {
			record.setVerId(verId);
			record.setVerValue(curVerValue); //read-only
		}
		
		/**
		 * Save the updated vertex value which will be read 
		 * by getForUpdate() and/or getForRespond() 
		 * in the next superstep.
		 * @param record
		 * @throws Exception
		 */
		public void putAftUpdate(GraphRecord<V, W, M, I> record) {
			nextVerValue = record.getVerValue();
		}
		
		/**
		 * Exchange for normally running the next superstep.
		 * Invoked before launching a new superstep.
		 */
		public void exchange() {
			curVerValue = nextVerValue;
			nextVerValue = null;
		}
	}
	
	/**
	 * A fragment in EBlock.
	 */
	private class EdgeFragment<V, W, M, I> {
		private int verId;
		private int edgeNum;
		private Integer[] edgeIds;
		private W[] edgeWeights;
		
		/**
		 * Initialize variables based on the given {@link GraphRecord}.
		 * TODO Using byte array or not?
		 * 
		 * @param record
		 */
		public EdgeFragment(GraphRecord<V, W, M, I> record) {
			verId = record.getVerId();
			edgeNum = record.getEdgeNum();
			edgeIds = record.getEdgeIds();
			edgeWeights = record.getEdgeWeights();
		}
		
		/**
		 * Get edges required in {@link GraphRecord}.getMsg(), 
		 * in order to respond pull requests from target vertices. 
		 * Edges in {@link GraphRecord} is read-only.
		 * 
		 * @param record
		 */
		public void getForRespond(GraphRecord<V, W, M, I> record) {
			record.setVerId(verId);
			record.setEdgeNum(edgeNum);
			record.setEdges(edgeIds, edgeWeights);
		}
	}
	
	/** VBlocks: [local_block_id][triple_idx] */
	private VertexTriple<V,W,M,I>[][] vBlocks;
	/** EBlocks: [local_block_id][global_block_id] = array of fragments */
	private ArrayList<EdgeFragment<V,W,M,I>>[][] eBlocks;
	private long vBlockByte = 0L, eBlockByte = 0L, veBlockByte = 0L;
	/** Index in one VBlock, used in getNextGraphRecord() */
	private int tripleIdx = 0;
	/** Need to exchange {@link VertexTriple}.curVerValue and 
	 *  {@link VertexTriple}.nextVerValue, or not? 
	 *  True:exchange, 
	 *  False:nothing.
	 *  False by default.
	 */
	private boolean[] vBlockExchFlag;
	
	/**
	 * Constructing GraphDataServerMem.
	 * 
	 * @param _parId
	 * @param _job
	 * @param _rootDir
	 * @throws Exception 
	 */
	public GraphDataServerMem(int _parId, BSPJob _job, String _rootDir) 
			throws Exception {
		super(_parId, _job);
		createDir(_rootDir);
		
		LOG.info("set useGraphInfo=" + this.useGraphInfo 
				+ " when loading graph data, memory version");
	}
	
	private void createDir(String _rootDir) throws Exception {
		File rootDir = new File(_rootDir);
		if (!rootDir.exists()) {
			rootDir.mkdirs();
		}
		
		File tmpBufFile = new File(_rootDir, "tmpBufFile");
		RandomAccessFile raf = new RandomAccessFile(tmpBufFile, "rw");
		FileChannel fc = raf.getChannel();
		this.byteBuf = 
			fc.map(FileChannel.MapMode.READ_WRITE, 0, 500);
	}
	
	@Override
	public void initMemOrDiskMetaData(LocalStatistics lStatis, 
			final CommRouteTable<V, W, M, I> _commRT) throws Exception {
		int locBucNum = lStatis.getBucNum();
		int locBucLen = lStatis.getBucLen();
		this.vBlocks = 
			(VertexTriple<V, W, M, I>[][]) new VertexTriple[locBucNum][];
		for (int locBid = 0; locBid < locBucNum; locBid++) {
			this.vBlocks[locBid] = 
				(VertexTriple<V, W, M, I>[]) new VertexTriple[locBucLen];
		}
		this.tripleIdx = 0;
		this.vBlockExchFlag = new boolean[locBucNum];
		Arrays.fill(this.vBlockExchFlag, false);
		
		/** only used in pull or hybrid */
		if (this.bspStyle != Constants.STYLE.Push) {
			int gBucNum = this.commRT.getGlobalSketchGraph().getBucNumJob();
			this.eBlocks = (ArrayList<EdgeFragment<V, W, M, I>>[][])
				new ArrayList[locBucNum][gBucNum];
			for (int locBid = 0; locBid < locBucNum; locBid++) {
				for (int idx = 0; idx < gBucNum; idx++) {
					this.eBlocks[locBid][idx] = 
						new ArrayList<EdgeFragment<V, W, M, I>>();
				}//gBucIdx
			}//locBid
		}//for pull or hybrid
	}
	
	private void putIntoVerBuf(GraphRecord<V, W, M, I> graph, 
			int _bid, int idx) {
		this.vBlockByte += (VERTEX_ID_BYTE + graph.getVerByte() 
				+ graph.getGraphInfoByte());
		this.vBlocks[_bid][idx] = 
			new VertexTriple<V, W, M, I>(graph);
	}
	
	private void putIntoEdgeBuf(ArrayList<GraphRecord<V, W, M, I>> graphs) {
		int vid = -1, locBid = -1, dstTid = -1, dstBid = -1, gBucIdx = -1;
		for (GraphRecord<V, W, M, I> graph: graphs) {
			this.eBlockByte += (VERTEX_ID_BYTE + graph.getEdgeByte());
			
			vid = graph.getVerId();
			locBid = graph.getSrcBucId();
			dstTid = graph.getDstParId();
			dstBid = graph.getDstBucId();
			gBucIdx = 
				this.commRT.getGlobalSketchGraph().getGlobalBucIndex(dstTid, dstBid);
			this.eBlocks[locBid][gBucIdx].add(
					new EdgeFragment<V, W, M, I>(graph));
			
			verBucMgr.updateBucEVidEdgeLen(locBid, dstTid, dstBid, 
					vid, VERTEX_ID_BYTE+graph.getEdgeByte());
			verBucMgr.updateBucEVerNum(locBid, dstTid, dstBid, vid, 1);
			edgeBucMgr.updateBucNum(dstTid, dstBid, 1, graph.getEdgeNum());
			for (int eid: graph.getEdgeIds()) {
				edgeBucMgr.updateBucEdgeIdBound(dstTid, dstBid, eid);
			}
		}
	}
	
	@Override
	public void loadGraphData(LocalStatistics lStatis, BytesWritable rawSplit, 
			String rawSplitClass) throws Exception {
		long startTime = System.currentTimeMillis();
		long edgeNum = 0L;
		initInputSplit(rawSplit, rawSplitClass);
		int[] idxs = new int[lStatis.getBucNum()]; //record index of triples
		
		int bid = -1, vid = 0;
		while (input.nextKeyValue()) {
			GraphRecord<V, W, M, I> graph = this.userTool.getGraphRecord();
			graph.initGraphData(input.getCurrentKey().toString(), 
					input.getCurrentValue().toString());
			edgeNum += graph.getEdgeNum();
			vid = graph.getVerId();
			bid = commRT.getDstBucId(parId, vid);
			graph.setSrcBucId(bid);
			this.verBucMgr.updateBucEdgeNum(bid, vid, graph.getEdgeNum());
			this.verBucMgr.updateBucVerInfoLen(bid, vid, 
					graph.getVerByte(), graph.getGraphInfoByte());
			
			putIntoVerBuf(graph, bid, idxs[bid]);
			idxs[bid]++;
			if (this.bspStyle != Constants.STYLE.Push) {
				ArrayList<GraphRecord<V, W, M, I>> graphs = 
					graph.decompose(commRT, lStatis);
				putIntoEdgeBuf(graphs);
			}
		}
		
		this.verBucMgr.loadOver(this.bspStyle);
		this.veBlockByte = this.vBlockByte + this.eBlockByte;
		
		int[] verNumBucs = new int[this.verBucMgr.getHashBucNum()];
		for (int i = 0; i < this.verBucMgr.getHashBucNum(); i++) {
			verNumBucs[i] = this.verBucMgr.getVerHashBucBeta(i).getVerNum();
		}
		int[] actVerNumBucs = new int[this.verBucMgr.getHashBucNum()];
		Arrays.fill(actVerNumBucs, 0);
		lStatis.setVerNumBucs(verNumBucs);
		lStatis.setActVerNumBucs(actVerNumBucs);
		lStatis.setEdgeNum(edgeNum);
		lStatis.setLoadByte(this.veBlockByte);
		this.memUsedByMetaData = this.verBucMgr.getMemUsage();
		if (this.bspStyle != Constants.STYLE.Push) {
			this.memUsedByMetaData += this.edgeBucMgr.getMemUsage();
		}
		
		long endTime = System.currentTimeMillis();
		LOG.info("load graph from HDFS, costTime=" 
				+ (endTime-startTime)/1000.0 + " seconds");
	}
	
	@Override
	public MsgPack<V, W, M, I> getMsg(int _tid, int _bid, int _iteNum) 
			throws Exception {
		if (this.proMsgOver[_tid]) {
			MsgPack<V, W, M, I> msgPack = new MsgPack<V, W, M, I>(userTool);
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
			return new MsgPack<V, W, M, I>(this.userTool); //no edge
		}
		/** create cache whose capacity = the number of destination vertices */
		MsgRecord<M>[] cache = 
			(MsgRecord<M>[]) new MsgRecord[dstVerMaxId-dstVerMinId+1];
		//io, edge_read, fragment_read, msg_pro, msg_rec, dstVerHasMsg.
		long[] statis = new long[6];
		for (int resBid = 0; resBid < this.verBucMgr.getHashBucNum(); resBid++) {
			VerHashBucBeta vHbb = this.verBucMgr.getVerHashBucBeta(resBid);
			VerMiniBucMgr vMbMgr = vHbb.getVerMiniBucMgr();
			
			if (!vHbb.isUpdated(type) || (vHbb.getEVerNum(_tid, _bid)==0)) {
				continue; //skip the whole hash bucket
			}
			
			//vMbMgr and cache: pass-by-reference
			this.getMsgMiniBucket(statis, vMbMgr, resBid, 
					type, _tid, _bid, cache, dstVerMinId);
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
			if (reqTid == this.parId) {
				MsgRecord<M>[] tmp = (MsgRecord<M>[]) new MsgRecord[(int)_statis[5]];
				int vCounter = 0;
				for (MsgRecord<M> msg: cache) {
					if (msg != null) {
						tmp[vCounter++] = msg;
						memUsage += msg.getMsgByte();
						msg = null;
					}
				}
				cache = null;
				//now, we use #dstVert as #recMsg
				msgPack.setLocal(tmp, vCounter, _statis[3], _statis[5]);
				this.proMsgOver[reqTid] = true;
				msgPack.setOver();
				this.proMsgOver[reqTid] = false; //pull local msgs only once!
			} else {
				/** msg for remote task, send them by several packs. */
				int vCounter = 0, mCounter = 0, packSize = this.job.getMsgPackSize();
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
				
				this.proMsgOver[reqTid] = true;
				if (this.msgBuf[reqTid].size() > 0) {
					//now, we use #dstVert as #recMsg
					msgPack.setRemote(this.msgBuf[reqTid].remove(0), 
							this.msgBufLen[reqTid].remove(0), _statis[3], _statis[5]);
					if (this.msgBuf[reqTid].size() == 0) {
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
	 * @param resBid
	 * @param type
	 * @param _tid
	 * @param _bid
	 * @param cache
	 * @param dstVerMinId
	 * @return
	 * @throws Exception
	 */
	private void getMsgMiniBucket(long[] statis, VerMiniBucMgr vMbMgr, int resBid, 
			int type, int _tid, int _bid, 
			MsgRecord<M>[] cache, 
			int dstVerMinId) throws Exception {
		int dstBucIdx = 
			this.commRT.getGlobalSketchGraph().getGlobalBucIndex(_tid, _bid);
		GraphRecord<V, W, M, I> graph = this.userTool.getGraphRecord();
		
		for (EdgeFragment<V, W, M, I> frag : this.eBlocks[resBid][dstBucIdx]) {
			frag.getForRespond(graph);
			statis[1] += graph.getEdgeNum(); // edge_read
			statis[2]++; // fragment_read

			if (!upFlag[type][graph.getVerId()-this.verMinId]) {
				continue;
			}
			this.vBlocks[resBid][graph.getVerId()-this.locMinVerIds[resBid]]
					.getForRespond(graph);

			MsgRecord<M>[] msgs = graph.getMsg(this.preIteStyle);
			this.locMatrix[resBid][dstBucIdx] += msgs.length;
			statis[3] += msgs.length; // msg_pro
			for (MsgRecord<M> msg : msgs) {
				int index = msg.getDstVerId() - dstVerMinId;
				if (cache[index] == null) {
					cache[index] = msg;
					statis[4]++; // msg_rec
					statis[5]++; // dstVerHasMsg
				} else {
					cache[index].collect(msg);
					if (!this.isAccumulated) {
						statis[4]++; // msg_rec
					}
				}
			}//put messages into one sub send-buffer(BS_{i})
		}//respond pull requests for one VBlock
	}
	
	@Override
	public void openGraphDataStreamOnlyForPush(int _parId, int _bid, int _iteNum) 
		throws Exception {
		this.tripleIdx = 0;
	}
	
	@Override
	public void closeGraphDataStreamOnlyForPush(int _parId, int _bid, int _iteNum) 
		throws Exception {
	}
	
	@Override
	public void openGraphDataStream(int _parId, int _bid, int _iteNum) 
			throws Exception {
		this.tripleIdx = 0;
		this.vBlockExchFlag[_bid] = true;
	}
	
	@Override
	public void closeGraphDataStream(int _parId, int _bid, int _iteNum) 
			throws Exception {
	}
	
	@Override
	public GraphRecord<V, W, M, I> getNextGraphRecord(int _bid) 
			throws Exception {
		graph_rw.setVerId(this.verBucMgr.getVerHashBucBeta(_bid).getVerId());
		this.vBlocks[_bid][tripleIdx].getForUpdate(graph_rw);
		this.tripleIdx++;
		return graph_rw;
	}
	
	@Override
	public void saveGraphRecord(int _bid, int _iteNum, 
			boolean _acFlag, boolean _upFlag) throws Exception {
		int index = graph_rw.getVerId() - this.verMinId; //global index
		int type = (_iteNum+1)%2;
		acFlag[index] = _acFlag;
		upFlag[type][index] = _upFlag;
		if (_upFlag) {
			this.verBucMgr.setBucUpdated(type, _bid, graph_rw.getVerId(), _upFlag);
			this.verBucMgr.incUpdVerNumBuc(_bid);
		}
		
		this.vBlocks[_bid][graph_rw.getVerId()-
		                      this.locMinVerIds[_bid]].putAftUpdate(graph_rw);
	}
	
	@Override
	public void clearBefIteMemOrDisk(int _iteNum) {
		for (int bid = 0; bid < this.verBucMgr.getHashBucNum(); bid++) {
			if (this.vBlockExchFlag[bid]) {
				int bucVerNum = this.verBucMgr.getVerHashBucBeta(bid).getVerNum();
				for (int idx = 0; idx < bucVerNum; idx++) {
					this.vBlocks[bid][idx].exchange();
				}
			}
			this.vBlockExchFlag[bid] = false;
        }
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
        
        for (int bid = 0; bid < this.verBucMgr.getHashBucNum(); bid++) {
        	int bucVerNum = this.verBucMgr.getVerHashBucBeta(bid).getVerNum();
        	for (int idx = 0; idx < bucVerNum; idx++) {
        		this.vBlocks[bid][idx].getForUpdate(graph_rw);
        		output.write(new Text(Integer.toString(graph_rw.getVerId())), 
    					new Text(graph_rw.getFinalValue().toString()));
            	saveNum++;
        	}
        }
        
		output.close(job);
		return saveNum;
	}
}
