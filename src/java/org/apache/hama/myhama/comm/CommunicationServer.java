/**
 * Termite System
 * 
 * copyright 2012-2010
 */
package org.apache.hama.myhama.comm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.ipc.CommunicationServerProtocol;
import org.apache.hama.monitor.JobInformation;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.graph.GraphDataServer;
import org.apache.hama.myhama.graph.MsgDataServer;

/**
 * CommunicationServer. 
 * 
 * @author 
 * @version 0.1
 */
public class CommunicationServer<V, W, M, I> 
		implements CommunicationServerProtocol<V, W, M, I> {
	private static final Log LOG = LogFactory.getLog(CommunicationServer.class);
	private int taskNum;
	private HamaConfiguration conf;
	private BSPJobID jobId;
	private int parId;
	
	private MsgDataServer<V, W, M, I> msgDataServer;
	private GraphDataServer<V, W, M, I> graphDataServer;
	
	private String bindAddr;
	private InetSocketAddress peerAddr;
	private int serverPort;
	private Server server;
	private CommRouteTable<V, W, M, I> commRT;
	
	private volatile Integer mutex = 0;
	private boolean hasNotify = false;
	private SuperStepCommand ssc;
	
	private ExecutorService msgHandlePool;
	private HashMap<Integer, Future<Boolean>> pushMsgResult;
	private ArrayList<Future<Boolean>> pullMsgResult;
	private ArrayList<Integer>[] pullRoute; //pull messages from these tasks.
	private AtomicInteger counter;
	private Integer pullNum = 0;
	private int localBucNum;
	
	private long io_byte = 0L, io_byte_vert;
	private long read_edge = 0L, read_fragment = 0L;
	/** 
	 * 1. msg_net: original network messages.
	 * 2. msg_net_actual: actual network messages after being combined/concatenated. 
	 *                    for push, .= msg_net, for pull, .<= msg_net.
	 * 3. msg_disk: messages resident on disk (push).
	 * 4. msg_rec: received messages. for push, .= msg_pro, for pull, .<= msg_pro.
	 * */
	private long msg_pro = 0L, msg_rec = 0L, 
				 msg_net = 0L, msg_net_actual = 0L, msg_disk = 0L;
	
	public class PushMsgDataThread implements Callable<Boolean> {
		private int srcParId, iteNum;
		private InetSocketAddress srcAddr, dstAddr;
		private MsgPack<V, W, M, I> msgPack;
		
		public PushMsgDataThread(int _srcParId, int _dstParId, int _iteNum, 
				MsgPack<V, W, M, I> _msgPack, InetSocketAddress _srcAddr, 
				InetSocketAddress _dstAddr) {
			this.srcParId = _srcParId;
			this.iteNum = _iteNum;
			this.srcAddr = _srcAddr; this.dstAddr = _dstAddr;
			this.msgPack = _msgPack;
		}
		
		public Boolean call() {
			try {
				long _msg_rec = 0L, _msg_net = 0L, _msg_disk = 0L;
				_msg_rec = this.msgPack.getMsgRecNum();
				if (this.srcAddr.equals(this.dstAddr)) {
					_msg_disk = 
						recMsgData(this.srcParId, this.iteNum, this.msgPack);
				} else {
					_msg_net = _msg_rec;
					CommunicationServerProtocol<V, W, M, I> comm = 
						commRT.getCommServer(this.dstAddr);
					_msg_disk = 
						comm.recMsgData(this.srcParId, this.iteNum, this.msgPack);
				}
				updateCounters(0L, 0L, 0L, 0L, 0L, 0L, _msg_net, _msg_net, _msg_disk);
				
				return true;
			} catch (Exception e) {
				LOG.error("pushMsgThread", e);
				return false;
			}
		}
	}
	
	private class PullMsgDataThread implements Callable<Boolean> {
		private int srcParId;
		private int bid;
		private int iteNum;
		private InetSocketAddress srcAddr, dstAddr;
		private boolean isOver;
		
		@SuppressWarnings("unchecked")
		public PullMsgDataThread(int _srcParId, int _bid, int _iteNum, 
				InetSocketAddress _srcAddr, InetSocketAddress _dstAddr) {
			this.srcParId = _srcParId;
			this.bid = _bid;
			this.iteNum = _iteNum;
			this.srcAddr = _srcAddr;
			this.dstAddr = _dstAddr;
			this.isOver = false;
		}
		
		@Override
		public Boolean call() {
			boolean flag = false;
			try {
				while (!this.isOver) {
					long _msg_rec = 0L, _msg_net = 0L, _msg_net_actual = 0L;
					MsgPack<V, W, M, I> recMsgPack = null;
					if (this.srcAddr.equals(this.dstAddr)) {
						recMsgPack = 
							obtainMsgData(this.srcParId, this.bid, this.iteNum);
					} else {
						CommunicationServerProtocol<V, W, M, I> comm = 
							commRT.getCommServer(this.dstAddr);
						if (comm == null) {
							LOG.error("[PullMsgDataThread]: comm is null " 
									+ dstAddr.toString());
							this.isOver = true;
							return isOver;
						} else {
							recMsgPack = 
								comm.obtainMsgData(this.srcParId, this.bid, this.iteNum);
							_msg_net = recMsgPack.getMsgProNum();
							_msg_net_actual = recMsgPack.getMsgRecNum();
						}
					}
					
					_msg_rec = recMsgPack.getMsgRecNum();
					updateCounters(recMsgPack.getIOByte(), 
							recMsgPack.getIOByteOfVertInPull(), 
							recMsgPack.getReadEdgeNum(), 
							recMsgPack.getReadFragNum(),
							recMsgPack.getMsgProNum(), _msg_rec, 
							_msg_net, _msg_net_actual, 0L);
					/** 
					 * Use .size() instead of getMsgRecNum(), 
					 * the latter of subsequent @{link MsgPack}s is zero.
					 **/
					if (recMsgPack.size() > 0) {
						msgDataServer.putIntoBuf(this.bid, this.iteNum, recMsgPack);
					}
					this.isOver = recMsgPack.isOver();
				} //while
				
				pullOver();
				
				flag = true;
			} catch (Exception e) {
				pullOver();
				LOG.error("pullMsgThread", e);
				flag = false;
			}
			return flag;
		}
	}
	
	public CommunicationServer (BSPJob job, int parId, TaskAttemptID taskId) 
			throws Exception {
		this.conf = new HamaConfiguration();
		this.jobId = job.getJobID();
		this.parId = parId;
		taskNum = job.getNumBspTask();
		this.msgHandlePool = Executors.newFixedThreadPool(taskNum);
		this.pushMsgResult = new HashMap<Integer, Future<Boolean>>(taskNum);
		this.pullMsgResult = new ArrayList<Future<Boolean>>(taskNum);
		LOG.info("start msg handle threads: " + taskNum);
		
		this.commRT = new CommRouteTable<V, W, M, I>(job, this.parId);
		this.bindAddr = job.get("host");
		this.serverPort = conf.getInt(Constants.PEER_PORT, 
				Constants.DEFAULT_PEER_PORT) 
				+ Integer.parseInt(jobId.toString().substring(17)) 
				+ Integer.parseInt(taskId.toString().substring(26,32));
		this.peerAddr = new InetSocketAddress(this.bindAddr, this.serverPort);
		
		this.server = RPC.getServer(this, 
				this.peerAddr.getHostName(), this.peerAddr.getPort(), this.conf);
		this.server.start();
		LOG.info("CommunicationServer address:" + this.peerAddr.getHostName() 
				+ " port:" + this.peerAddr.getPort());
		
		this.counter = new AtomicInteger(0);
	}
	
	public void bindGraphData(GraphDataServer<V, W, M, I> _graphDataServer, 
			int _locBucNum) {
		graphDataServer = _graphDataServer;
		this.localBucNum = _locBucNum;
	}
	
	public void bindMsgDataServer(MsgDataServer<V, W, M, I> _msgDataServer) {
		msgDataServer = _msgDataServer;
	}
	
	public String getAddress() {
		return bindAddr;
	}
	
	public int getPort() {
		return this.serverPort;
	}
	
	/**
	 * Push messages to target vertices.
	 * First, search the route table for each message 
	 * and save it in the sendMessageBuffer.
	 * Second, if the sendMessageBuffer is full, 
	 * then send messages to the target task.
	 * If the target task is itself, save messages on the
	 * local disk directly, else send messages by RPC Server.
	 * @param result
	 * @param superStepCounter
	 */
	public void pushMsgData(MsgRecord<M>[] msgData, int _iteNum) 
			throws Exception {
		int dstVid, dstPid, pro_msg = msgData.length;
		updateCounters(0L, 0L, 0L, 0L, pro_msg, pro_msg, 0L, 0L, 0L);
		
		for(int idx = 0; idx < pro_msg; idx++) {
			dstVid = msgData[idx].getDstVerId();
			dstPid = commRT.getDstTaskId(dstVid);
			switch(this.msgDataServer.putIntoSendBuffer(dstPid, msgData[idx])) {
			case NORMAL :
				break;
			case OVERFLOW :
				MsgPack<V, W, M, I> msgPack = this.msgDataServer.getMsgPack(dstPid);
				InetSocketAddress dstAddress = commRT.getInetSocketAddress(dstPid);
				if (this.pushMsgResult.containsKey(dstPid)) {
					Future<Boolean> monitor = this.pushMsgResult.remove(dstPid);
					if (!monitor.isDone()) {
						monitor.get();
					}
					if (monitor.get() == false) {
						throw new Exception("ERROR");
					}
				}
				startPushMsgDataThread(dstPid, dstAddress, _iteNum, msgPack);
				break;
			default : LOG.error("[sendMsgData] Fail send messages to Partition " 
					+ dstPid + " at SuperStep " + _iteNum);
					throw new Exception("invalid BufferStatus");
			}
		}
	}
	
	/** 
	 * Flush all remaining messages in the sendBuffer, 
	 * used in push at the end of one superstep. 
	 * */
	public void pushFlushMsgData(int iteNum) throws Exception {
		InetSocketAddress dstAddr;
		
		clearPushMsgResult();
		for (int dstParId = 0; dstParId < taskNum; dstParId++) {
			dstAddr = commRT.getInetSocketAddress(dstParId);
			if (this.msgDataServer.getSendBufferSize(dstParId) > 0) {
				MsgPack<V, W, M, I> pack = this.msgDataServer.getMsgPack(dstParId);
				startPushMsgDataThread(dstParId, dstAddr, iteNum, pack);
			}
		}
		clearPushMsgResult();
		this.msgDataServer.clearSendBuffer();
	}
	
	/**
	 * Clear the pushMsgResult at the end of one SuperStep.
	 */
	private void clearPushMsgResult() throws Exception {
		for (Future<Boolean> e : this.pushMsgResult.values()) {
			if (e.get() == false) {
				throw new Exception("ERROR");
			}
		}
		this.pushMsgResult.clear();
	}
	
	/**
	 * Pull messages from source vertices.
	 * First, signal each essential source task to 
	 * produce messages based on source vertices.
	 * Second, pull messages in {@link MsgPack} 
	 * one by one for each source task, 
	 * and combine them.
	 * Wait until all messages have been pulled and combined.
	 * For local messages, directly combine them, 
	 * otherwise, get them by RPC Server.
	 * @param _srcParId
	 * @param _bid
	 * @param _iteNum
	 * @return
	 * @throws Exception
	 */
	public long pullMsgFromSource(int _srcParId, int _bid, int _iteNum) 
			throws Exception {
		if (_iteNum == 1) {
			return 0L;
		}
		
		long start = System.currentTimeMillis();
		if (_bid==0 || !this.msgDataServer.isAccumulated()) {
			this.pullNum = this.pullRoute[_bid].size();
			for (int tid: this.pullRoute[_bid]) {
				InetSocketAddress dstAddress = commRT.getInetSocketAddress(tid);
				startPullMsgDataThread(tid, dstAddress, _bid, _iteNum);
			}
		}
		
		if (this.pullNum > 0) {
			this.barrier(); //wait, until all msgs have been pulled for cur bucket.
		}
		
		for (Future<Boolean> f: this.pullMsgResult) {
			if (f.get() == false) {
				throw new Exception("ERROR");
			}
		} //check if Exception happens when pulling.
		this.pullMsgResult.clear();
		
		if (this.msgDataServer.isAccumulated()) {
			this.msgDataServer.switchPreMsgToCache();
			if ((_bid+1)<this.localBucNum) {
				this.pullNum = this.pullRoute[(_bid+1)].size();
				for (int tid: this.pullRoute[(_bid+1)]) {
					InetSocketAddress dstAddress = commRT.getInetSocketAddress(tid);
					startPullMsgDataThread(tid, dstAddress, (_bid+1), _iteNum);
				}
			}
		}
		
		return (System.currentTimeMillis()-start);
	}
	
	/**
	 * Update counters.
	 * For Push, this should be invoked when sending messages.
	 * For Pull, this should be invoked when pulling messages.
	 * @param _io_byte
	 * @param _io_byte_vert
	 * @param _read_edge
	 * @param _read_fragment
	 * @param _msg_pro
	 * @param _msg_rec
	 * @param _msg_net
	 * @param _msg_net_actual
	 * @param _msg_disk
	 */
	public synchronized void updateCounters(long _io_byte, long _io_byte_vert, 
			long _read_edge, long _read_fragment,
			long _msg_pro, long _msg_rec, 
			long _msg_net, long _msg_net_actual, long _msg_disk) {
		this.io_byte += _io_byte;
		this.io_byte_vert += _io_byte_vert;
		this.read_edge += _read_edge;
		this.read_fragment += _read_fragment;
		this.msg_pro += _msg_pro;
		this.msg_rec += _msg_rec;
		this.msg_net += _msg_net;
		this.msg_net_actual += _msg_net_actual;
		this.msg_disk += _msg_disk;
	}
	
	/** 
	 * Return io_byte when pulling messages from source vertices.
	 * Only make sense for style.Pull.
	 * It is produced due to reading fragments 
	 * and vertex_values in source tasks.
	 */
	public long getIOByte() {
		return this.io_byte;
	}
	
	public long getIOByteOfVertInPull() {
		return this.io_byte_vert;
	}
	
	/**
	 * Return #edges read when pulling messages from source vertices.
	 * Only make sense for style.Pull.
	 * @return
	 */
	public long getReadEdgeNum() {
		return this.read_edge;
	}
	
	/**
	 * Return #fragments read when pulling messages from source vertices.
	 * Only make sense for style.Pull.
	 * @return
	 */
	public long getReadFragmentNum() {
		return this.read_fragment;
	}
	
	/**
	 * Return #messages produced at the sender side.
	 * @return
	 */
	public long getMsgProNum() {
		return this.msg_pro;
	}
	
	/**
	 * Return #messages received at the receiver side. 
	 * For style.Push, it is equal with getMsgProNum();
	 * for style.Pull, it should be less than getMsgProNum() 
	 * due to combining/concatenating.
	 * @return
	 */
	public long getMsgRecNum() {
		return this.msg_rec;
	}
	
	/**
	 * Return #messages transferred via network. 
	 * (no combining/concatentating)
	 * @return
	 */
	public long getMsgNetNum() {
		return this.msg_net;
	}
	
	/**
	 * Return #messages actually transferred via network. 
	 * For style.Push, it is equal with getMsgNetNum();
	 * for style.Pull, it should be less than getMsgNetNum() 
	 * due to combining/cancatenating.
	 * @return
	 */
	public long getMsgNetActualNum() {
		return this.msg_net_actual;
	}
	
	/**
	 * Return #messages written onto local disks. 
	 * It only makes sense for style.Push. 
	 * For style.Pull, that shoule be zero.
	 */
	public long getMsgOnDisk() {
		return this.msg_disk;
	}
	
	public void clearBefIte(int _iteNum, int _iteStyle) {
		this.io_byte = 0L;
		this.io_byte_vert = 0L;
		this.read_edge = 0L;
		this.read_fragment = 0L;
		this.msg_pro = 0L;
		this.msg_rec = 0L;
		this.msg_net = 0L;
		this.msg_net_actual = 0L;
		this.msg_disk = 0L;
	}
	
	private void startPushMsgDataThread(int dstParId, InetSocketAddress dstAddr, 
			int iteNum, MsgPack<V, W, M, I> msgPack) {
		Future<Boolean> future = this.msgHandlePool.submit(
				new PushMsgDataThread(parId, dstParId, iteNum, 
						msgPack, peerAddr, dstAddr));
		this.pushMsgResult.put(dstParId, future);
	}
	
	private void startPullMsgDataThread(int _dstParId, InetSocketAddress dstAddr, 
			int _bid, int _iteNum) {
		Future<Boolean> future =
			this.msgHandlePool.submit(new PullMsgDataThread(parId, _bid, 
					_iteNum, peerAddr, dstAddr));
		this.pullMsgResult.add(future);
	}
	
	public void barrier() throws Exception {
		synchronized(mutex) {
			if (!this.hasNotify) {
				mutex.wait();
			}
			
			this.hasNotify = false;
		}
	}
	
	public void pullOver() {
		int cur = this.counter.incrementAndGet();
		//LOG.info("counter=" + this.counter.get() + ", pullNum=" + this.pullNum);
		if (cur == this.pullNum) {
			this.counter.set(0);
			synchronized(mutex) {
				this.hasNotify = true;
				mutex.notify();
			}
		}
	}
	
	@Override
	public long recMsgData(int srcParId, int iteNum, MsgPack<V, W, M, I> pack) 
			throws Exception {
		if (pack.size() > 0) {
			return this.msgDataServer.recMsgData(srcParId, pack);
		} else {
			return 0;
		}
	}
	
	@Override
	public MsgPack<V, W, M, I> obtainMsgData(int _srcParId, int _bid, int _iteNum) 
			throws Exception {
		return this.graphDataServer.getMsg(_srcParId, _bid, _iteNum);
	}
	
	@Override
	public void buildRouteTable(JobInformation global) {
		this.commRT.initialilze(global);
		
		synchronized(mutex) {
			this.hasNotify = true;
			mutex.notify();
		}
	}
	
	@Override
	public void setPreparation(JobInformation _jobInfo) {
		this.commRT.resetJobInformation(_jobInfo);
		
		synchronized(mutex) {
			this.hasNotify = true;
			mutex.notify();
		}
	}
	
	@Override
	public void setNextSuperStepCommand(SuperStepCommand ssc) {
		this.ssc = ssc;
		this.pullRoute = this.ssc.getRealRoute();
		
		synchronized(mutex) {
			this.hasNotify = true;
			mutex.notify();
		}
	}
	
	@Override
	public void startNextSuperStep() {
		synchronized(mutex) {
			this.hasNotify = true;
			mutex.notify();
		}
	}
	
	@Override
	public void quitSync() {
		synchronized(mutex) {
			this.hasNotify = true;
			mutex.notify();
		}
	}
	
	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return CommunicationServerProtocol.versionID;	
	}
	
	@Override
	public void close() {
		this.server.stop();
		this.msgHandlePool.shutdownNow();
	}
	
	public final CommRouteTable<V, W, M, I> getCommRouteTable() {
		return this.commRT;
	}
	
	public SuperStepCommand getNextSuperStepCommand() {
		return this.ssc;
	}

	
	public final JobInformation getJobInformation() {
		return this.commRT.getJobInformation();
	}
}
