/**
 * Termite System
 * 
 * copyright 2012-2010
 */
package org.apache.hama.myhama.comm;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
	private MiniSuperStepCommand mssc;
	
	private ExecutorService msgHandlePool;
	private HashMap<Integer, Future<Boolean>> pushMsgResult;
	private ArrayList<Future<Boolean>> pullMsgResult;
	private ArrayList<Integer>[] pullRoute; //pull messages from these tasks.
	private AtomicInteger counter;
	private Integer pullNum = 0;
	private int localBucNum;
	
	private long io_byte = 0L, io_byte_vert, io_byte_log;
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
	
	/**
	 * Whether or not a connection error is found.
	 */
	private boolean connectionError = false;
	
	public class PushMsgDataThread implements Callable<Boolean> {
		private int srcParId;
		private InetSocketAddress srcAddr, dstAddr;
		private MsgPack<V, W, M, I> msgPack;
		
		public PushMsgDataThread(int _srcParId, int _dstParId, 
				MsgPack<V, W, M, I> _msgPack, InetSocketAddress _srcAddr, 
				InetSocketAddress _dstAddr) {
			this.srcParId = _srcParId;
			this.srcAddr = _srcAddr; 
			this.dstAddr = _dstAddr;
			this.msgPack = _msgPack;
		}
		
		public Boolean call() {
			boolean done = false;
			long _msg_rec = 0L, _msg_net = 0L, _msg_disk = 0L;
			try {
				_msg_rec = this.msgPack.getMsgRecNum();
				if (this.srcAddr.equals(this.dstAddr)) {
					_msg_disk = 
						recMsgData(this.srcParId, this.msgPack);
				} else {
					_msg_net = _msg_rec;
					CommunicationServerProtocol<V, W, M, I> comm = 
						commRT.getCommServer(this.dstAddr);
					_msg_disk = 
						comm.recMsgData(this.srcParId, this.msgPack);
				}
				updateCounters(msgPack.getIOByte(), 0L, msgPack.getIOByteOfLoggedMsg(), 
						0L, 0L, 0L, 0L, _msg_net, _msg_net, _msg_disk);
				done = true;
			} catch (UndeclaredThrowableException un) {
				LOG.warn("communication exception is caught but ignored", un);
				done = true;
				connectionError = true;
			} catch (Exception e) {
				LOG.error("fatal unknown error", e);
			}
			
			this.msgPack = null;
			
			return done;
		}
	}
	
	private class PullMsgDataThread implements Callable<Boolean> {
		private int toTaskId;
		private int toBlkId;
		private int iteNum;
		private InetSocketAddress fromAddr, toAddr;
		private boolean isOver;
		
		@SuppressWarnings("unchecked")
		public PullMsgDataThread(int _toTaskId, int _toBlkId, int _iteNum, 
				InetSocketAddress _toAddr, InetSocketAddress _fromAddr) {
			toTaskId = _toTaskId;
			toBlkId = _toBlkId;
			iteNum = _iteNum;
			toAddr = _toAddr;
			fromAddr = _fromAddr;
			isOver = false;
		}
		
		@Override
		public Boolean call() {
			boolean done = false;
			
			try {
				while (!isOver) {
					long _msg_rec = 0L, _msg_net = 0L, _msg_net_actual = 0L;
					MsgPack<V, W, M, I> recMsgPack = null;
					if (toAddr.equals(fromAddr)) {
						recMsgPack = 
							obtainMsgData(toTaskId, toBlkId, iteNum);
					} else {
						CommunicationServerProtocol<V, W, M, I> comm = 
							commRT.getCommServer(fromAddr);
						recMsgPack = 
							comm.obtainMsgData(toTaskId, toBlkId, iteNum);
						_msg_net = recMsgPack.getMsgProNum();
						_msg_net_actual = recMsgPack.getMsgRecNum();
					}
					
					_msg_rec = recMsgPack.getMsgRecNum();
					updateCounters(recMsgPack.getIOByte(), 
							recMsgPack.getIOByteOfVertInPull(), 
							recMsgPack.getIOByteOfLoggedMsg(), 
							recMsgPack.getReadEdgeNum(), 
							recMsgPack.getReadFragNum(),
							recMsgPack.getMsgProNum(), _msg_rec, 
							_msg_net, _msg_net_actual, 0L);
					/** 
					 * Use .size() instead of getMsgRecNum(), 
					 * the latter of subsequent @{link MsgPack}s is zero.
					 **/
					if (recMsgPack.size() > 0) {
						msgDataServer.putIntoBuf(
								toBlkId, iteNum, recMsgPack);
					}
					this.isOver = recMsgPack.isOver();
					
					recMsgPack = null;
				} //while
				
				done = true;
			} catch (UndeclaredThrowableException un) {
				LOG.error("communication exception is caught but ignored", un);
				done = true;
				connectionError = true;
			} catch (NullPointerException nulle) {
				LOG.error("fatal null pointer error", nulle);
			} catch (Exception e) {
				LOG.error("fatal unknown error", e);
			} finally {
				pullOver();
			}
			
			return done;
		}
	}
	
	public CommunicationServer (BSPJob job, int parId, TaskAttemptID taskId, int port) 
			throws Exception {
		this.conf = new HamaConfiguration();
		this.parId = parId;
		taskNum = job.getNumBspTask();
		this.msgHandlePool = Executors.newFixedThreadPool(taskNum);
		this.pushMsgResult = new HashMap<Integer, Future<Boolean>>(taskNum);
		this.pullMsgResult = new ArrayList<Future<Boolean>>(taskNum);
		LOG.info("start msg handle threads: " + taskNum);
		
		this.commRT = new CommRouteTable<V, W, M, I>(job, this.parId);
		this.bindAddr = job.get("host");
		this.serverPort = conf.getInt(Constants.PEER_PORT, 
				Constants.DEFAULT_PEER_PORT) + port;
		this.peerAddr = new InetSocketAddress(this.bindAddr, this.serverPort);
		
		this.server = RPC.getServer(this, 
				this.peerAddr.getHostName(), this.peerAddr.getPort(), this.conf);
		this.server.start();
		LOG.info(this.peerAddr.getHostName() + ":" + this.peerAddr.getPort());
		
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
	 * Push messages to target vertices. If target vertices reside in the task where 
	 * messages are produced, messages are directly put into the receiving buffer. 
	 * Otherwise, messages are sent via RPC. When recovering failures, surviving tasks 
	 * only allow messages sent to restart tasks to be transmitted. 
	 * @param msgData outgoing messages
	 * @param filters ids of failed tasks for filterring messages
	 * @param flag true->filter messages
	 */
	public void pushMsgData(int vid, MsgRecord<M>[] msgData, HashSet<Integer> filters, boolean flag) 
			throws Exception {
		int dstVid, dstPid, pro_msg = msgData.length;
		updateCounters(0L, 0L, 0L, 0L, 0L, pro_msg, pro_msg, 0L, 0L, 0L);
		
		for(int idx = 0; idx < pro_msg; idx++) {
			dstVid = msgData[idx].getDstVerId();
			dstPid = commRT.getDstTaskId(dstVid);
			if (flag) {
				if (!filters.contains(dstPid)) {
					continue;
				}
			}
			
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
				startPushMsgDataThread(dstPid, dstAddress, msgPack);
				msgPack = null;
				break;
			default : LOG.error("[sendMsgData] Fail send messages to Partition " 
					+ dstPid);
					throw new Exception("invalid BufferStatus");
			}
		}
	}
	
	/**
	 * Directly read logged messages on local disks and then push these messages 
	 * to restart tasks. Used in fault-tolerance for PUSH.
	 * @param filters set of restart task ids
	 */
	public void directAndConfinedPushMsg(HashSet<Integer> filters) 
			throws Exception {
		this.msgDataServer.prepareLoadMsgLoggedUnderPush(filters);
		boolean stop = false;
		while(!stop) {
			stop = true;
			for (Integer dstTaskId: filters) {
				MsgPack<V, W, M, I> msgPack = 
					this.msgDataServer.getMsgPackFromLog(dstTaskId);
				if (msgPack != null) {
					stop = false;
					
					InetSocketAddress dstAddress = commRT.getInetSocketAddress(dstTaskId);
					if (this.pushMsgResult.containsKey(dstTaskId)) {
						Future<Boolean> monitor = this.pushMsgResult.remove(dstTaskId);
						if (!monitor.isDone()) {
							monitor.get();
						}
						if (monitor.get() == false) {
							throw new Exception("ERROR");
						}
					}
					startPushMsgDataThread(dstTaskId, dstAddress, msgPack);
				}//confined message pushing operation
			}//one loop
		}
	}
	
	/** 
	 * Flush all remaining messages in the sendBuffer, 
	 * used in push at the end of one superstep. 
	 * */
	public void pushFlushMsgData() throws Exception {
		InetSocketAddress dstAddr;
		
		clearPushMsgResult();
		for (int dstParId = 0; dstParId < taskNum; dstParId++) {
			dstAddr = commRT.getInetSocketAddress(dstParId);
			if (this.msgDataServer.getSendBufferSize(dstParId) > 0) {
				MsgPack<V, W, M, I> pack = this.msgDataServer.getMsgPack(dstParId);
				startPushMsgDataThread(dstParId, dstAddr, pack);
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
	 * First, the system signals each essential source task to 
	 * produce messages based on source vertices.
	 * Second, the newly produced messages are sent to the requesting 
	 * task (this) in {@link MsgPack}s (Combination is applied before 
	 * sending if possible). When all messages have been received, 
	 * this function returns the runtime cost of pulling operations. 
	 * Note that messages on the requesting task will be directly put 
	 * into {@link MsgDataServer}. Otherwise, messages are transmitted 
	 * via RPC.
	 * @param _toBlkId id of local {@link VBlock} to which messages are sent
	 * @param _iteNum
	 * @return
	 * @throws Exception
	 */
	public long pullMsgFromSource(int _toBlkId, int _iteNum) 
			throws Exception {
		if (_iteNum == 1) {
			return 0L;
		}
		
		long start = System.currentTimeMillis();
		if (_toBlkId==0 || !this.msgDataServer.isAccumulated()) {
			this.pullNum = this.pullRoute[_toBlkId].size();
			for (int tid: this.pullRoute[_toBlkId]) {
				InetSocketAddress fromAddress = commRT.getInetSocketAddress(tid);
				startPullMsgDataThread(fromAddress, _toBlkId, _iteNum);
			}
		}
		
		if (this.pullNum > 0) {
			this.suspend(); //wait, until all msgs have been pulled for cur bucket.
		}
		
		for (Future<Boolean> f: this.pullMsgResult) {
			if (f.get() == false) {
				throw new Exception("ERROR");
			}
		} //check if Exception happens when pulling.
		this.pullMsgResult.clear();
		
		/**
		 * When finding connection error, pre-fetching messages 
		 * will invoke pullOver() one more time and then makes 
		 * the local computation quit the suspend() barrier abnormally.
		 */
		if (this.msgDataServer.isAccumulated() && 
				!findConnectionError()) {
			this.msgDataServer.switchPreMsgToCache();
			if ((_toBlkId+1)<this.localBucNum) {
				this.pullNum = this.pullRoute[(_toBlkId+1)].size();
				for (int tid: this.pullRoute[(_toBlkId+1)]) {
					InetSocketAddress fromAddress = commRT.getInetSocketAddress(tid);
					startPullMsgDataThread(fromAddress, (_toBlkId+1), _iteNum);
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
	 * @param _io_byte_log
	 * @param _read_edge
	 * @param _read_fragment
	 * @param _msg_pro
	 * @param _msg_rec
	 * @param _msg_net
	 * @param _msg_net_actual
	 * @param _msg_disk
	 */
	public synchronized void updateCounters(long _io_byte, 
			long _io_byte_vert, long _io_byte_log, 
			long _read_edge, long _read_fragment,
			long _msg_pro, long _msg_rec, 
			long _msg_net, long _msg_net_actual, long _msg_disk) {
		this.io_byte += _io_byte;
		this.io_byte_vert += _io_byte_vert;
		this.io_byte_log += _io_byte_log;
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
	
	public long getIOByteOfLoggedMsg() {
		return this.io_byte_log;
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
	
	public void clearBefIte(int _iteNum) {
		this.io_byte = 0L;
		this.io_byte_vert = 0L;
		this.io_byte_log = 0L;
		this.read_edge = 0L;
		this.read_fragment = 0L;
		this.msg_pro = 0L;
		this.msg_rec = 0L;
		this.msg_net = 0L;
		this.msg_net_actual = 0L;
		this.msg_disk = 0L;
		
		this.connectionError = false;
	}
	
	/**
	 * Is there any connection error at the current iteration?
	 * If yes, then some failures happen on other tasks. In this 
	 * case, this task can skipp remaining local computations 
	 * and directly enter the barrier, since wokloads at the 
	 * current iteration will be ignored when recovering failures.
	 * @return
	 */
	public boolean findConnectionError() {
		return this.connectionError;
	}
	
	private void startPushMsgDataThread(int dstParId, InetSocketAddress dstAddr, 
			MsgPack<V, W, M, I> msgPack) {
		Future<Boolean> future = this.msgHandlePool.submit(
				new PushMsgDataThread(parId, dstParId, 
						msgPack, peerAddr, dstAddr));
		this.pushMsgResult.put(dstParId, future);
	}
	
	private void startPullMsgDataThread(InetSocketAddress _fromAddr, 
			int _toBlkId, int _iteNum) {
		Future<Boolean> future =
			this.msgHandlePool.submit(new PullMsgDataThread(parId, _toBlkId, 
					_iteNum, peerAddr, _fromAddr));
		this.pullMsgResult.add(future);
	}
	
	public void pullOver() {
		int cur = this.counter.incrementAndGet();
		//LOG.info("counter=" + this.counter.get() + ", pullNum=" + this.pullNum);
		if (cur == this.pullNum) {
			this.counter.set(0);
			//LOG.info("pullOver");
			quit();
		}
	}
	
	@Override
	public long recMsgData(int srcParId, MsgPack<V, W, M, I> pack) {
		if (pack.size() > 0) {
			long result = this.msgDataServer.recMsgData(srcParId, pack);
			pack = null;
			return result;
		} else {
			return 0;
		}
	}
	
	@Override
	public MsgPack<V, W, M, I> obtainMsgData(int _toTaskId, int _toBlkId, int _iteNum) {
		return this.graphDataServer.getMsg(_toTaskId, _toBlkId, _iteNum);
	}
	
	@Override
	public void setRouteTable(JobInformation jobInfo) {
		this.commRT.initialilze(jobInfo);
		//LOG.info("setRouteTable");
		quit();
	}
	
	@Override
	public void updateRouteTable(JobInformation jobInfo) {
		this.commRT.initialilze(jobInfo);
	}
	
	@Override
	public void setRegisterInfo(JobInformation _jobInfo) {
		this.commRT.resetJobInformation(_jobInfo);
		//LOG.info("setRegisterInfo");
		quit();
	}
	
	@Override
	public void setNextSuperStepCommand(SuperStepCommand _ssc) {
		this.ssc = _ssc;
		this.pullRoute = this.ssc.getRealRoute();
		//LOG.info("setNextSuperStepCommand");
		quit();
	}
	
	@Override
	public void setNextMiniSuperStepCommand(MiniSuperStepCommand _mssc) {
		this.mssc = _mssc;
		quit();
	}
	
	/**
	 * Suspend computing thread until notified by someone else, 
	 * like functions in {@link JobInProgress} or this.pullMsgFromSource().
	 * @throws Exception
	 */
	public void suspend() throws Exception {
		//LOG.info("enter...");
		synchronized(mutex) {
			if (!this.hasNotify) {
				mutex.wait();
			}
			
			this.hasNotify = false;
		}
		//LOG.info("leave...");
	}
	
	@Override
	public void quit() {
		//LOG.info("quit()");
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
	
	public MiniSuperStepCommand getNextMiniSuperStepCommand() {
		return this.mssc;
	}
	
	public final JobInformation getJobInformation() {
		return this.commRT.getJobInformation();
	}
}
