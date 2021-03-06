/**
 * Termite System
 * 
 * copyright 2012-2010
 */
package org.apache.hama.myhama.graph;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.Constants.BufferStatus;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.api.UserTool;
import org.apache.hama.myhama.comm.MsgPack;
import org.apache.hama.myhama.util.LocalFileOperation;

/**
 * MsgDataServer.
 * 
 * @author 
 * @version 0.1
 */
public class MsgDataServer<V, W, M, I> {
	private static final Log LOG = LogFactory.getLog(MsgDataServer.class);
	
	private class MemoryUsage {
		private long[] usedBySendBuf;   //length = #tasks
		private long[] usedByIncomedBuf;  //length = #tasks*#local_buckets
		private long[] usedByIncomingBuf; //length = #tasks*#local_buckets
		private long usedByCache;
		private long usedByPreCache;
		
		/**
		 * Constructor, 
		 * should be invoked after taskNum and locBucNum are available.
		 */
		public MemoryUsage() {
			this.usedBySendBuf = new long[taskNum];
			this.usedByIncomedBuf = new long[taskNum*locBucNum];
			this.usedByIncomingBuf = new long[taskNum*locBucNum];
		}
		
		public void clear() {
			Arrays.fill(this.usedBySendBuf, 0L);
			Arrays.fill(this.usedByIncomedBuf, 0L);
			Arrays.fill(this.usedByIncomingBuf, 0L);
			this.usedByCache = 0L;
			this.usedByPreCache = 0L;
		}
		
		/**
		 * Return the total memory usage, 
		 * including incomingBuf, incomedBuf, cache, and pre_cache. 
		 * Here, we ignore the size of sendBuffer used by Push.
		 * @return
		 */
		public long size() {
			long counter = 0;
			/*for (long mem: this.usedBySendBuf) {
				counter += mem;
			}*/
			for (long mem: this.usedByIncomedBuf) {
				counter += mem;
			}
			for (long mem: this.usedByIncomingBuf) {
				counter += mem;
			}
			return (counter+this.usedByCache+this.usedByPreCache);
		}
		
		public void updateSendBuf(int tid, long mem) {
			this.usedBySendBuf[tid] = Math.max(this.usedBySendBuf[tid], mem);
		}
		
		public void updateIncomedBuf(int srcParBucId, long mem) {
			this.usedByIncomedBuf[srcParBucId] = 
				Math.max(this.usedByIncomedBuf[srcParBucId], mem);
		}
		
		public void updateIncomingBuf(int srcParBucId, long mem) {
			this.usedByIncomingBuf[srcParBucId] = 
				Math.max(this.usedByIncomingBuf[srcParBucId], mem);
		}
		
		public void updateCache(long mem) {
			this.usedByCache = Math.max(this.usedByCache, mem);
		}
		
		public void updatePreCache(long mem) {
			this.usedByPreCache = Math.max(this.usedByPreCache, mem);
		}
	}
	
	private BSPJob jobConf;
	private Constants.STYLE bspStyle;
	private boolean isAccumulated;
	
	private UserTool<V, W, M, I> userTool;
	private int[] verMinIds; //len is bucNum
	
	/** used by push and pull */
	private MsgRecord<M>[] cache; //capacity is bucketLen
	private long msgNum;
	private long cacheMem;
	
	/** used in push or pull&accumulated, 
	 * pre-fetch messages from local memory/disk or remote source-tasks **/
	private MsgRecord<M>[] pre_cache; //capacity is bucketLen
	private long pre_msgNum;
	private long pre_cacheMem;
	
	/** used in push */
	private int parId = -1;
	private int taskNum = 0;
	private int MESSAGE_SEND_BUFFER_THRESHOLD;
	private int MESSAGE_RECEIVE_BUFFER_THRESHOLD;
	private ArrayList<MsgRecord<M>>[] sendBuffer; //[DstPartitionId]: msgs
	private int[] packageVersion; //for logging outgoing messages (fault-tolerance)
	
	/**                                                    _ 
	 *              msgDataDir        => Root Dir           |     
	 *              /        \                              |
	 *        Archived      Received  => PUSH, PULL         |>global variables
	 *        /      \       /    \                         | 
	 *     ss-0     ss-1       ...    => SuperStep Dir     _|
	 *     /  \      / \               (comed/ing/outgoing)_ 
	 * blk-0  blk-1  ...              => Block Dir          |
	 *   / \   /  \                                         |>local variables
	 *   ...  f-0 f-1                 => Data File         _|
	 *                                                        
	 */
	/** root directory of messages */
	private File msgDataDir;
	/** sub-directory of archived messages (fault-tolerance) */
	private File msgDataArchivedDir;
	/** sub-direcotry of received messages under PUSH */
	private File msgDataReceivedDir;
	
	/** messages received from the previous iteration under PUSH */
	private File msgDataPreIteIncomedDir;
	/** messages being received at the current iteration under PUSH */
	private File msgDataCurIteIncomingDir;
	/** messages being sent at the current iteration (fault-tolerance) */
	private File msgDataCurIteOutgoingDir;
	/** messages being sent at the current iteration (fault-tolerance) */
	private File msgDataCurIteOutgoingPushDir;
	/** messages being pulled at the current iteration (fault-tolerance) */
	private File msgDataCurIteOutgoingPullDir;
	
	private int locVerMinId = -1;
	private int locBucLen = -1;
	private int locBucNum = -1;
	private boolean[][] locBucHitFlags; //[srcParId]: local_bucket_ids
	
	private MsgRecord<M>[][] incomingBuffer; //[SrcParBucId]: <dstId, msgValue>
	private MsgRecord<M>[][] incomedBuffer; //[SrcParBucId]: <dstId, msgValue>
	private int[] incomingBufLen; //[SrcParBucId]: the length of incomingBuffer
	private long[] incomingBufByte;
	
	private ExecutorService locMemPullExecutor;
	private Future<Boolean> locMemPullResult;
	private ExecutorService locDiskPullExecutor;
	private ArrayList<Future<Boolean>> locDiskPullResult;
	
	private long io_byte;
	private MemoryUsage memUsage;
	
	private LocalFileOperation localFileOpt;
	
	public MsgDataServer() {
		
	}
	
	public void init(BSPJob job, int _bucLen, int _bucNum, int[] _verMinIds, 
			int _parId, String _rootDir, boolean miniSuperStep) {
		jobConf = job;
		parId = _parId;
		bspStyle = job.getBspStyle();
		taskNum = job.getNumBspTask();
		packageVersion = new int[taskNum];
		
		localFileOpt = new LocalFileOperation();
		cache = (MsgRecord<M>[]) new MsgRecord[_bucLen];
		verMinIds = _verMinIds;
		locBucLen = _bucLen;
		locBucNum = _bucNum;
		memUsage = new MemoryUsage();
		
		userTool = 
	    	(UserTool<V, W, M, I>) 
	    	ReflectionUtils.newInstance(job.getConf().getClass(
	    		Constants.USER_JOB_TOOL_CLASS, UserTool.class), job.getConf());
		this.isAccumulated = userTool.isAccumulated();
		for (int i = 0; i < _bucLen; i++) {
			cache[i] = userTool.getMsgRecord();
		}
		this.msgNum = 0L;
		this.io_byte = 0L;
		
		createMsgDir(new File(_rootDir));
		
		/** used in push or pull/hybrid&accumulated **/
		if (this.bspStyle!=Constants.STYLE.PULL 
				|| this.isAccumulated) {
			this.pre_cache = (MsgRecord<M>[]) new MsgRecord[this.locBucLen];
			this.pre_msgNum = 0L;
			for (int i = 0; i < _bucLen; i++) {
				this.pre_cache[i] = userTool.getMsgRecord();
			}
		}
		
		/** used in push or hybrid of push&pull */
		if ((this.bspStyle!=Constants.STYLE.PULL) || miniSuperStep) {
			MESSAGE_SEND_BUFFER_THRESHOLD = job.getMsgSendBufSize(); 
			MESSAGE_RECEIVE_BUFFER_THRESHOLD = 1 + 
				job.getMsgRecBufSize() / (this.taskNum*this.locBucNum);
			StringBuffer sb = new StringBuffer("\ninitialize parameters used in PUSH:");
			sb.append("\nsend_buffer=");
			sb.append(MESSAGE_SEND_BUFFER_THRESHOLD);
			sb.append(" (#, per dstination task)");
			sb.append("\nreceive_buffer=");
			sb.append(MESSAGE_RECEIVE_BUFFER_THRESHOLD);
			sb.append(" (#, per (#tasks*#local_buckets)) and total_buffer=");
			sb.append(job.getMsgRecBufSize());
			sb.append(" (#, per destination task)");
			LOG.info(sb.toString());
			
			/** used in push: send messages */
			this.sendBuffer = new ArrayList[this.taskNum];
			for (int index = 0; index < this.taskNum; index++) {
				this.sendBuffer[index] = new ArrayList<MsgRecord<M>>();
			}
			
			this.locVerMinId = verMinIds[0];
			this.locBucHitFlags = new boolean[this.taskNum][this.locBucNum];
			for (int tid = 0; tid < this.taskNum; tid++) {
				Arrays.fill(this.locBucHitFlags[tid], false);
			}
			
			/** used in push: receive messages */
			int length = this.taskNum * this.locBucNum;
		    this.incomingBuffer = (MsgRecord<M>[][]) new MsgRecord[length][];
		    this.incomedBuffer = (MsgRecord<M>[][])new MsgRecord[length][];
		    this.incomingBufLen = new int[length];
		    this.incomingBufByte = new long[length];
		    
		    /** used in push: pull messages from local memory and disk */
		    this.locMemPullExecutor = Executors.newSingleThreadExecutor();
			this.locDiskPullExecutor = 
				Executors.newFixedThreadPool(this.taskNum);
			this.locDiskPullResult = new ArrayList<Future<Boolean>>();
		}
	}
	
	private void createMsgDir(File rootDir) {
		msgDataDir = new File(rootDir, Constants.Msg_Dir);
		if (msgDataDir.exists()) {
			localFileOpt.deleteDir(msgDataDir);
		}
		msgDataDir.mkdir();
		msgDataArchivedDir = new File(msgDataDir, Constants.Msg_Arch_Dir);
		msgDataArchivedDir.mkdirs();
		msgDataReceivedDir = new File(msgDataDir, Constants.Msg_Rec_Dir);
		msgDataReceivedDir.mkdirs();
	}
	
	/**
	 * Create a directory to store messages at the given iteration, 
	 * and then return the directory. This function is called only 
	 * by the main thread. When logging outgoing messages or getting 
	 * incomed message directory, existing directory should be preserved.
	 * @param dir
	 * @param iteNum
	 * @return directory File
	 */
	private File getMsgDirSuperStep(File dir, int iteNum) {
		File f = new File(dir, "ss-" + iteNum);
		f.mkdirs();
		return f;
	}
	
	private File getMsgDirSuperStepStyle(File dir, Constants.STYLE style) {
		File f = new File(dir, style.toString());
		f.mkdirs();
		return f;
	}
	
	/**
	 * Create the message block directory and then return it. For PUSH, 
	 * the block id is the {@link VBlock} id, and for PULL, it is the 
	 * global id given by 
	 * {@link JobInformation}.getGlobalBlkIdx(int _dstTid, int _dstBid). 
	 * This function may be called concurrently by multiple threads under 
	 * PUSH because many source tasks may send messages to a specific 
	 * {@link VBlock}. However it is thread-safe under PULL since messages 
	 * for a specific {@link VBlock} are fetched in a serialized manner. 
	 * @param dir
	 * @param bid Message block id.
	 * @param version
	 * @return
	 */
	private synchronized File getMsgDirBlock(File dir, int blkId) { 
		File f = new File(dir, "blk-" + blkId);
		if (!f.exists()) {
			f.mkdirs();
		}
		return f;
	}
	
	/**
	 * For PUSH, <code>id</code> indicates which source task sends messages. 
	 * For PULL, it indicates the "id"-th package of messages generated for 
	 * requesting {@link VBlock}. One package with respect to a disk file, 
	 * will be transmitted as a unit. This function is thread-safe.
	 * @param dir
	 * @param id
	 * @return
	 */
	private File getMsgDataFile(File dir, int id) {
		return new File(dir, "file-" + id);
	}
	
	public synchronized void addMsgNum(long _msgNum) {
		this.msgNum += _msgNum;
	}
	
	public synchronized void addPreMsgNum(long _locMsgNum) {
		this.pre_msgNum += _locMsgNum;
	}
	
	public synchronized void addIOByte(long _io) {
		this.io_byte += _io;
	}
	
	public boolean isAccumulated() {
		return this.isAccumulated;
	}
	
	//===============================================================
	//       Used for push: manage sending and receiving messages
	//===============================================================
	/** Put messages into the sendBuffer and return the status of buffer */
	public BufferStatus putIntoSendBuffer(int dstPid, MsgRecord<M> msg) {
		this.sendBuffer[dstPid].add(msg);
		
		if (this.sendBuffer[dstPid].size() 
		                       >= this.MESSAGE_SEND_BUFFER_THRESHOLD) {
			return BufferStatus.OVERFLOW;
		} else {
			return BufferStatus.NORMAL;
		}
	}
	
	/**
	 * Get one {@link MsgPack} when recovering failures under PUSH.
	 * @param dstTaskId
	 * @return
	 * @throws Exception
	 */
	public MsgPack<V, W, M, I> getMsgPackFromLog(int dstTaskId) throws Exception {
		if (packageVersion[dstTaskId] == 0) {
			return null;
		}
		
		MsgPack<V, W, M, I> msgPack = new MsgPack<V, W, M, I>(userTool);
		//io, edge_read, fragment_read, msg_pro, msg_rec, dstVerHasMsg, io_vert.
		long[] statis = new long[7];
		ByteArrayOutputStream messages = 
			new ByteArrayOutputStream(this.MESSAGE_SEND_BUFFER_THRESHOLD);
		int vCounter = loadOutgoingMsg(messages, dstTaskId, packageVersion[dstTaskId]-1, 
					statis, Constants.STYLE.PUSH);
		msgPack.setEdgeInfo(statis[0], 0L, 0L, 0L);
		msgPack.setRemote(messages, vCounter, 0L, vCounter, 0L);
		packageVersion[dstTaskId]--;
		
		messages = null;
		statis = null;
		
		return msgPack;
	}
	
	/** Get one {@link MsgPack} and then clear the related buffer under PUSH */
	public MsgPack<V, W, M, I> getMsgPack(int dstPid) throws Exception {
		MsgPack<V, W, M, I> msgPack = 
			new MsgPack<V, W, M, I>(userTool); //message pack
		int counter = 0;
		long mem = 0L;
		boolean logMsg = false;
		long loggedBytes = 0L;
		if (jobConf.getCheckPointPolicy() 
				== Constants.CheckPoint.Policy.ConfinedRecoveryLogMsg) {
			logMsg = true;
		}
		
		if (this.parId == dstPid) {
			MsgRecord<M>[] msgData = 
				(MsgRecord<M>[]) new MsgRecord[this.sendBuffer[dstPid].size()];
			for (MsgRecord<M> msg: this.sendBuffer[dstPid]) {
				msgData[counter++] = msg;
				mem += msg.getMsgByte();
			}
			
			if (logMsg) {
				ByteArrayOutputStream bytes = 
					new ByteArrayOutputStream(this.sendBuffer[dstPid].size());
				DataOutputStream stream = new DataOutputStream(bytes);
				for (MsgRecord<M> msg: this.sendBuffer[dstPid]) {
					msg.serialize(stream);
				}
				stream.close();
				bytes.close();
				packageVersion[dstPid]++;
				int version = packageVersion[dstPid] - 1; //starting from zero
				//io, edge_read, fragment_read, msg_pro, msg_rec, dstVerHasMsg, io_vert.
				long[] statis = new long[7];
				statis[5] = counter;
				loggedBytes = 
					logOutgoingMsg(bytes, dstPid, version, statis, counter, 
							Constants.STYLE.PUSH);
				
				bytes = null;
				statis = null;
			}
			
			msgPack.setEdgeInfo(0L, 0L, 0L, 0L);
			msgPack.setLocal(msgData, counter, 0L, counter, loggedBytes);
			
			msgData = null;
		} else {
			ByteArrayOutputStream bytes = 
				new ByteArrayOutputStream(this.sendBuffer[dstPid].size());
			DataOutputStream stream = new DataOutputStream(bytes);
			for (MsgRecord<M> msg: this.sendBuffer[dstPid]) {
				msg.serialize(stream);
				counter++;
			}
			stream.close();
			bytes.close();
			mem += stream.size();
			
			if (logMsg) {
				packageVersion[dstPid]++;
				int version = packageVersion[dstPid] - 1;//starting from zero
				//io, edge_read, fragment_read, msg_pro, msg_rec, dstVerHasMsg, io_vert.
				long[] statis = new long[7];
				statis[5] = counter;
				loggedBytes = 
					logOutgoingMsg(bytes, dstPid, version, statis, counter, 
							Constants.STYLE.PUSH);
				
				statis = null;
			}
			
			msgPack.setEdgeInfo(0L, 0L, 0L, 0L);
			msgPack.setRemote(bytes, counter, 0L, counter, loggedBytes);
			
			bytes = null;
		}
		
		this.sendBuffer[dstPid].clear();
		this.memUsage.updateSendBuf(dstPid, mem);
		
		return msgPack;
	}
	
	/** Get the len of a given buffer */
	public long getSendBufferSize(int dstParId) {			
		return this.sendBuffer[dstParId].size();
	}
	
	/** Clear buffer and variables at the end of one iteration */
	public void clearSendBuffer() {
		for (int idx = 0; idx < taskNum; idx++) {
			sendBuffer[idx].clear();
			packageVersion[idx] = 0;
		}
	}
	
	/** 
	 *  Receive messages, used in push.
	 *  Store them in incomingBuffer first, 
	 *  and spill the buffer targeted to one bucket 
	 *  onto disk if it is overflow.
	 *  
	 * @param srcParId
	 * @param pack
	 * 
	 * @return #messages on disk, or -1 if any exception happens.
	 */
	public long recMsgData(int srcParId, MsgPack<V, W, M, I> pack) {
		int bid = -1, pbid = -1;
		long msgCountOnDisk = 0L;
		
		try {
			pack.setUserTool(this.userTool);
			for (MsgRecord<M> msg: pack.get()) {
				bid = (msg.getDstVerId()-this.locVerMinId) / this.locBucLen;
				pbid = srcParId * this.locBucNum + bid;
				if (!this.locBucHitFlags[srcParId][bid]) {
					this.locBucHitFlags[srcParId][bid] = true;
				}
				
				this.incomingBuffer[pbid][this.incomingBufLen[pbid]] = msg;
				this.incomingBufLen[pbid]++;
				this.incomingBufByte[pbid] += msg.getMsgByte();
				
				if (this.incomingBufLen[pbid] >= MESSAGE_RECEIVE_BUFFER_THRESHOLD) {
					File dir = this.getMsgDirBlock(msgDataCurIteIncomingDir, bid);
					msgCountOnDisk = this.incomingBufLen[pbid];
					
					spillReceivedMsgToDisk(dir, srcParId, this.incomingBuffer[pbid], 
							this.incomingBufLen[pbid], this.incomingBufByte[pbid]);
					this.memUsage.updateIncomingBuf(pbid, this.incomingBufByte[pbid]);
					
					this.incomingBuffer[pbid] = null; 
					
					this.incomingBuffer[pbid] = 
						(MsgRecord<M>[]) new MsgRecord[MESSAGE_RECEIVE_BUFFER_THRESHOLD];
					this.incomingBufLen[pbid] = 0;
					this.incomingBufByte[pbid] = 0;
				}
			}
			
			pack = null;
			return msgCountOnDisk;
		} catch (Exception e) {
			return -1;
		}
	}
	
	/**
	 * Log outgoing messages onto local disks. For PULL, this function 
	 * is called by {@link GraphDataServer}.getMsg(). For PUSH, it is 
	 * called by {@link MsgDataServer}.xx().
	 * @param messages
	 * @param globalBlkIdx
	 * @param version starting from zero
	 * @return long bytes
	 * @throws Exception
	 */
	public long logOutgoingMsg(ByteArrayOutputStream messages, int globalBlkIdx, int version, 
			long[] statistics, int vCounter, Constants.STYLE style) throws IOException {
		long result = 0L;
		File dir = null; 
		if (style == Constants.STYLE.PULL) {
			dir = getMsgDirBlock(msgDataCurIteOutgoingPullDir, globalBlkIdx);
		} else {
			dir = getMsgDirBlock(msgDataCurIteOutgoingPushDir, globalBlkIdx);
		}
		File dataFile = getMsgDataFile(dir, version);
		if (dataFile.exists()) {
			dataFile.delete();
		}
		//LOG.info("log msg: " + dataFile);
		RandomAccessFile raf = new RandomAccessFile(dataFile, "rw");
		FileChannel fc = raf.getChannel();
		//length of messages in bytes, vCounter
		int sizeOfBuffer = messages.size() + 4*2;
		if (version == 0) {
			//length of the statistic array and the long array values
			sizeOfBuffer += (4 + 8*statistics.length);
		}
		MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_WRITE, 0L, 
				sizeOfBuffer);
		if (version == 0) {
			mbb.putInt(statistics.length);
			for (long val: statistics) {
				mbb.putLong(val);
			} //statistics are logged only once
		}
		
		byte[] writeBytes = messages.toByteArray();
		mbb.putInt(vCounter);
		mbb.putInt(writeBytes.length);
		mbb.put(writeBytes);
		result = fc.size();
		fc.close();
		raf.close(); 
		
		messages = null;
		statistics = null;
		
		return result;
	}
	
	public int getNumberOfMsgPacks(int globalBlkIdx, Constants.STYLE style) {
		File dir = null; 
		if (style == Constants.STYLE.PULL) {
			dir = getMsgDirBlock(msgDataCurIteOutgoingPullDir, globalBlkIdx);
		} else {
			dir = getMsgDirBlock(msgDataCurIteOutgoingPushDir, globalBlkIdx);
		}
		
		if (dir.exists()) {
			return dir.listFiles().length;
		} else {
			return 0;
		}
	}
	
	/**
	 * Prepare to load outgoing messages logged under PUSH. 
	 * Here, only packageVersion[] is initialized. 
	 * @param filters set of failed tasks
	 */
	public void prepareLoadMsgLoggedUnderPush(HashSet<Integer> filters) {
		for (Integer dstTaskId: filters) {
			packageVersion[dstTaskId] = 
				getNumberOfMsgPacks(dstTaskId, Constants.STYLE.PUSH);
		}
	}
	
	public void clearLoggedMsg(int iteNum) {
		File dir = getMsgDirSuperStep(msgDataArchivedDir, iteNum);
		localFileOpt.deleteDir(dir);
	}
	
	/**
	 * Load logged outgoing messages. Messages are put into the 
	 * parameter "messages" and statistics will be returned only 
	 * when "version" is zero. In particular, statistics[0] indicates 
	 * the bytes of logged messages, instead of the logged value of statistics[0].
	 * @param messages
	 * @param globalBlkIdx
	 * @param version
	 * @param statistics initialized if version is zero, do nothing otherwise
	 * @return vCounter int
	 * @throws Exception
	 */
	public int loadOutgoingMsg(ByteArrayOutputStream messages, int globalBlkIdx, 
			int version, long[] statis, Constants.STYLE style) throws IOException {
		File dir = null;
		if (style == Constants.STYLE.PULL) {
			dir = getMsgDirBlock(msgDataCurIteOutgoingPullDir, globalBlkIdx);
		} else {
			dir = getMsgDirBlock(msgDataCurIteOutgoingPushDir, globalBlkIdx);
		}
		File dataFile = getMsgDataFile(dir, version);
		RandomAccessFile raf = new RandomAccessFile(dataFile, "r");
		FileChannel fc = raf.getChannel();
		MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_ONLY, 0L, 
				dataFile.length());
		if (version == 0) {
			int length = mbb.getInt();;
			for (int i = 0; i < length; i++) {
				statis[i] = mbb.getLong();
			}
			statis[0] = 0L;
		}
		statis[0] += fc.size();
		
		int vCounter = mbb.getInt();
		byte[] readBytes = new byte[mbb.getInt()];
		mbb.get(readBytes);
		messages.write(readBytes);
		
		fc.close();
		raf.close();
		
		readBytes = null;
		
		return vCounter;
	}
	
	/** 
	 * Spill received messages from source task with id as srcParId to some 
	 * {@link VBlock} (indicated by "dir") onto disk, used in Push. This function 
	 * is thread-safe because {@link MsgPack}s from a specific source task to the
	 * specific {@link VBlock} are transmitted in a serialized manner.
	 * */
	private boolean spillReceivedMsgToDisk(File dir, int srcParId, 
			MsgRecord<M>[] messages, int length, long bytes) 
			throws FileNotFoundException, IOException {		
		File dataFile = getMsgDataFile(dir, srcParId);
		dataFile.createNewFile();
		
		RandomAccessFile ra = new RandomAccessFile(dataFile, "rw");
		FileChannel fc = ra.getChannel();
		MappedByteBuffer mbb = 
			fc.map(FileChannel.MapMode.READ_WRITE, ra.length(), bytes);
		
		for (int i = 0; i < length; i++) {
			messages[i].serialize(mbb);
		}
		
		fc.close();	ra.close();
		
		messages = null;
		
		return true;
	}
	
	/** Switch messages pushed from the previous iteration into Incomed buffer, 
	 * to be processed. And then clear Incoming buffer. */
	private void switchIncomingToIncomed() {
		int length = this.taskNum * this.locBucNum;
		for (int pbid = 0; pbid < length; pbid++) {
			this.incomedBuffer[pbid] = null;
			if (this.incomingBufLen[pbid] > 0) {
				this.incomedBuffer[pbid] = 
					(MsgRecord<M>[]) new MsgRecord[this.incomingBufLen[pbid]];
				for (int i = 0; i < this.incomingBufLen[pbid]; i++) {
					this.incomedBuffer[pbid][i] = this.incomingBuffer[pbid][i];
				}
				this.memUsage.updateIncomingBuf(pbid, this.incomingBufByte[pbid]);
				this.memUsage.updateIncomedBuf(pbid, this.incomingBufByte[pbid]);
			} 
			this.incomingBuffer[pbid] = null;
			this.incomingBufLen[pbid] = 0;
			this.incomingBufByte[pbid] = 0;
		}
	}
	
	/** Switch messages pre-fetch from local memory/disks 
	 * or remote source-tasks into cache, to be used when updating vertices.
	 * Single-thread.
	 */
	public void switchPreMsgToCache() {
		if (this.pre_msgNum == 0L) {
			return;
		} else {
			this.msgNum = this.pre_msgNum;
		}
		
		for (int i = 0; i < this.locBucLen; i++) {
			if (this.pre_cache[i].isValid()) {
				this.cache[i].collect(this.pre_cache[i]);
				this.pre_cacheMem += this.pre_cache[i].getMsgByte();
				this.pre_cache[i].reset();
			}
		}
		
		this.pre_msgNum = 0L;
		this.memUsage.updatePreCache(this.pre_cacheMem);
		this.pre_cacheMem = 0L;
	}
	
	/**
	 * Pull messages from local memory and disk, used in push style.
	 * In the push style, all messages have been pushed 
	 * to the local memory and disk in the previous iteration.
	 * 
	 * @param bid
	 * @param iteNum
	 * @return
	 */
	public long pullMsgFromLocal(int bid, int iteNum) throws Exception {
		if (iteNum == 1) {
			return 0; //skip iteNum=1, since no messages are received
		}
		long start = System.currentTimeMillis();
		if (bid == 0) { //for the first bucket, start threads to prepare messages
			File msgDir = getMsgDirBlock(msgDataPreIteIncomedDir, bid);
			
			this.locMemPullResult = 
				this.locMemPullExecutor.submit(
						new LocalMemPullThread(bid, this.verMinIds[bid]));
		
			if (msgDir.exists() && msgDir.isDirectory()) {
				for (File fileName : msgDir.listFiles()) {
					if (fileName.toString().endsWith("~")) {
						continue;
					}
					this.locDiskPullResult.add(this.locDiskPullExecutor.submit(
							new LocalDiskPullThread(this.verMinIds[bid], fileName)));
				}
			}
		}
		
		/** wait until threads are done, to get messages targeted to the requested bucket */
		if (this.locMemPullResult.get() == false) {
			throw new Exception("Error when reading messages from local memory!");
		}
		for (Future<Boolean> f: this.locDiskPullResult) {
			if (!f.isDone()) {
				f.get();
			}
			if (f.get() == false) {
				throw new Exception("Error when reading messages from local disks!");
			}
		}
		this.locDiskPullResult.clear();
		//put messages into cache, to be accessed when updating vertices
		switchPreMsgToCache();

		/** start threads to prepare messages for the next bucket asynchronously */
		if ((bid+1) < this.locBucNum) {
			File msgDir = getMsgDirBlock(msgDataPreIteIncomedDir, (bid+1));
			
			this.locMemPullResult = 
				this.locMemPullExecutor.submit(
						new LocalMemPullThread(bid+1, this.verMinIds[bid+1]));
		
			if (msgDir.exists() && msgDir.isDirectory()) {
				for (File fileName : msgDir.listFiles()) {
					if (fileName.toString().endsWith("~")) {
						continue;
					}
					this.locDiskPullResult.add(this.locDiskPullExecutor.submit(
							new LocalDiskPullThread(this.verMinIds[bid+1], fileName)));
				}
			}
		} //pre-pulling messages for the next bucket
		return (System.currentTimeMillis()-start);
	}
	
	/** 
	 * Return io_byte when writing and reading messages from local disks.
	 * It makes sense in style.Push. 
	 * */
	public long getLocMsgIOByte() {
		return this.io_byte;
	}
	
	//==============================
	//  Used for Pull
	//==============================
	/** 
	 * Put messages pulled from source vertices into message cache.
	 * Used in Pull.
	 * Invoked by multiple-threads.
	 * @param _bid
	 * @param _iteNum
	 * @param recMsgPack
	 * @throws Exception
	 */
	public boolean putIntoBuf(int _bid, int _iteNum, 
			MsgPack<V, W, M, I> recMsgPack) {
		try {
			recMsgPack.setUserTool(userTool);
			if (this.isAccumulated) {
				this.addPreMsgNum(recMsgPack.getMsgRecNum());
				int index = 0;
				for (MsgRecord<M> msg: recMsgPack.get()) {
					index = msg.getDstVerId() - verMinIds[_bid];
					/** Lock for each target vertex, 
					 * different ones may be processed at the same time */
					this.pre_cache[index].collect(msg);
				}
			} else {
				this.addMsgNum(recMsgPack.getMsgRecNum()); //synchronize method
				int index = 0;
				for (MsgRecord<M> msg: recMsgPack.get()) {
					index = msg.getDstVerId() - verMinIds[_bid];
					/** Lock for each target vertex, 
					 * different ones may be processed at the same time */
					this.cache[index].collect(msg);
				}
			}
			return true;
		} catch (Exception e) {
			return false;
		}

	}
	
	//==============================
	//  Used for computation
	//==============================
	
	/** 
	 * Prepare before running one iteration.
	 * For push, the corresponding message dir should be created.
	 * Also, the receiving buffer should be cleared and created.
	 * Single-Thread.
	 **/
	public void clearBefIte(int _iteNum, Constants.STYLE _preIteStyle, 
			Constants.STYLE _curIteStyle, boolean miniSuperStep) 
			throws Exception {
		this.io_byte = 0L;
		int cur_IteNum = _iteNum, next_IteNum = _iteNum+1;
		
		clearBefBucket();
		/** used in push or pull/hybrid&accumulated **/
		if (this.bspStyle!=Constants.STYLE.PULL 
				|| this.isAccumulated) {
			for (int i = 0; i < this.locBucLen; i++) {
				this.pre_cache[i].reset();
			}
			this.pre_msgNum = 0;
			this.pre_cacheMem = 0;
		}
		
		if ((this.bspStyle!=Constants.STYLE.PULL) || miniSuperStep) {
			clearSendBuffer();
		}
		
		/** 
		 * msgDataOutgoingDir" is initialized to archive outgoing messages 
		 * generated by this task. 
		 **/
		msgDataCurIteOutgoingDir = 
			getMsgDirSuperStep(msgDataArchivedDir, cur_IteNum);
		msgDataCurIteOutgoingPullDir = 
			getMsgDirSuperStepStyle(msgDataCurIteOutgoingDir, Constants.STYLE.PULL);
		msgDataCurIteOutgoingPushDir = 
			getMsgDirSuperStepStyle(msgDataCurIteOutgoingDir, Constants.STYLE.PUSH);
		
		/** 
		 * If _preStyle==Push, messages in "incomingBuffer" are moved into 
		 * the "incomedBuffer" variable, and also, the "msgDataIncomedDir" 
		 * variable is initialized for reading messages on local disk.
		 **/
		if (_preIteStyle == Constants.STYLE.PUSH) {
			switchIncomingToIncomed();
			msgDataPreIteIncomedDir = 
				getMsgDirSuperStep(msgDataReceivedDir, cur_IteNum);
		}
		
		/** 
		 * If _curIteStype==Push, "incomingBuffer" and "msgDataIncomingDir" are 
		 * initialized to store messages received at the current iteration but 
		 * used at the next one. Meanwhile, "locBucHitFlags" is reset to collect 
		 * runtime statistics for estimating i/o-costs if PULL were executed.
		 **/
		if ((_curIteStyle==Constants.STYLE.PUSH) || miniSuperStep) {
			int length = this.taskNum * this.locBucNum;
			for (int index = 0; index < length; index++) {
				this.incomingBuffer[index] = null;
				this.incomingBuffer[index] = 
					(MsgRecord<M>[]) new MsgRecord[MESSAGE_RECEIVE_BUFFER_THRESHOLD];
				this.incomingBufLen[index] = 0;
				this.incomingBufByte[index] = 0;
			}
			
			//preserve-flag is true to preserve received messages at the failed iteration, 
			//so that surviving tasks can easily continue failed iteration.
			msgDataCurIteIncomingDir = 
				getMsgDirSuperStep(msgDataReceivedDir, next_IteNum);
			
			for (int tid = 0; tid < this.taskNum; tid++) {
				Arrays.fill(this.locBucHitFlags[tid], false);
			}
		}
	}
	
	/**
	 * Cleanup function. 
	 * @param _iteNum
	 * @param _preIteStyle
	 * @param _curIteStyle
	 * @param isInterrupted
	 * @throws Exception
	 */
	public void clearAftIte(int _iteNum, Constants.STYLE _preIteStyle, 
			Constants.STYLE _curIteStyle, boolean isInterrupted) throws Exception {
		if (_preIteStyle == Constants.STYLE.PUSH) {
			/**
			 * If the current iteration is interrupted by failures of other tasks, we 
			 * spill received messages kept in "incomedBuffer" onto local disk. Also, 
			 * existing message disk file cannot be deleted. In this way, surviving 
			 * tasks can correctly redo the failed iteration based on messages from 
			 * the previous iteration.
			 * 
			 * If the current iteration is successfully done, we delete all message 
			 * disk files and empty "incomedBuffer" to save storage space.
			 */
			if (isInterrupted) {
				for (int srcTaskId = 0; srcTaskId < taskNum; srcTaskId++) {
					for (int bid = 0; bid < locBucNum; bid++) {
						int taskToBlkIdx = srcTaskId * locBucNum + bid;
						int numOfMsg = 0;
						long bytesOfMsg = 0L;
						if (incomedBuffer[taskToBlkIdx] != null) {
							for (MsgRecord<M> msg: incomedBuffer[taskToBlkIdx]) {
								numOfMsg++;
								bytesOfMsg += msg.getMsgByte();
							}
							File dir = this.getMsgDirBlock(msgDataPreIteIncomedDir, bid);						
							spillReceivedMsgToDisk(dir, srcTaskId, incomedBuffer[taskToBlkIdx], 
									numOfMsg, bytesOfMsg);
							incomedBuffer[taskToBlkIdx] = null;
						}
					}
				}
			} else {
				int length = taskNum * locBucNum;
				localFileOpt.deleteDir(msgDataPreIteIncomedDir);
				for (int index = 0; index < length; index++) {
					incomedBuffer[index] = null;
				}
			}
		}
		
		/**
		 * Under PUSH, all messages received at the interrupted iteration should be 
		 * deleted because they will be re-received when recovering failures. 
		 */
		if (_curIteStyle==Constants.STYLE.PUSH && isInterrupted) {
			localFileOpt.deleteDir(msgDataCurIteIncomingDir);
		}
	}
	
	/** 
	 * Clear message cache before pulling messages from local(used in Push) 
	 * or remote(used in Pull).
	 * Single-thread.
	 **/
	public void clearBefBucket() {
		for (int i = 0; i < this.cache.length; i++) {
			this.cache[i].reset();
		}
		this.msgNum = 0L;
		this.cacheMem = 0L;
	}
	
	public void clearAftBucket() {
		this.memUsage.updateCache(this.cacheMem);
	}
	
	/** Has messages targeted to the _vid? */
	public boolean hasMsg(int _bid, int _vid) {
		return this.cache[_vid-verMinIds[_bid]].isValid();
	}
	
	/** Get messages targeted to the _vid. null is returned if no messages */
	public MsgRecord<M> getMsg(int _bid, int _vid) {
		int index = _vid - verMinIds[_bid];
		if (this.cache[index].isValid()) {
			this.cacheMem += this.cache[index].getMsgByte();
			return this.cache[index];
		} else {
			return null;
		}
	}
	
	/** Return #messages targeted to the current bucket */
	public long getMsgNum() {
		return this.msgNum;
	}
	
	/**
	 * Return the total memory usage, and then clear the counters in MemoryUsage.
	 * Including incomingBuf, incomedBuf, cache, and pre_cache. 
	 * Here, we ignore the size of sendBuf used by Push.
	 * @return
	 */
	public long getAndClearMemUsage() {
		long size = this.memUsage.size();
		this.memUsage.clear();
		return size;
	}
	
	/** Close {@link MsgDataServer}, mainly close some thread pools. */
	public void close() {
		if (this.bspStyle != Constants.STYLE.PULL) {
			this.locMemPullExecutor.shutdown();
			this.locDiskPullExecutor.shutdown();
		}
	}
	
	//==========================================
	//    Local Pull Thread in Memory and Disk
	//==========================================
	/**
	 * Pull/Read messages from local memory, used in Push.
	 */
	private class LocalMemPullThread implements Callable<Boolean> {
		private int bucketId;
		private int startIndex;
		
		public LocalMemPullThread(int _bucketId, int _startIndex) {
			this.bucketId = _bucketId;
			this.startIndex = _startIndex;
		}
		
		@Override
		public Boolean call() {
			boolean flag = false;
			try {
				int length = taskNum*locBucNum, dstId, msgIndex;
				
				for (int srcPBID = this.bucketId; srcPBID < length; 
						srcPBID = srcPBID + locBucNum) {
					if (incomedBuffer[srcPBID] == null) {
						continue;
					}
					
					for (int index = 0; 
							index < incomedBuffer[srcPBID].length; index++) {
						dstId = incomedBuffer[srcPBID][index].getDstVerId();
						msgIndex = dstId - this.startIndex;
						pre_cache[msgIndex].collect(
								incomedBuffer[srcPBID][index].clone());
					}
					addPreMsgNum(incomedBuffer[srcPBID].length);
					//incomedBuffer[srcPBID] = null; //clear at clearAftIte
				}
				flag = true;
			} catch (Exception e) {
				LOG.error("errors", e);
			}
			
			return flag;
		}
	}
	
	/**
	 * Pull/Read messages from local disks, used in Push.
	 * @author root
	 *
	 */
	private class LocalDiskPullThread implements Callable<Boolean> {
		private int startIndex;
		private File fileName;
		
		public LocalDiskPullThread(int _startIndex, File _fileName) {
			this.startIndex = _startIndex;
			this.fileName = _fileName;
		}
		
		@Override
		public Boolean call() {
			boolean flag = false;
			try{
				FileChannel fc = 
					new RandomAccessFile(this.fileName, "r").getChannel();
				MappedByteBuffer mbb = 
					fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
				int dstId = -1, msgIndex = 0;
				long counter = 0L;
				
				while (mbb.hasRemaining()) {
					MsgRecord<M> message = userTool.getMsgRecord();
					message.deserialize(mbb);
					dstId = message.getDstVerId();
					msgIndex = dstId - this.startIndex;
					pre_cache[msgIndex].collect(message);
					counter++;
					message = null;
				}
				addPreMsgNum(counter);
				addIOByte(2*fc.size());
				
				fc.close();
				//delete all message files if and only if this iteration is 
				//successfully done, please refer to clearAftIte().
				//this.fileName.delete();
				flag = true;
			} catch(Exception e) {
				LOG.error("[readMsgThread]: " + this.fileName, e);
			}
			
			return flag;
		} //call()
	} //LocalDiskPullThread.
}
