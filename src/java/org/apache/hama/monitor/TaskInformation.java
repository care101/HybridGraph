package org.apache.hama.monitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TaskInformation implements Writable {
	private int taskId;
	private int verMinId, verMaxId;
	private int port;
	private String hostName;
	
	private int byteOfOneMsg = 0;
	private boolean isAccumulated = false;
	
	private int verNum = 0;
	private long edgeNum = 0L;
	private int blkLen = 0;
	private int blkNum = 0;
	private long loadByte = 0L;
	
	private JobInformation jobInfo;
	/** dependency among VBlocks with responding vertices */
	private boolean[][] resDepend;
	private int[] verNumBlks; //total source vertices of each VBlock
	private int[] resVerNumBlks; //responding source vertices of each VBlock
	
	public TaskInformation () {
		
	}
	
	public TaskInformation (int _taskId, int _minVerId, int _port, 
			String _hostName, int _byteOfOneMsg, boolean _isAccumulated) {
		taskId = _taskId;
		verMinId = _minVerId;
		port = _port;
		hostName = _hostName;
		
		this.byteOfOneMsg = _byteOfOneMsg;
		this.isAccumulated = _isAccumulated;
	}
	
	public void init(JobInformation _jobInfo) {
		this.jobInfo = _jobInfo;
		this.verMaxId = jobInfo.getVerMaxId(taskId);
		this.verNum = verMaxId - verMinId + 1;
		this.blkNum = jobInfo.getBlkNumOfTasks(taskId);
		this.blkLen = jobInfo.getBlkLenOfTasks(taskId);
		this.resDepend = new boolean[blkNum][jobInfo.getBlkNumOfJob()];
		this.verNumBlks = new int[blkNum];
		this.resVerNumBlks = new int[blkNum];
	}
	
	public int getTaskId() {
		return taskId;
	}
	
	public int getVerMinId() {
		return verMinId;
	}
	
	public int getVerMaxId() {
		return verMaxId;
	}
	
	public int getPort() {
		return port;
	}
	
	public String getHostName() {
		return hostName;
	}
	
	public int getBlkLen() {
		return this.blkLen;
	}
	
	public int getBlkNum() {
		return this.blkNum;
	}
	
	public void setVerNum(int _verNum) {
		this.verNum = _verNum;
	}
	
	public int getVerNum() {
		return this.verNum;
	}
	
	public void setVerNumBlks(int[] _array) {
		this.verNumBlks = _array;
	}
	
	public int[] getVerNumBlks() {
		return this.verNumBlks;
	}
	
	public void setRespondVerNumBlks(int[] _array) {
		this.resVerNumBlks = _array;
	}
	
	public int[] getRespondVerNumBlks() {
		return this.resVerNumBlks;
	}
	
	/**
	 * Set the value of edge number.
	 * Note that this function should be invoked after localize the graph data.
	 * @param _edgeNum
	 */
	public void setEdgeNum(long _edgeNum) {
		this.edgeNum = _edgeNum;
	}
	
	public long getEdgeNum() {
		return this.edgeNum;
	}
	
	/**
	 * Set the io_bytes of loading subgraph onto local disk.
	 * @param _byte
	 */
	public void setLoadByte(long _byte) {
		this.loadByte = _byte;
	}
	
	/**
	 * Get the io_bytes of loading subgraph onto local disk.
	 * @return
	 */
	public long getLoadByte() {
		return this.loadByte;
	}
	
	/**
	 * Update the dependency relationship among VBlocks.
	 * This function should be invoked by decompose() of {@link GraphRecord}.
	 * @param taskId
	 * @param vId
	 * @param exch
	 */
	public void updateRespondDependency(int _dstTid, int _dstBid, 
			int vid, long len) {
		int row = jobInfo.getLocalBlkIdx(this.taskId, vid);
		int col = jobInfo.getGlobalBlkIdx(_dstTid, _dstBid);
		this.resDepend[row][col] = true;
	}
	
	public boolean[][] getRespondDependency() {
		return this.resDepend;
	}
	
	public void clear() {
		this.resDepend = null;
		this.verNumBlks = null;
	}
	
	public int getByteOfOneMessage() {
		return this.byteOfOneMsg;
	}
	
	public boolean isAccumulated() {
		return this.isAccumulated;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[taskId]="); sb.append(this.taskId);
		sb.append(" #vertex="); sb.append(this.verNum);
		sb.append(" #edge="); sb.append(this.edgeNum);
		sb.append(" #bucLen="); sb.append(this.blkLen);
		sb.append(" #bucNum="); sb.append(this.blkNum);
		sb.append(" loadByte="); sb.append(this.loadByte);
		
		return sb.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		taskId = in.readInt();
		verMinId = in.readInt();
		port = in.readInt();
		hostName = Text.readString(in);
		
		this.byteOfOneMsg = in.readInt();
		this.isAccumulated = in.readBoolean();
		
		this.verNum = in.readInt();
		this.edgeNum = in.readLong();
		this.loadByte = in.readLong();
		
		int rowNum = in.readInt();
		if (rowNum != 0) {
			this.resDepend = new boolean[rowNum][];
			for (int i = 0; i < rowNum; i++) {
				int colNum = in.readInt();
				this.resDepend[i] = new boolean[colNum];
				for (int j = 0; j < colNum; j++) {
					this.resDepend[i][j] = in.readBoolean();
				}
			}
		} else {
			this.resDepend = null;
		}
		
		int num = in.readInt();
		if (num != 0) {
			this.verNumBlks = new int[num];
			for (int i = 0; i < num; i++) {
				this.verNumBlks[i] = in.readInt();
			}
		} else {
			this.verNumBlks = null;
		}
		
		num = in.readInt();
		if (num != 0) {
			this.resVerNumBlks = new int[num];
			for (int i = 0; i < num; i++) {
				this.resVerNumBlks[i] = in.readInt();
			}
		} else {
			this.resVerNumBlks = null;
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(taskId);
		out.writeInt(verMinId);
		out.writeInt(port);
		Text.writeString(out, hostName);
		
		out.writeInt(this.byteOfOneMsg);
		out.writeBoolean(this.isAccumulated);
		
		out.writeInt(this.verNum);
		out.writeLong(this.edgeNum);
		out.writeLong(this.loadByte);
		
		if (this.resDepend != null) {
			out.writeInt(this.resDepend.length);
			for (int i = 0; i < this.resDepend.length; i++) {
				out.writeInt(this.resDepend[i].length);
				for (int j = 0; j < this.resDepend[i].length; j++) {
					out.writeBoolean(this.resDepend[i][j]);
				}
			}
		} else {
			out.writeInt(0);
		}
		
		if (this.verNumBlks != null) {
			out.writeInt(this.blkNum);
			for (int i = 0; i < this.blkNum; i++) {
				out.writeInt(this.verNumBlks[i]);
			}
		} else {
			out.writeInt(0);
		}
		
		if (this.resVerNumBlks != null) {
			out.writeInt(this.blkNum);
			for (int i = 0; i < this.blkNum; i++) {
				out.writeInt(this.resVerNumBlks[i]);
			}
		} else {
			out.writeInt(0);
		}
	}
}
