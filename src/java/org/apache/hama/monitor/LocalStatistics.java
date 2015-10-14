package org.apache.hama.monitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class LocalStatistics implements Writable {
	private static final Log LOG = LogFactory.getLog(LocalStatistics.class);
	private int taskId;
	private int verMinId, verMaxId;
	private int port;
	private String hostName;
	
	private int byteOfOneMsg = 0;
	private boolean isAccumulated = false;
	
	private int verNum = 0;
	private long edgeNum = 0L;
	private int bucLen = 0;
	private int bucNum = 0;
	private long loadByte = 0L;
	
	private GlobalSketchGraph skGraph = null;
	private long[][] locMatrix;
	private int[] verNumBucs;
	private int[] actVerNumBucs;
	private int bucNumJob;
	
	public LocalStatistics () {
		
	}
	
	public LocalStatistics (int _taskId, int _minVerId, int _port, String _hostName, 
			int _byteOfOneMsg, boolean _isAccumulated) {
		taskId = _taskId;
		verMinId = _minVerId;
		port = _port;
		hostName = _hostName;
		
		this.byteOfOneMsg = _byteOfOneMsg;
		this.isAccumulated = _isAccumulated;
	}
	
	public void init(GlobalStatistics global) {
		this.verMaxId = global.getRangeMaxId(taskId);
		this.verNum = verMaxId - verMinId + 1;
		this.skGraph = global.getGlobalSketchGraph();
		this.bucNum = skGraph.getBucNumTask(taskId);
		this.bucLen = skGraph.getBucLenTask(taskId);
		this.locMatrix = new long[bucNum][skGraph.getBucNumJob()];
		this.verNumBucs = new int[bucNum];
		this.actVerNumBucs = new int[bucNum];
		this.bucNumJob = skGraph.getBucNumJob();
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
	
	public int getBucLen() {
		return this.bucLen;
	}
	
	public int getBucNum() {
		return this.bucNum;
	}
	
	public int getBucNumJob() {
		return this.bucNumJob;
	}
	
	public void setVerNum(int _verNum) {
		this.verNum = _verNum;
	}
	
	public int getVerNum() {
		return this.verNum;
	}
	
	public void setVerNumBucs(int[] _array) {
		this.verNumBucs = _array;
	}
	
	public int[] getVerNumBucs() {
		return this.verNumBucs;
	}
	
	public void setActVerNumBucs(int[] _array) {
		this.actVerNumBucs = _array;
	}
	
	public int[] getActVerNumBucs() {
		return this.actVerNumBucs;
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
	 * Update the local matrix among virtual buckets.
	 * This function should be invoked by decompose() of {@link GraphRecord}.
	 * @param taskId
	 * @param vId
	 * @param exch
	 */
	public void updateLocMatrix(int _dstTid, int _dstBid, int vid, long len) {
		int row = skGraph.getTaskBucIndex(this.taskId, vid);
		int col = skGraph.getGlobalBucIndex(_dstTid, _dstBid);
		try {
			this.locMatrix[row][col] += len;
		} catch (Exception e) {
			LOG.error("row=" + row + " col=" + col, e);
		}
	}
	
	public void updateLocalMatrix(long[][] m) {
		for (int i = 0; i < this.bucNum; i++) {
			for (int j = 0; j < this.skGraph.getBucNumJob(); j++) {
				this.locMatrix[i][j] = m[i][j];
			}
		}
	}
	
	public long[][] getLocalMatrix() {
		return this.locMatrix;
	}
	
	public void clearLocalMatrix() {
		for (int bid = 0; bid < this.bucNum; bid++) {
			Arrays.fill(this.locMatrix[bid], 0);
		}
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
		sb.append(" #bucLen="); sb.append(this.bucLen);
		sb.append(" #bucNum="); sb.append(this.bucNum);
		sb.append(" loadByte="); sb.append(this.loadByte);
		
		sb.append("\nLocal Edge Matrix Info.\n");
		for (int i = 0; i < this.bucNum; i++) {
			sb.append(Arrays.toString(this.locMatrix[i])); sb.append("\n");
		}
		
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
			this.locMatrix = new long[rowNum][];
			for (int i = 0; i < rowNum; i++) {
				int colNum = in.readInt();
				this.locMatrix[i] = new long[colNum];
				for (int j = 0; j < colNum; j++) {
					this.locMatrix[i][j] = in.readLong();
				}
			}
		} else {
			this.locMatrix = null;
		}
		
		int num = in.readInt();
		if (num != 0) {
			this.verNumBucs = new int[num];
			for (int i = 0; i < num; i++) {
				this.verNumBucs[i] = in.readInt();
			}
		} else {
			this.verNumBucs = null;
		}
		
		num = in.readInt();
		if (num != 0) {
			this.actVerNumBucs = new int[num];
			for (int i = 0; i < num; i++) {
				this.actVerNumBucs[i] = in.readInt();
			}
		} else {
			this.actVerNumBucs = null;
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
		
		if (this.locMatrix != null) {
			out.writeInt(this.locMatrix.length);
			for (int i = 0; i < this.locMatrix.length; i++) {
				out.writeInt(this.locMatrix[i].length);
				for (int j = 0; j < this.locMatrix[i].length; j++) {
					out.writeLong(this.locMatrix[i][j]);
				}
			}
		} else {
			out.writeInt(0);
		}
		
		if (this.verNumBucs != null) {
			out.writeInt(this.bucNum);
			for (int i = 0; i < this.bucNum; i++) {
				out.writeInt(this.verNumBucs[i]);
			}
		} else {
			out.writeInt(0);
		}
		
		if (this.actVerNumBucs != null) {
			out.writeInt(this.bucNum);
			for (int i = 0; i < this.bucNum; i++) {
				out.writeInt(this.actVerNumBucs[i]);
			}
		} else {
			out.writeInt(0);
		}
	}
}
