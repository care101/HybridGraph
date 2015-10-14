package org.apache.hama.monitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public class TaskBackupMetadata implements Writable {
	private int parId;
	private int parNum;
	private int hInter;
	private int hNum;
	/**
	 * The disbribution of maximal values for ghost vertices and edges 
	 * which backed up by parId in theory.
	 */
	private int[] maxOuterGhostVertex;
	private long[] maxOuterGhostEdge;
	
	/**
	 * A histogram denotes the distribution of outer ghost edges.
	 * For example, edgeHist[i][0]=50, denotes that:
	 * The number of outer ghost vertices is 50, here outer ghost edges 
	 * for task i of one vertex is between 1 and 10.
	 * That means the benefit is between 50*(1-1) and 50*(10-1).
	 * Note that, the interval length is 10, or defined by users.
	 */
	private int[][] maxOuterEdgeHist;
	
	/**
	 * The disbribution of theory values for ghost vertices and edges 
	 * which backed up by parId in practise.
	 */
	@SuppressWarnings("unused")
	private int[] theOuterGhostVertex;
	private long[] theOuterGhostEdge;
	
	/**
	 * The disbribution of real values for ghost vertices and edges 
	 * which backed up by parId in practise.
	 */
	private int[] realOuterGhostVertex;
	private long[] realOuterGhostEdge;
	
	/**
	 * The summ of ghost vertices and edges backed up on this task,
	 * including itself.
	 */
	private int totalInnerGhostVertex;
	private long totalInnerGhostEdge;
	
	/**
	 * The summ of ghost vertices and edges backed up by this task,
	 * including itself.
	 */
	private int totalOuterGhostVertex;
	private long totalOuterGhostEdge;
	
	private int[] threVector;
	
	public TaskBackupMetadata() {
		
	}
	
	public void initialize(int parId, int parNumber) {
		this.parId = parId;
		this.parNum = parNumber;
		setHistParam();
		
		this.maxOuterGhostVertex = new int[parNumber];
		this.maxOuterGhostEdge = new long[parNumber];
		this.theOuterGhostVertex = new int[parNumber];
		this.theOuterGhostEdge = new long[parNumber];
		this.realOuterGhostVertex = new int[parNumber];
		this.realOuterGhostEdge = new long[parNumber];
		
		this.maxOuterEdgeHist = new int[parNumber][];
		for (int index = 0; index < parNumber; index++) {
			this.maxOuterEdgeHist[index] = new int[this.hNum];
		}
		
		this.threVector = new int[parNumber];
	}
	
	/**
	 * Evaluate the histogram parameters based on the available memory.
	 * Now, we just set parameters manually.
	 */
	private void setHistParam() {
		this.hInter = 10;//50
		this.hNum = 200;//300
	}
	
	public int getHistInterval() {
		return this.hInter;
	}
	
	public int getHistNumber() {
		return this.hNum;
	}
	
	public void updateMaxOuterGhostInfo(int dstParId, 
			int vertexId, int edgeNumber) {
		this.maxOuterGhostVertex[dstParId]++;
		this.maxOuterGhostEdge[dstParId] += edgeNumber;
		int index = edgeNumber / this.hInter;
		if (index < this.hNum) {
			this.maxOuterEdgeHist[dstParId][index] += edgeNumber;
		} else {
			this.maxOuterEdgeHist[dstParId][this.hNum - 1] += edgeNumber;
		}
	}
	
	public long[] getMaxOuterGhostEdge() {
		return this.maxOuterGhostEdge;
	}
	
	private void computeThresholdVector() {
		for (int dstParId = 0; dstParId < this.parNum; dstParId++) {
			if (this.theOuterGhostEdge[dstParId] <= 0) {
				this.threVector[dstParId] = Integer.MAX_VALUE;
			} else {
				int sum = 0, index = this.hNum - 1;
				for (; index >= 0; index--) {
					sum += this.maxOuterEdgeHist[dstParId][index];
					if (sum > this.theOuterGhostEdge[dstParId]) {
						break;
					}
				}
				
				if (index < 0) {index = 0;}
				//index = 0;
				this.threVector[dstParId] = this.hInter * index;
				/*if (this.threVector[dstParId] == 0) {
					this.threVector[dstParId] = this.hInter;
				}*/
			}
		}
	}
	
	public int[] getThresholdVector(){
		return this.threVector;
	}
	
	public void setTheOuterGhostInfo(long[] _theOuterGhostEdge) {
		theOuterGhostEdge = _theOuterGhostEdge;
		theOuterGhostEdge[parId] = Long.MAX_VALUE; //note that this
		computeThresholdVector();
	}
	
	public long getTheOuterGhostEdge(int dstParId) {
		return this.theOuterGhostEdge[dstParId];
	}
	
	public long getRealOuterGhostEdge(int dstParId) {
		return this.realOuterGhostEdge[dstParId];
	}
	
	public boolean isOverFlow(int dstParId) {
		if (this.realOuterGhostEdge[dstParId] > 
		this.theOuterGhostEdge[dstParId]) {
			return true;
		} else {
			return false;
		}
	}
	
	public void updateTotalOuterGhostInfo(int dstParId, int vertex, int edge) {
		this.realOuterGhostVertex[dstParId] += vertex;
		this.realOuterGhostEdge[dstParId] += edge;
		this.totalOuterGhostVertex += vertex;
		this.totalOuterGhostEdge += edge;
	}
	
	public void setTotalInnerGhostInfo(int vertex, long edge) {
		this.totalInnerGhostVertex = vertex;
		this.totalInnerGhostEdge = edge;
	}
	
	public int getTotalInnerGhostVertex() {
		return this.totalInnerGhostVertex;
	}
	
	public long getTotalInnerGhostEdge() {
		return this.totalInnerGhostEdge;
	}
	
	public int getTotalOuterGhostVertex() {
		return this.totalOuterGhostVertex;
	}
	
	public long getTotalOuterGhostEdge() {
		return this.totalOuterGhostEdge;
	}
	
	public long[] getRealOuterGhostEdge() {
		return this.realOuterGhostEdge;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("\n");
		sb.append("==================== TaskBackupMetadata ====================");
		
		sb.append("\nMaxOuterGhostVertex:");
		sb.append(Arrays.toString(this.maxOuterGhostVertex));
		sb.append("\nMaxOuterGhostEdge:");
		sb.append(Arrays.toString(this.maxOuterGhostEdge));
		
		sb.append("\nMaxOuterEdgeHistogram:");
		for (int index = 0; index < this.parNum; index++) {
			sb.append("\ndstParId="); sb.append(index);
			sb.append(Arrays.toString(this.maxOuterEdgeHist[index]));
		}
		
		sb.append("\nTheoryOuterGhostVertex:");
		sb.append(Arrays.toString(this.theOuterGhostVertex));
		sb.append("\nTheoryOuterGhostEdge:");
		sb.append(Arrays.toString(this.theOuterGhostEdge));
		sb.append("\nThresholdVector:");
		sb.append(Arrays.toString(this.threVector));
		
		sb.append("\nRealOuterGhostVertex:");
		sb.append(Arrays.toString(this.realOuterGhostVertex));
		sb.append("\nRealOuterGhostEdge:");
		sb.append(Arrays.toString(this.realOuterGhostEdge));
		
		sb.append("\nInnerInfo: #vertex="); sb.append(this.totalInnerGhostVertex);
		sb.append(", #edge="); sb.append(this.totalInnerGhostEdge);
		sb.append("\nOuterInfo: #vertex="); sb.append(this.totalOuterGhostVertex);
		sb.append(", #edge="); sb.append(this.totalOuterGhostEdge);
		sb.append("\nHistogram: interval="); sb.append(this.hInter);
		sb.append(", #bucket="); sb.append(this.hNum);
		sb.append("\n============================================================");
		
		return sb.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.parId = in.readInt();
		
		this.parNum = in.readInt();
		this.maxOuterGhostVertex = new int[this.parNum];
		this.maxOuterGhostEdge = new long[this.parNum];
		this.realOuterGhostEdge = new long[this.parNum];
		for (int i = 0; i < this.parNum; i++) {
			this.maxOuterGhostVertex[i] = in.readInt();
			this.maxOuterGhostEdge[i] = in.readLong();
			this.realOuterGhostEdge[i] = in.readLong();
		}
		
		/*this.hInter = in.readInt();
		this.hNum = in.readInt();
		this.maxOuterEdgeHist = new int[this.parNum][];
		for (int i = 0; i < this.parNum; i++) {
			this.maxOuterEdgeHist[i] = new int[this.hNum];
			for (int j = 0; j < this.hNum; j++) {
				this.maxOuterEdgeHist[i][j] = in.readInt();
			}
		}*/
		
		this.totalOuterGhostVertex = in.readInt();
		this.totalOuterGhostEdge = in.readLong();
		this.totalInnerGhostVertex = in.readInt();
		this.totalInnerGhostEdge = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.parId);
	
		out.writeInt(this.parNum);
		for (int i = 0; i < this.parNum; i++) {
			out.writeInt(this.maxOuterGhostVertex[i]);
			out.writeLong(this.maxOuterGhostEdge[i]);
			out.writeLong(this.realOuterGhostEdge[i]);
		}
		
		/*out.writeInt(this.hInter);
		out.writeInt(this.hNum);
		for (int i = 0; i < this.parNum; i++) {
			for (int j = 0; j < this.hNum; j++) {
				out.writeInt(this.maxOuterEdgeHist[i][j]);
			}
		}*/
		
		out.writeInt(this.totalOuterGhostVertex);
		out.writeLong(this.totalOuterGhostEdge);
		out.writeInt(this.totalInnerGhostVertex);
		out.writeLong(this.totalInnerGhostEdge);
	}
}
