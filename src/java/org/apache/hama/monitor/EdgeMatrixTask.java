package org.apache.hama.monitor;

import java.util.Arrays;

public class EdgeMatrixTask {
	private int taskNum;
	private long[][] maxEdgeMatrix;
	private long[][] theEdgeMatrix;
	
	public EdgeMatrixTask(int taskNum) {
		this.taskNum = taskNum;
		
		this.maxEdgeMatrix = new long[taskNum][];
		this.theEdgeMatrix = new long[taskNum][];
		for (int parId = 0; parId < taskNum; parId++) {
			this.maxEdgeMatrix[parId] = new long[taskNum];
			this.theEdgeMatrix[parId] = new long[taskNum];
		}
	}
	
	public void setMaxEdgeMatrix(int srcParId, long[] edgeArray) {
		this.maxEdgeMatrix[srcParId] = edgeArray;
	}
	
	public long[][] getMaxEdgeMatrix() {
		return this.maxEdgeMatrix;
	}
	
	public long[][] getTheEdgeMatrix() {
		return this.theEdgeMatrix;
	}
	
	public long[][] doNoneEdgeExchange() {
		for (int srcParId = 0; srcParId < this.taskNum; srcParId++) {
			this.theEdgeMatrix[srcParId][srcParId] = 0;
			for (int dstParId = srcParId + 1; dstParId < this.taskNum; dstParId++) {
				this.theEdgeMatrix[srcParId][dstParId] = 0;
				this.theEdgeMatrix[dstParId][srcParId] = this.theEdgeMatrix[srcParId][dstParId];
			}
		}
		return this.theEdgeMatrix;
	}
	
	public long[][] doMinEdgeExchange() {
		for (int srcParId = 0; srcParId < this.taskNum; srcParId++) {
			this.theEdgeMatrix[srcParId][srcParId] = this.maxEdgeMatrix[srcParId][srcParId];
			for (int dstParId = srcParId + 1; dstParId < this.taskNum; dstParId++) {
				this.theEdgeMatrix[srcParId][dstParId] = 
					Math.min(this.maxEdgeMatrix[srcParId][dstParId], 
							this.maxEdgeMatrix[dstParId][srcParId]);
				this.theEdgeMatrix[dstParId][srcParId] = this.theEdgeMatrix[srcParId][dstParId];
			}
		}
		return this.theEdgeMatrix;
	}
	
	public long[][] doMaxEdgeExchange() {
		for (int srcParId = 0; srcParId < this.taskNum; srcParId++) {
			this.theEdgeMatrix[srcParId][srcParId] = this.maxEdgeMatrix[srcParId][srcParId];
			for (int dstParId = srcParId + 1; dstParId < this.taskNum; dstParId++) {
				this.theEdgeMatrix[srcParId][dstParId] = 
					Math.max(this.maxEdgeMatrix[srcParId][dstParId], 
							this.maxEdgeMatrix[dstParId][srcParId]);
				this.theEdgeMatrix[dstParId][srcParId] = this.theEdgeMatrix[srcParId][dstParId];
			}
		}
		return this.theEdgeMatrix;
	}
	
	public long[][] doGreedyEdgeExchange() {
		for (int srcParId = 0; srcParId < this.taskNum; srcParId++) {
			this.theEdgeMatrix[srcParId][srcParId] = 0;
			for (int dstParId = srcParId + 1; dstParId < this.taskNum; dstParId++) {
				if (srcParId == 0) {
					this.theEdgeMatrix[srcParId][dstParId] = this.maxEdgeMatrix[srcParId][dstParId];
				} else {
					this.theEdgeMatrix[srcParId][dstParId] = 
						Math.min(Math.min(this.maxEdgeMatrix[srcParId][dstParId], 
								this.maxEdgeMatrix[dstParId][srcParId]), 
								Math.min(this.maxEdgeMatrix[srcParId][dstParId], 
										this.maxEdgeMatrix[dstParId][srcParId]) + 500000);
				}
				
				this.theEdgeMatrix[dstParId][srcParId] = this.theEdgeMatrix[srcParId][dstParId];
			}
		}
		return this.theEdgeMatrix;
	}
	  
	public String disEdgeMatrix(long[][] matrix, String name) {
		StringBuffer sb = new StringBuffer();
		sb.append(name);
		for (int srcParId = 0; srcParId < this.taskNum; srcParId++) {
			sb.append("\nparId=" + srcParId + " ");
			sb.append(Arrays.toString(matrix[srcParId]));
		}
		return sb.toString();
	}
}
