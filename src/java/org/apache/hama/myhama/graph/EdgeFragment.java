package org.apache.hama.myhama.graph;

import org.apache.hama.myhama.api.GraphRecord;

/**
 * A fragment in EBlock.
 */
public class EdgeFragment<V, W, M, I> {
	protected int verId;
	protected int edgeNum;
	protected Integer[] edgeIds;
	protected W[] edgeWeights;
	
	public EdgeFragment() { }
	
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
