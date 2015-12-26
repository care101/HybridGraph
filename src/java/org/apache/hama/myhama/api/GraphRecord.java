package org.apache.hama.myhama.api;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hama.monitor.TaskInformation;
import org.apache.hama.myhama.comm.CommRouteTable;
import org.apache.hama.myhama.graph.EdgeFragmentEntry;

/**
 * GraphRecord implemented by users, 
 * which consists of five components:
 *   1) variables and operations for the VBlock's Triple;
 *   2) variables and operations for the EBlock's Fragment;
 *   3) some functions used for disk operations;
 *   4) user-defined functions for loading and decomposing graph data;
 *   5) variables and operations used by decomposing.
 * 
 * @author root
 *
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public abstract class GraphRecord<V, W, M, I> 
		implements GraphRecordInterface<V, W, M, I> {
	
	public GraphRecord() { };
	
	//===================================================================
	// Variables and operations in Triple (i.e., elements of a VBlock)
	//===================================================================
	protected int verId;
	protected V verValue;
	protected I graphInfo;
	
	public void setVerId(int _verId) {
		verId = _verId;
	}
	
	public final int getVerId() {
		return verId;
	}
	
	public void setVerValue(V _verValue) {
		verValue = _verValue;
	}
	
	public final V getVerValue() {
		return verValue;
	}
	
    public final I getGraphInfo() {
    	return graphInfo;
    }
    
    public void setGraphInfo(I _graphInfo) {
    	graphInfo = _graphInfo;
    }
    
    /**
     * Get the final value and save it onto HDFS.
     * For most algorithms, such as SSSP, it is equal to the current value.
     * Thus, the defalut return-value is this.verValue.
     * However, for PageRank, the current value is RealValue/OutDegree.
     * Thus, users should override this function to get the correct final value.
     * @return
     */
    public V getFinalValue() {
    	return verValue;
    }
    
    /**
     * Serialize vertex value onto the local memory or disk.
     * For memory, it is used to separate the two values 
     * used in superstep t and (t+1).
     * 
     * @param vOut
     * @throws EOFException
     * @throws IOException
     */
    public abstract void serVerValue(ByteBuffer vOut) 
			throws EOFException, IOException;

    /**
     * Deserialize vertex value from the local memory or disk.
     * For memory, it is used to separate the two values 
     * used in superstep t and (t+1).
     * 
     * @param vIn
     * @throws EOFException
     * @throws IOException
     */
    public abstract void deserVerValue(ByteBuffer vIn) 
			throws EOFException, IOException;
    
	/**
	 * Return the bytes of vertex, now only including vertex value.
	 * @return
	 */
	public abstract int getVerByte();
	
	/**
	 * Return the bytes of statistic data. 
	 * For instance, the summ of original outer edges.
	 * @return
	 */
	public abstract int getGraphInfoByte();
	
	
	//======================================================================
	// Variables and operations in Fragments (i.e., elements of an EBlock)
	//======================================================================
	protected int edgeNum = 0;
	protected Integer[] edgeIds;
	protected W[] edgeWeights;
    
	public void setEdgeNum(int _num) {
		this.edgeNum = _num;
	}
	    
	public final int getEdgeNum() {
		return edgeNum;
	}
	    
	public void setEdges(Integer[] _edgeIds, W[] _edgeWeights) {
		edgeIds = _edgeIds;
		edgeWeights = _edgeWeights;
		edgeNum = _edgeIds==null? 0:_edgeIds.length;
	}
	    
	public final Integer[] getEdgeIds() {
		return this.edgeIds;
	}
	    
	public final W[] getEdgeWeights() {
		return edgeWeights;
	}
	
	/**
	 * Return the bytes of edges. 
	 * Including #edges, edgeId, edge weight.
	 * @return
	 */
	public abstract int getEdgeByte();
	
    
    //=============================================
    // User-defined functions if disk is used.
    //=============================================
    
    /**
     * Serialize vertex id onto the local disk.
     * This should be overrided if the local disk is used.
     * 
     * @param vOut
     * @throws EOFException
     * @throws IOException
     */
    public void serVerId(ByteBuffer vOut) 
    		throws EOFException, IOException { vOut.putInt(this.verId); };
    
    /**
     * Deserialize vertex id from the local disk.
     * This should be overrided if the local disk is used.
     * 
     * @param vIn
     * @throws EOFException
     * @throws IOException
     */
    public void deserVerId(ByteBuffer vIn) 
    		throws EOFException, IOException { this.verId = vIn.getInt(); };
    
    /**
     * Searialize graph record statistics info. onto the local disk.
     * This should be overrided if the local disk is used.
     * 
     * @param eOut
     * @throws EOFException
     * @throws IOException
     */
    public void serGrapnInfo(ByteBuffer eOut) 
			throws EOFException, IOException { };

    /**
     * Deserialize graph record statistics info. from the local disk.
     * This should be overrided if the local disk is used.
     * 
     * @param eIn
     * @throws EOFException
     * @throws IOException
     */
    public void deserGraphInfo(ByteBuffer eIn) 
			throws EOFException, IOException { };
    
    /**
     * Serialize outer edges onto the local disk.
     * Include the number of edges.
     * This should be overrided if the local disk is used.
     * 
     * @param eOut
     * @throws EOFException
     * @throws IOException
     */
    public void serEdges(ByteBuffer eOut) 
			throws EOFException, IOException { };

    /**
     * Deserialize outer edges from the local disk.
     * Include the number of edges.
     * This should be overrided if the local disk is used.
     * 
     * @param eIn
     * @throws EOFException
     * @throws IOException
     */
    public void deserEdges(ByteBuffer eIn) 
			throws EOFException, IOException { };
		
			
   //==========================================================
   // The following functions must be implemented by users.
   //==========================================================
   /**
     * Parse the graph data and then initialize variables 
     * in {@link GraphRecord}.
     * This function is only be invoked in 
     * {@link GraphDataServer}.loadGraph().
     * vData is read from HDFS as the <code>key</code> 
     * and eData is read from HDFS as the <code>value</code>.
     * 
     * @param vData String
     * @param eData String
     */
    public abstract void parseGraphData(String vData, String eData);

	
	//==============================================================================
	// Other operations used to divide an adjacency list into several fragments.
	//==============================================================================
	protected int srcBid; //blkId which the source vertex id belong to
    
	public void initialize(EdgeFragmentEntry<V,W,M,I> frag) {
		this.verId = frag.getVerId();
		setEdges(frag.getEdgeIds(), frag.getEdgeWeights());
	}
    
    /**
     * Set the id of local VBlock where the source vertex belongs.
     * @param _srcBucId
     */
    public void setSrcBlkId(int _srcBid) {
    	srcBid = _srcBid;
    }
    
    /**
     * Decompose a given {@link GraphRecord} 
     * into several {@link GraphRecord}s/fragments.
     * Now, the decomposing policy is to divide the outgoing edges 
     * depends on {@link CommRouteTable}.
     * 
     * By the way, {@link TaskInformation} will be invoked to 
     * update the dependency among VBlocks.
     * 
     * Note: the outgoing edge data of this {@link GraphRecord}
     * will be changed after decomposing.
     * @param commRT
     * @param taskInfo
     * @return
     */
    @SuppressWarnings("unchecked")
	public ArrayList<EdgeFragmentEntry<V,W,M,I>> 
    			decompose(CommRouteTable<V, W, M, I> commRT, 
    					TaskInformation taskInfo) {
		int dstTid, dstBid, taskNum = commRT.getTaskNum();
		int[] blkNumOfTask = commRT.getGlobalSketchGraph().getBucNumTask();
		boolean hasWeight = this.edgeWeights==null? false:true;
		ArrayList<Integer>[][] idOfFragments = new ArrayList[taskNum][];
		ArrayList<W>[][] weightOfFragments = null;
		for (dstTid = 0; dstTid < taskNum; dstTid++) {
			idOfFragments[dstTid] = new ArrayList[blkNumOfTask[dstTid]];
		}
		if (hasWeight) {
			weightOfFragments = new ArrayList[taskNum][];
			for (dstTid = 0; dstTid < taskNum; dstTid++) {
				weightOfFragments[dstTid] = new ArrayList[blkNumOfTask[dstTid]];
			}
		}
		
		//decomposing
		for (int index = 0; index < this.edgeNum; index++) {
			dstTid = commRT.getDstParId(this.edgeIds[index]);
			dstBid = commRT.getDstBucId(dstTid, this.edgeIds[index]);
			if (idOfFragments[dstTid][dstBid] == null) {
				idOfFragments[dstTid][dstBid] = new ArrayList<Integer>(); 
				if (hasWeight) {
					weightOfFragments[dstTid][dstBid] = new ArrayList<W>();
				}
			}
			idOfFragments[dstTid][dstBid].add(this.edgeIds[index]);
			if (hasWeight) {
				weightOfFragments[dstTid][dstBid].add(this.edgeWeights[index]);
			}
		}
		
		//constructing fragments
		ArrayList<EdgeFragmentEntry<V,W,M,I>> result = 
			new ArrayList<EdgeFragmentEntry<V,W,M,I>>();
		for (dstTid = 0; dstTid < taskNum; dstTid++) {
			for (dstBid = 0; dstBid < blkNumOfTask[dstTid]; dstBid++) {
				if (idOfFragments[dstTid][dstBid] != null) {
					EdgeFragmentEntry<V,W,M,I> frag = 
						new EdgeFragmentEntry<V,W,M,I>(
								this.verId, this.srcBid, dstTid, dstBid);
					
					Integer[] tmpEdgeIds = 
						new Integer[idOfFragments[dstTid][dstBid].size()];
					idOfFragments[dstTid][dstBid].toArray(tmpEdgeIds);
					W[] tmpEdgeWeights = null;
					if (hasWeight) {
						tmpEdgeWeights = 
							(W[]) new Object[idOfFragments[dstTid][dstBid].size()];
					}
					
					taskInfo.updateRespondDependency(
							dstTid, dstBid, this.verId, tmpEdgeIds.length);
					frag.initialize(tmpEdgeIds, tmpEdgeWeights);
					result.add(frag);
				}
			}
		}
		this.setEdges(null, null);

		return result;
    }
}