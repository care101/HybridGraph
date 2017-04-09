package org.apache.hama.myhama.api;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hama.monitor.TaskInformation;
import org.apache.hama.myhama.comm.CommRouteTable;
import org.apache.hama.myhama.graph.EdgeFragmentEntry;

/**
 * GraphRecord is a data structure used by HybridGraph. 
 * Intuitively, it represents one adjacency list.
 *  
 * Users should define their own representation by 
 * extending {@link GraphRecord}.
 * 
 * @author zhigang wang
 *
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public abstract class GraphRecord<V, W, M, I> {
	
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
	
	/**
	 * Set the vertex value.
	 * The physical meaning of vertex value in HybridGraph 
	 * may be different from that in Giraph for some algorithms. 
	 * Actually, in HybridGraph, vertex value is used to generate 
	 * correct messages in {@link BSP}.getMessages() without 
	 * help of statistical information of edges. 
	 * This is because edges may be divided and then stored in 
	 * several fragments, and in getMessages(), only partial edges 
	 * in one fragment are provided. Obviously, the global statistical 
	 * information, such as out-degree, is not available.
	 * Take PageRank as an example. 
	 * it is PageRank_Score/out-degree, 
	 * because "out-degree" is not available in getMessages(). 
	 * However, for Giraph, it is PageRank_Score, and the corrent 
	 * message value can be calculated based on out-degree, because 
	 * edges are not divided. 
	 * 
	 * @param _verValue
	 */
	public void setVerValue(V _verValue) {
		verValue = _verValue;
	}
	
	/**
	 * Get a read-only vertex value.
	 * The physical meaning of vertex value in HybridGraph 
	 * may be different from that in Giraph for some algorithms. 
	 * Actually, in HybridGraph, vertex value is used to generate 
	 * correct messages in {@link BSP}.getMessages() without 
	 * help of statistical information of edges. 
	 * This is because edges may be divided and then stored in 
	 * several fragments, and in getMessages(), only partial edges 
	 * in one fragment are provided. Obviously, the global statistical 
	 * information, such as out-degree, is not available.
	 * Take PageRank as an example. 
	 * it is PageRank_Score/out-degree, 
	 * because "out-degree" is not available in getMessages(). 
	 * However, for Giraph, it is PageRank_Score, and the corrent 
	 * message value can be calculated based on out-degree, because 
	 * edges are not divided. 
	 * @return
	 */
	public final V getVerValue() {
		return verValue;
	}
	
	/**
	 * Get a read-only statistical information. 
	 * Take PageRank as an example. 
     * The information should be the out-degree 
     * which can be used in {@link BSP}.update() to 
     * calculate vertex value.
     * We use graphInfo instead of edgeNum, because 
     * edges may not be available in update() when running 
     * style.Pull.
	 * @return
	 */
    public final I getGraphInfo() {
    	return graphInfo;
    }
    
    /**
     * Set the statistical information. 
	 * Take PageRank as an example. 
     * The information should be the out-degree 
     * which can be used in {@link BSP}.update() to 
     * calculate vertex value.
     * We use graphInfo instead of edgeNum, because 
     * edges may not be available in update() when running 
     * style.Pull.
     * @param _graphInfo
     */
    public void setGraphInfo(I _graphInfo) {
    	graphInfo = _graphInfo;
    }
    
    /**
     * Get the correct vertex value and save it onto HDFS. 
     * Return this.verValue if {@link BSPJob}useGraphInfoInUpdate(false). 
     * Otherwise, it should be defined based on the logics in 
     * {@link BSP}.update(). 
     * Take PageRank as an example, this.verValue actually represents 
     * PageRank_Score/out-degree calculated in {@link BSP}.update(). 
     * Thus, users should recovery the correct value PageRank_Score 
     * by this.verValue*this.graphInfo.
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
     * Only used for testing checkpoint
     * @param valData
     */
    public void parseVerValue(String valData) {
    	
    }

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
	 * Searialize graph record statistics info. onto the local disk. Do
	 * nothing if {@link BSPJob}.setGraphDataOnDisk(false); or
	 * {@link BSPJob}useGraphInfoInUpdate(false).
	 * 
	 * @param eOut
	 * @throws EOFException
	 * @throws IOException
	 */			     
    public void serGrapnInfo(ByteBuffer eOut) 
			throws EOFException, IOException { };

	/**
	 * Deserialize graph record statistics info. from the local disk.
	 * Do nothing if {@link BSPJob}.setGraphDataOnDisk(false) 
	 * or {@link BSPJob}useGraphInfoInUpdate(false).
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
     * Do nothing if {@link BSPJob}.setGraphDataOnDisk(false);
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
     * Do nothing if {@link BSPJob}.setGraphDataOnDisk(false);
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
		int[] blkNumOfTask = commRT.getJobInformation().getBlkNumOfTasks();
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
			dstTid = commRT.getDstTaskId(this.edgeIds[index]);
			dstBid = commRT.getDstLocalBlkIdx(dstTid, this.edgeIds[index]);
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

		return result;
    }
    
    /**
     * Return #fragments. 
     * #fragments is used to calculate the io bytes of reading vertex values 
     * during pulling messages, when the current superstep is running style.Push.
     * @param commRT
     * @param hitFlag
     * @return
     */
	public int getFragmentNum(CommRouteTable<V, W, M, I> commRT, 
			boolean[][] hitFlag) {
		for (int i = 0; i < hitFlag.length; i++) {
			Arrays.fill(hitFlag[i], false);
		}
		int dstTid = -1, dstBid = -1, counter = 0;
		for (int index = 0; index < this.edgeNum; index++) {
			dstTid = commRT.getDstTaskId(this.edgeIds[index]);
			dstBid = commRT.getDstLocalBlkIdx(dstTid, this.edgeIds[index]);
			if (!hitFlag[dstTid][dstBid]) {
				hitFlag[dstTid][dstBid] = true;
				counter++;
			}
		}
		
		return counter;
	}
}