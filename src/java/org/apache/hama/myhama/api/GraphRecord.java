package org.apache.hama.myhama.api;

import java.io.EOFException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hama.monitor.LocalStatistics;
import org.apache.hama.myhama.comm.CommRouteTable;

/**
 * GraphRecord implemented by users.
 * @author root
 *
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public abstract class GraphRecord<V, W, M, I> {
	protected int verId;
	protected V verValue;
	protected I graphInfo;
	
	protected int edgeNum = 0;
	protected Integer[] edgeIds;
	protected W[] edgeWeights;
	
	protected int dstParId; //parId which the destination vertex id belong to
	protected int dstBucId; //bucId which the destination vertex id belong to
	protected int srcBucId; //bucId which the source vertex id belong to

    public void setVerId(int _verId) {
        verId = _verId;
    }

    public int getVerId() {
        return verId;
    }
    
    public void setVerValue(V _verValue) {
        verValue = _verValue;
    }

    public V getVerValue() {
        return verValue;
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
    
    public void setEdgeNum(int _num) {
    	this.edgeNum = _num;
    }
    
    public int getEdgeNum() {
    	return edgeNum;
    }
    
    public void setEdges(Integer[] _edgeIds, W[] _edgeWeights) {
    	edgeIds = _edgeIds;
    	edgeWeights = _edgeWeights;
    	edgeNum = _edgeIds==null? 0:_edgeIds.length;
    }
    
    public Integer[] getEdgeIds() {
        return this.edgeIds;
    }
    
    public W[] getEdgeWeights() {
        return edgeWeights;
    }
    
    public void setDstParId(int _dstParId) {
    	dstParId = _dstParId;
    }
    
    public int getDstParId() {
    	return dstParId;
    }
    
    public void setDstBucId(int _dstBucId) {
    	dstBucId = _dstBucId;
    }
    
    public int getDstBucId() {
    	return dstBucId;
    }
    
    public void setSrcBucId(int _srcBucId) {
    	this.srcBucId = _srcBucId;
    }
    
    public int getSrcBucId() {
    	return this.srcBucId;
    }
    
    public I getGraphInfo() {
    	return graphInfo;
    }
    
    public void setGraphInfo(I _graphInfo) {
    	graphInfo = _graphInfo;
    }
    
    public int getNumOfFragments(int iteStyle,
    		CommRouteTable commRT, boolean[][] hitFlag) {
    	return 0;
    }
    
    @Override
    public String toString() {
    	StringBuffer sb = new StringBuffer();
    	sb.append("id="); sb.append(verId);
    	sb.append(" val="); sb.append(verValue);
    	sb.append(" dstParId="); sb.append(dstParId);
    	sb.append(" dstBucId="); sb.append(dstBucId);
    	sb.append("\nedgeId="); sb.append(Arrays.toString(edgeIds));
    	return sb.toString();
    }
    
    /**
     * Initialize the graph data according to the String vData and eData.
     * This function will only be invoked during the localize the graph record 
     * in the {@link GraphDataServer.localizeGraphData()}.
     * The String vData is read from HDFS as the <code>key</code> 
     * and eData is read from HDFS as the <code>value</code>.
     * 
     * @param vData String
     * @param eData String
     */
    public abstract void initGraphData(String vData, String eData);
    
    /**
     * Serialize vertex id onto the local disk.
     * @param vOut
     * @throws EOFException
     * @throws IOException
     */
    public abstract void serVerId(MappedByteBuffer vOut) 
    		throws EOFException, IOException;
    
    /**
     * Deserialize vertex id from the local disk.
     * @param vIn
     * @throws EOFException
     * @throws IOException
     */
    public abstract void deserVerId(MappedByteBuffer vIn) 
    		throws EOFException, IOException;
    
    /**
     * Serialize vertex value onto the local disk.
     * @param vOut
     * @throws EOFException
     * @throws IOException
     */
    public abstract void serVerValue(MappedByteBuffer vOut) 
			throws EOFException, IOException;

    /**
     * Deserialize vertex value from the local disk.
     * @param vIn
     * @throws EOFException
     * @throws IOException
     */
    public abstract void deserVerValue(MappedByteBuffer vIn) 
			throws EOFException, IOException;
    
    /**
     * Searialize graph record statistics info. onto the local disk.
     * @param eOut
     * @throws EOFException
     * @throws IOException
     */
    public abstract void serGrapnInfo(MappedByteBuffer eOut) 
			throws EOFException, IOException;

    /**
     * Deserialize graph record statistics info. from the local disk.
     * @param eIn
     * @throws EOFException
     * @throws IOException
     */
    public abstract void deserGraphInfo(MappedByteBuffer eIn) 
			throws EOFException, IOException;
    
    /**
     * Serialize outer edges onto the local disk.
     * Include the number of edges.
     * @param eOut
     * @throws EOFException
     * @throws IOException
     */
    public abstract void serEdges(MappedByteBuffer eOut) 
			throws EOFException, IOException;

    /**
     * Deserialize outer edges from the local disk.
     * Include the number of edges.
     * @param eIn
     * @throws EOFException
     * @throws IOException
     */
    public abstract void deserEdges(MappedByteBuffer eIn) 
			throws EOFException, IOException;
    
    /**
     * Decompose a given {@link GraphRecord} into several {@link GraphRecord}s.
     * Now, the decomposing policy is to divide the outgoing edges 
     * depends on {@link CommRouteTable}.
     * 
     * By the way, {@link LocalStatistics} will be invoked to 
     * update the local matrix among virtual buckets.
     * 
     * Note: the outgoing edge data of this {@link GraphRecord}
     * will be changed after decomposing.
     * @param commRT
     * @param local
     * @return
     */
    public abstract ArrayList<GraphRecord<V, W, M, I>> 
    			decompose(CommRouteTable commRT, LocalStatistics local);
    
    /**
     * Get {@link MsgRecord}s from the ghost {@link GraphRecord}.
     * @return
     */
	public abstract MsgRecord<M>[] getMsg(int _iteStyle);
    
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
	
	/**
	 * Return the bytes of edges. 
	 * Including #edges, edgeId, edge weight.
	 * @return
	 */
	public abstract int getEdgeByte();
}
