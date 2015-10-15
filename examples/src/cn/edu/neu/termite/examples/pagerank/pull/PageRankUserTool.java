/**
 * NeuSoft Termite System
 * copyright 2011-2016
 */
package cn.edu.neu.termite.examples.pagerank.pull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.monitor.LocalStatistics;
import org.apache.hama.myhama.comm.CommRouteTable;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.api.UserTool;

/**
 * PageRankUserTool.java
 * Support for {@link PageRankGraphRecord} and {@link PageRankMsgRecord}.
 * 
 * @author zhigang wang
 * @version 0.1
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 * @param <S> send value
 */
public class PageRankUserTool extends UserTool<Double, Integer, Double, Integer> {
	public static final Log LOG = LogFactory.getLog(PageRankUserTool.class);
	
	public class PRGraphRecord extends GraphRecord<Double, Integer, Double, Integer> {
		
		@Override
	    public void initGraphData(String vData, String eData) {
			int length = 0, begin = 0, end = 0;
			this.verId = Integer.valueOf(vData);
			this.verValue = 10.0;
	        
	        ArrayList<Integer> tmpEdgeId = new ArrayList<Integer>();
	        
	        if (eData.equals("")) {
	 			setEdges(new Integer[]{this.verId}, null);
	 	        this.graphInfo = 1;
	        	return;
			}
	        
	    	char edges[] = eData.toCharArray();
	        length = edges.length; begin = 0; end = 0;
	        
	        for(end = 0; end < length; end++) {
	            if(edges[end] != ':') {
	                continue;
	            }
	            tmpEdgeId.add(Integer.valueOf(new String(edges, begin, end - begin)));
	            begin = ++end;
	        }
	        tmpEdgeId.add(Integer.valueOf(new String(edges, begin, end - begin)));
	        
	        Integer[] tmpTransEdgeId = new Integer[tmpEdgeId.size()];
	        tmpEdgeId.toArray(tmpTransEdgeId);
			
			setEdges(tmpTransEdgeId, null);
	        this.graphInfo = this.edgeNum;
	    }
		
		@Override
		public void serVerId(MappedByteBuffer vOut) throws EOFException, IOException {
			vOut.putInt(this.verId);
		}

		public void deserVerId(MappedByteBuffer vIn) throws EOFException, IOException {
			this.verId = vIn.getInt();
		}

		public void serVerValue(MappedByteBuffer vOut) throws EOFException, IOException {
			vOut.putDouble(this.verValue);
		}

		public void deserVerValue(MappedByteBuffer vIn) throws EOFException, IOException {
			this.verValue = vIn.getDouble();
		}

		public void serGrapnInfo(MappedByteBuffer eOut) throws EOFException, IOException {
			eOut.putInt(this.graphInfo);
		}

		public void deserGraphInfo(MappedByteBuffer eIn) throws EOFException, IOException {
			this.graphInfo = eIn.getInt();
		}

		public void serEdges(MappedByteBuffer eOut) throws EOFException, IOException {
			eOut.putInt(this.edgeNum);
	    	for (int index = 0; index < this.edgeNum; index++) {
	    		eOut.putInt(this.edgeIds[index]);
	    	}
		}

		public void deserEdges(MappedByteBuffer eIn) throws EOFException, IOException {
			this.edgeNum = eIn.getInt();
	    	this.edgeIds = new Integer[this.edgeNum];
	    	for (int index = 0; index < this.edgeNum; index++) {
	    		this.edgeIds[index] = eIn.getInt();
	    	}
		}
		
		@Override
		public ArrayList<GraphRecord<Double, Integer, Double, Integer>> 
    			decompose(CommRouteTable commRT, LocalStatistics local) {
			int dstTid, dstBid, tNum = commRT.getTaskNum();
			int[] bNum = commRT.getGlobalSketchGraph().getBucNumTask();
			ArrayList<Integer>[][] container = new ArrayList[tNum][];
			for (dstTid = 0; dstTid < tNum; dstTid++) {
				container[dstTid] = new ArrayList[bNum[dstTid]];
			}
			
			for (int index = 0; index < this.edgeNum; index++) {
				dstTid = commRT.getDstParId(this.edgeIds[index]);
				dstBid = commRT.getDstBucId(dstTid, this.edgeIds[index]);
				if (container[dstTid][dstBid] == null) {
					container[dstTid][dstBid] = new ArrayList<Integer>();
				}
				container[dstTid][dstBid].add(this.edgeIds[index]);
			}

			ArrayList<GraphRecord<Double, Integer, Double, Integer>> result = 
				new ArrayList<GraphRecord<Double, Integer, Double, Integer>>();
			for (dstTid = 0; dstTid < tNum; dstTid++) {
				for (dstBid = 0; dstBid < bNum[dstTid]; dstBid++) {
					if (container[dstTid][dstBid] != null) {
						Integer[] tmpEdgeIds = new Integer[container[dstTid][dstBid].size()];
						container[dstTid][dstBid].toArray(tmpEdgeIds);
						local.updateLocMatrix(dstTid, dstBid, verId, tmpEdgeIds.length);
						PRGraphRecord graph = new PRGraphRecord();
						graph.setVerId(verId);
						graph.setDstParId(dstTid);
						graph.setDstBucId(dstBid);
						graph.setSrcBucId(this.srcBucId);
						graph.setEdges(tmpEdgeIds, null);
						result.add(graph);
					}
				}
			}
			this.setEdges(null, null);

			return result;
		}

		@Override
		public MsgRecord<Double>[] getMsg(int iteStyle) {
			PRMsgRecord[] result = new PRMsgRecord[this.edgeNum];
			for (int i = 0; i < this.edgeNum; i++) {
				result[i] = new PRMsgRecord();
				result[i].initialize(this.verId, this.edgeIds[i], this.verValue);
			}
			return result;
		}
		
		@Override
		public Double getFinalValue() {
			return this.graphInfo==0? this.verValue:this.verValue*this.graphInfo;
		}
		
		/**
		 * Only for the byte of vertex value.
		 */
		@Override
		public int getVerByte() {
			return 8;
		}
		
		@Override
		public int getGraphInfoByte() {
			return 4;
		}
		
		/**
		 * Bytes for edge number and every edge.
		 * Only include normal edges.
		 * 4 + 4 * this.edgeNum
		 */
		@Override
		public int getEdgeByte() {
			return (4 + 4*this.edgeNum);
		}
	}
	
	public class PRMsgRecord extends MsgRecord<Double> {
		@Override
		public void combiner(MsgRecord<Double> msg) {
			this.msgValue += msg.getMsgValue();
		}
		
		@Override
		public int getMsgByte() {
			return 12; //int+double
		}
		
		@Override
		public void deserialize(DataInputStream in) throws IOException {
			this.dstId = in.readInt();
			this.msgValue = in.readDouble();
		}

		@Override
		public void serialize(DataOutputStream out) throws IOException {
			out.writeInt(this.dstId);
			out.writeDouble(this.msgValue);
		}
		
	}
	
	@Override
	public GraphRecord<Double, Integer, Double, Integer> getGraphRecord() {
		return new PRGraphRecord();
	}

	@Override
	public MsgRecord<Double> getMsgRecord() {
		return new PRMsgRecord();
	}
	
	@Override
	public boolean isAccumulated() {
		return true;
	}
}
