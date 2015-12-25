/**
 * copyright 2011-2016
 */
package hybridgraph.examples.sssp.hybrid;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.monitor.TaskInformation;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.api.UserTool;
import org.apache.hama.myhama.comm.CommRouteTable;

/**
 * CCUserTool.java
 * Support for {@link SPGraphRecord}, {@link SPMsgRecord}.
 * 
 * @author 
 * @version 0.1
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class SPUserTool extends UserTool<Double, Double, Double, EdgeSet> {
	public static final Log LOG = LogFactory.getLog(SPUserTool.class);
	
	public static class SPGraphRecord 
			extends GraphRecord<Double, Double, Double, EdgeSet> {
		
		/**
		 * Assume that the weight of each edge is a random Integer.
		 */
		@Override
	    public void initGraphData(String vData, String eData) {
			int length = 0, begin = 0, end = 0;
			this.verId = Integer.valueOf(vData);
			this.verValue = Double.MAX_VALUE;
			
			if (eData.equals("")) {
				setEdges(new Integer[]{this.verId}, null);
				this.graphInfo = new EdgeSet();
	        	return;
			}
	        
	        ArrayList<Integer> tmpEdgeId = new ArrayList<Integer>();
	    	char edges[] = eData.toCharArray();
	        length = edges.length; begin = 0; end = 0;
	        
	        for(end = 0; end < length; end++) {
	            if(edges[end] != ':') {
	                continue;
	            }
	            tmpEdgeId.add(Integer.valueOf(
	            		new String(edges, begin, end-begin)));
	            begin = ++end;
	        }
	        tmpEdgeId.add(Integer.valueOf(
	        		new String(edges, begin, end-begin)));
	        
	        Integer[] tmpTransEdgeId = new Integer[tmpEdgeId.size()];
	        tmpEdgeId.toArray(tmpTransEdgeId);

	        setEdges(tmpTransEdgeId, null);
	        this.graphInfo = new EdgeSet(tmpTransEdgeId);
	    }
		
		@Override
		public void serVerId(ByteBuffer vOut) 
				throws EOFException, IOException {
			vOut.putInt(this.verId);
		}

		@Override
		public void deserVerId(ByteBuffer vIn) 
				throws EOFException, IOException {
			this.verId = vIn.getInt();
		}

		@Override
		public void serVerValue(ByteBuffer vOut) 
				throws EOFException, IOException {
			vOut.putDouble(this.verValue);
		}

		@Override
		public void deserVerValue(ByteBuffer vIn) 
				throws EOFException, IOException {
			this.verValue = vIn.getDouble();
		}

		@Override
		public void serGrapnInfo(ByteBuffer eOut) 
				throws EOFException, IOException {
			this.graphInfo.write(eOut);
		}
		
		@Override
		public void deserGraphInfo(ByteBuffer eIn) 
				throws EOFException, IOException {
			this.graphInfo = new EdgeSet();
			this.graphInfo.readFields(eIn);
		}

		@Override
		public void serEdges(ByteBuffer eOut) 
				throws EOFException, IOException {
			eOut.putInt(this.edgeNum);
	    	for (int index = 0; index < this.edgeNum; index++) {
	    		eOut.putInt(this.edgeIds[index]);
	    	}
		}

		@Override
		public void deserEdges(ByteBuffer eIn) 
				throws EOFException, IOException {
			this.edgeNum = eIn.getInt();
	    	this.edgeIds = new Integer[this.edgeNum];
	    	for (int index = 0; index < this.edgeNum; index++) {
	    		this.edgeIds[index] = eIn.getInt();
	    	}
		}
		
		@Override
		public int getVerByte() {
			return 8;
		}
		
		@Override
		public int getGraphInfoByte() {
			return this.graphInfo.getByteSize();
		}
		
		/**
		 * 4 + (4 + 4) * this.edgeNum
		 */
		@Override
		public int getEdgeByte() {
			return (4 + 4*this.edgeNum);
		}
		
		@Override
		public ArrayList<GraphRecord<Double, Double, Double, EdgeSet>> 
    			decompose(CommRouteTable<Double, Double, Double, EdgeSet> commRT, 
    					TaskInformation taskInfo) {
			int dstTid = 0, dstBid = 0, tNum = commRT.getTaskNum();
			int[] bNum = commRT.getGlobalSketchGraph().getBucNumTask();
			ArrayList<Integer>[][] conIds = new ArrayList[tNum][];
			for (dstTid = 0; dstTid < tNum; dstTid++) {
				conIds[dstTid] = new ArrayList[bNum[dstTid]];
			}
			
			for (int index = 0; index < this.edgeNum; index++) {
				dstTid = commRT.getDstParId(this.edgeIds[index]);
				dstBid = commRT.getDstBucId(dstTid, this.edgeIds[index]);
				if (conIds[dstTid][dstBid] == null) {
					conIds[dstTid][dstBid] = new ArrayList<Integer>();
				}
				conIds[dstTid][dstBid].add(this.edgeIds[index]);
			}

			ArrayList<GraphRecord<Double, Double, Double, EdgeSet>> result = 
				new ArrayList<GraphRecord<Double, Double, Double, EdgeSet>>();
			for (dstTid = 0; dstTid < tNum; dstTid++) {
				for (dstBid = 0; dstBid < bNum[dstTid]; dstBid++) {
					if (conIds[dstTid][dstBid] != null) {
						Integer[] tmpEdgeIds = 
							new Integer[conIds[dstTid][dstBid].size()];
						conIds[dstTid][dstBid].toArray(tmpEdgeIds);
						taskInfo.updateRespondDependency(
								dstTid, dstBid, verId, tmpEdgeIds.length);
						SPGraphRecord graph = new SPGraphRecord();
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
		public int getNumOfFragments(int iteStyle, 
				CommRouteTable commRT, boolean[][] hitFlag) {
			return this.graphInfo.getNumOfFragments(commRT, hitFlag);
		}
	}
	
	public static class SPMsgRecord extends MsgRecord<Double> {
		
		@Override
		public void combiner(MsgRecord<Double> msg) {
			if (this.msgValue > msg.getMsgValue()) {
				this.msgValue = msg.getMsgValue();
			}
		}
		
		@Override
		public int getMsgByte() {
			return 12; //int+double
		}
		
		@Override
	    public void serialize(ByteBuffer out) throws IOException {
	    	out.putInt(this.dstId);
	    	out.putDouble(this.msgValue);
	    }
	    
		@Override
	    public void deserialize(ByteBuffer in) throws IOException {
	    	this.dstId = in.getInt();
	    	this.msgValue = in.getDouble();
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
	public GraphRecord<Double, Double, Double, EdgeSet> getGraphRecord() {
		return new SPGraphRecord();
	}

	@Override
	public MsgRecord<Double> getMsgRecord() {
		return new SPMsgRecord();
	}
	
	@Override
	public boolean isAccumulated() {
		return true;
	}
}
