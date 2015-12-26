/**
 * copyright 2011-2016
 */
package hybridgraph.examples.pagerank.pull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.api.UserTool;

/**
 * PageRankUserTool.java
 * Support for {@link PageRankGraphRecord} and {@link PageRankMsgRecord}.
 * 
 * @author 
 * @version 0.1
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 * @param <S> send value
 */
public class PageRankUserTool 
		extends UserTool<Double, Integer, Double, Integer> {
	public static final Log LOG = LogFactory.getLog(PageRankUserTool.class);
	
	public static class PRGraphRecord 
			extends GraphRecord<Double, Integer, Double, Integer> {
		
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
	            tmpEdgeId.add(Integer.valueOf(
	            		new String(edges, begin, end-begin)));
	            begin = ++end;
	        }
	        tmpEdgeId.add(Integer.valueOf(
	        		new String(edges, begin, end-begin)));
	        
	        Integer[] tmpTransEdgeId = new Integer[tmpEdgeId.size()];
	        tmpEdgeId.toArray(tmpTransEdgeId);
			
			setEdges(tmpTransEdgeId, null);
	        this.graphInfo = this.edgeNum;
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
			eOut.putInt(this.graphInfo);
		}

		@Override
		public void deserGraphInfo(ByteBuffer eIn) 
				throws EOFException, IOException {
			this.graphInfo = eIn.getInt();
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
		public Double getFinalValue() {
			return this.graphInfo==0? 
					this.verValue:this.verValue*this.graphInfo;
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
	
	public static class PRMsgRecord extends MsgRecord<Double> {
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
	public GraphRecord<Double, Integer, Double, Integer> 
			getGraphRecord() {
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
