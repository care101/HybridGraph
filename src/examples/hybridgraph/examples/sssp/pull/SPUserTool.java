/**
 * copyright 2011-2016
 */
package hybridgraph.examples.sssp.pull;

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
 * @param <S> send value
 */
public class SPUserTool extends UserTool<Double, Double, Double, Integer> {
	public static final Log LOG = LogFactory.getLog(SPUserTool.class);
	
	public static class SPGraphRecord 
			extends GraphRecord<Double, Double, Double, Integer> {
		
		/**
		 * Assume that the weight of each edge is a random Integer, or 1 (as Pregel).
		 */
		@Override
	    public void initGraphData(String vData, String eData) {
			int length = 0, begin = 0, end = 0;
			this.verId = Integer.valueOf(vData);
			this.verValue = Double.MAX_VALUE;
			
			if (eData.equals("")) {
				setEdges(new Integer[]{this.verId}, null);
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
			return 0;
		}
		
		/**
		 * 4 + (4 + 4) * this.edgeNum
		 */
		@Override
		public int getEdgeByte() {
			return (4 + 4*this.edgeNum);
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
	public GraphRecord<Double, Double, Double, Integer> getGraphRecord() {
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
