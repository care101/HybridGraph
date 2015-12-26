/**
 * copyright 2011-2016
 */
package hybridgraph.examples.sssp.hybrid;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.api.UserTool;
import org.apache.hama.myhama.io.EdgeParser;

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
	private static EdgeParser edgeParser = new EdgeParser();
	
	public static class SPGraphRecord 
			extends GraphRecord<Double, Double, Double, EdgeSet> {
		
		/**
		 * Assume that the weight of each edge is a random Integer 
		 * generated by Random() when producing messages.
		 */
		@Override
	    public void parseGraphData(String vData, String eData) {
			this.verId = Integer.valueOf(vData);
			this.verValue = Double.MAX_VALUE;
			
			if (eData.equals("")) {
				setEdges(new Integer[]{this.verId}, null);
				this.graphInfo = new EdgeSet();
	        	return;
			}
	        
			Integer[] parsedEdgeIds = edgeParser.parseEdgeIdArray(eData,':');
	        setEdges(parsedEdgeIds, null);
	        this.graphInfo = new EdgeSet(parsedEdgeIds);
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
	}
	
	public static class SPMsgRecord extends MsgRecord<Double> {
		
		@Override
		public void combiner(MsgRecord<Double> msg) {
			this.msgValue = this.msgValue>msg.getMsgValue()? 
					msg.getMsgValue():this.msgValue;
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
