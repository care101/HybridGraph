/**
 * copyright 2011-2016
 */
package hybridgraph.examples.sssp.determ;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.api.UserTool;
import org.apache.hama.myhama.io.EdgeParser;
import org.apache.hama.myhama.io.EdgeParser.IntDoubleEdgeSet;

/**
 * SPUserToolDeterm.java
 * Support for {@link SPGraphRecordDeterm}, {@link SPMsgRecordDeterm}.
 * 
 * @author 
 * @version 0.1
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class SPUserToolDeterm extends UserTool<Double, Double, Double, Integer> {
	private static EdgeParser edgeParser = new EdgeParser();
	
	public static class SPGraphRecordDeterm 
			extends GraphRecord<Double, Double, Double, Integer> {
		
		/**
		 * Load a weighted graph.
		 */
		@Override
	    public void parseGraphData(String vData, String eData) {
			this.verId = Integer.valueOf(vData);
			this.verValue = Double.MAX_VALUE;
			
			if (eData.equals("")) {
				setEdges(new Integer[]{this.verId}, null);
	        	return;
			}
	        
			if (eData.equals("")) {
				setEdges(new Integer[]{this.verId}, new Double[]{0.1});
	        	return;
			}
	        
			IntDoubleEdgeSet set = 
				edgeParser.parseEdgeIdWeightArray(eData, ':');
	        setEdges(set.getEdgeIds(), set.getEdgeWeights());
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
	    public void parseVerValue(String valData) {
	    	this.verValue = Double.parseDouble(valData);
	    }
	    
		@Override
		public Double[] getWeightArray(int capacity) {
			return new Double[capacity];
		}

		@Override
		public void serEdges(ByteBuffer eOut) 
				throws EOFException, IOException {
			eOut.putInt(this.edgeNum);
	    	for (int index = 0; index < this.edgeNum; index++) {
	    		eOut.putInt(this.edgeIds[index]);
	    		
	    		eOut.putDouble(this.edgeWeights[index]);
	    	}
		}

		@Override
		public void deserEdges(ByteBuffer eIn) 
				throws EOFException, IOException {
			this.edgeNum = eIn.getInt();
	    	this.edgeIds = new Integer[this.edgeNum];
	    	this.edgeWeights = new Double[this.edgeNum];
	    	for (int index = 0; index < this.edgeNum; index++) {
	    		this.edgeIds[index] = eIn.getInt();
	    		this.edgeWeights[index] = eIn.getDouble();
	    	}
		}
		
		@Override
		public int getVerByte() {
			return 8;
		}
		
		/**
		 * 4 + (4 + 8) * this.edgeNum
		 */
		@Override
		public int getEdgeByte() {
			return (4 + (4+8)*this.edgeNum);
		}

		@Override
		public int getGraphInfoByte() {
			return 0;
		}
	}
	
	public static class SPMsgRecordDeterm extends MsgRecord<Double> {
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
	public GraphRecord<Double, Double, Double, Integer> getGraphRecord() {
		return new SPGraphRecordDeterm();
	}

	@Override
	public MsgRecord<Double> getMsgRecord() {
		return new SPMsgRecordDeterm();
	}
	
	@Override
	public boolean isAccumulated() {
		return true;
	}
}
