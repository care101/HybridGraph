/**
 * copyright 2011-2016
 */
package hybridgraph.examples.sa.pull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.api.UserTool;

/**
 * LPAUserTool.java
 * Support for {@link SAGraphRecord}, {@link SAMsgRecord}.
 * 
 * <K>, vertex value, send value;
 * <W>, message value and edge weight;
 * <I>, graphInfo;
 * 
 * @author 
 * @version 0.1
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class SAUserTool 
		extends UserTool<Value, Integer, MsgBundle, Integer> {
	
	public static class SAGraphRecord 
			extends GraphRecord<Value, Integer, MsgBundle, Integer> {
		
		public SAGraphRecord() {
			this.verValue = new Value();
		}
		
		@Override
	    public void initGraphData(String vData, String eData) {
			int length = 0, begin = 0, end = 0;
			this.verId = Integer.valueOf(vData);
			this.verValue = new Value(this.verId, 1);
			
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
			this.verValue.write(vOut);
		}

		@Override
		public void deserVerValue(ByteBuffer vIn) throws EOFException, IOException {
			this.verValue.read(vIn);
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
			return this.verValue.getByteSize();
		}
		
		@Override
		public int getGraphInfoByte() {
			return 0;
		}
		
		/**
		 * 4 + 4 * this.edgeNum
		 */
		@Override
		public int getEdgeByte() {
			return (4 + 4*this.edgeNum);
		}
	}
	
	public static class SAMsgRecord extends MsgRecord<MsgBundle> {
		
		@Override
		public void combiner(MsgRecord<MsgBundle> msg) {
			this.msgValue.combine(msg.getMsgValue().getAll());
		}
		
		@Override
		public int getMsgByte() {
			return this.msgValue==null? 8:(4+this.msgValue.getByteSize());
		}
		
		@Override
		public void deserialize(DataInputStream in) throws IOException {
			this.dstId = in.readInt();
			this.msgValue = new MsgBundle();
			this.msgValue.read(in);
		}
		
		@Override
		public void serialize(DataOutputStream out) throws IOException {
			out.writeInt(this.dstId);
			this.msgValue.write(out);
		}
	}
	
	@Override
	public GraphRecord<Value, Integer, MsgBundle, Integer> getGraphRecord() {
		return new SAGraphRecord();
	}

	@Override
	public MsgRecord<MsgBundle> getMsgRecord() {
		return new SAMsgRecord();
	}
	
	@Override
	public boolean isAccumulated() {
		return false;
	}
}
