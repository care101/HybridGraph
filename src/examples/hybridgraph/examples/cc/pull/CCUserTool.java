/**
 * copyright 2011-2016
 */
package hybridgraph.examples.cc.pull;

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
 * CCUserTool.java
 * Support for {@link CCGraphRecord}, {@link CCMsgRecord}.
 * 
 * @author 
 * @version 0.1
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class CCUserTool extends UserTool<Integer, Integer, Integer, Integer> {
	
	public static class CCGraphRecord 
			extends GraphRecord<Integer, Integer, Integer, Integer> {
		@Override
	    public void initGraphData(String vData, String eData) {
			int length = 0, begin = 0, end = 0;
			this.verId = Integer.valueOf(vData);
			this.verValue = this.verId;
			
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
	            tmpEdgeId.add(Integer.valueOf(new String(edges, begin, end - begin)));
	            begin = ++end;
	        }
	        tmpEdgeId.add(Integer.valueOf(new String(edges, begin, end - begin)));
	        
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
			vOut.putInt(this.verValue);
		}

		@Override
		public void deserVerValue(ByteBuffer vIn) 
				throws EOFException, IOException {
			this.verValue = vIn.getInt();
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
			return 4;
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
	
	public static class CCMsgRecord extends MsgRecord<Integer> {	
		@Override
		public void combiner(MsgRecord<Integer> msg) {
			if (this.msgValue > msg.getMsgValue()) {
				this.msgValue = msg.getMsgValue();
			}
		}
		
		@Override
		public int getMsgByte() {
			return 8; //4+4
		}
		
		@Override
		public void deserialize(DataInputStream in) throws IOException {
			this.dstId = in.readInt();
			this.msgValue = in.readInt();
		}
		
		@Override
		public void serialize(DataOutputStream out) throws IOException {
			out.writeInt(this.dstId);
			out.writeInt(this.msgValue);
		}
	}
	
	@Override
	public GraphRecord<Integer, Integer, Integer, Integer> 
			getGraphRecord() {
		return new CCGraphRecord();
	}

	@Override
	public MsgRecord<Integer> getMsgRecord() {
		return new CCMsgRecord();
	}
	
	@Override
	public boolean isAccumulated() {
		return true;
	}
}
