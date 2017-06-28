/**
 * copyright 2011-2016
 */
package hybridgraph.examples.mm;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.api.UserTool;
import org.apache.hama.myhama.io.EdgeParser;
import org.apache.hama.myhama.util.Null;

/**
 * MMBipartiteUserTool.java
 * Support for {@link MMBipartiteGraphRecord}, {@link MMBipartiteMsgRecord}.
 * 
 * @author 
 * @version 0.1
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class MMBipartiteUserTool 
		extends UserTool<MMBipartiteValue, Null, BipartiteMsgBundle, Null> {
	private static EdgeParser edgeParser = new EdgeParser();
	
	public static class MMBipartiteGraphRecord 
			extends GraphRecord<MMBipartiteValue, Null, BipartiteMsgBundle, Null> {
		
		public MMBipartiteGraphRecord() {
			this.verValue = new MMBipartiteValue();
		}
		
		@Override
	    public void parseGraphData(String vData, String eData) {
			this.verId = Integer.valueOf(vData);
			this.verValue = new MMBipartiteValue((byte)0, -1);
			
			setEdges(edgeParser.parseEdgeIdArrayFilterBipartiteGraph(
					eData, ':', this.verId), null);
	    }

		@Override
		public void serVerValue(ByteBuffer vOut) 
				throws EOFException, IOException {
			this.verValue.write(vOut);
		}

		@Override
		public void deserVerValue(ByteBuffer vIn) 
				throws EOFException, IOException {
			this.verValue.read(vIn);
		}
		
	    @Override
	    public void parseVerValue(String vData) {
	    	this.verValue.parseValue(vData);
	    }
	    
		@Override
		public Null[] getWeightArray(int capacity) {
			return null;
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
	
	public static class MMBipartiteMsgRecord extends MsgRecord<BipartiteMsgBundle> {
		
		@Override
		public void combiner(MsgRecord<BipartiteMsgBundle> msg) {
			this.msgValue.combine(msg.getMsgValue().get());
		}
		
		@Override
		public int getMsgByte() {
			return this.msgValue==null? 
					12:(4+this.msgValue.getByteSize());
		}
		
		@Override
	    public void serialize(ByteBuffer out) throws IOException {
	    	out.putInt(this.dstId);
	    	this.msgValue.write(out);
	    }
	    
		@Override
	    public void deserialize(ByteBuffer in) throws IOException {
	    	this.dstId = in.getInt();
	    	this.msgValue = new BipartiteMsgBundle();
	    	this.msgValue.read(in);
	    }
		
		@Override
		public void serialize(DataOutputStream out) throws IOException {
			out.writeInt(this.dstId);
			this.msgValue.write(out);
		}
		
		@Override
		public void deserialize(DataInputStream in) throws IOException {
			this.dstId = in.readInt();
			this.msgValue = new BipartiteMsgBundle();
			this.msgValue.read(in);
		}
		
		@Override
		public MMBipartiteMsgRecord clone() {
			BipartiteMsgBundle val = new BipartiteMsgBundle();
			for (int label: this.getMsgValue().get()) {
				val.add(label);
			}
			MMBipartiteMsgRecord msg = new MMBipartiteMsgRecord();
			msg.initialize(srcId, dstId, val);
			return msg;
		}
	}
	
	@Override
	public GraphRecord<MMBipartiteValue, Null, BipartiteMsgBundle, Null> 
			getGraphRecord() {
		return new MMBipartiteGraphRecord();
	}

	@Override
	public MsgRecord<BipartiteMsgBundle> getMsgRecord() {
		return new MMBipartiteMsgRecord();
	}
	
	@Override
	public boolean isAccumulated() {
		return false;
	}
}
