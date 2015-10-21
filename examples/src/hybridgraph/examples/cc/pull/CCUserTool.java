/**
 * copyright 2011-2016
 */
package hybridgraph.examples.cc.pull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;

import org.apache.hama.monitor.LocalStatistics;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.api.UserTool;
import org.apache.hama.myhama.comm.CommRouteTable;

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
 * @param <S> send value
 */
public class CCUserTool extends UserTool<Integer, Integer, Integer, Integer> {
	
	public class CCGraphRecord extends GraphRecord<Integer, Integer, Integer, Integer> {
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
		public void serVerId(MappedByteBuffer vOut) throws EOFException, IOException {
			vOut.putInt(this.verId);
		}

		public void deserVerId(MappedByteBuffer vIn) throws EOFException, IOException {
			this.verId = vIn.getInt();
		}

		public void serVerValue(MappedByteBuffer vOut) throws EOFException, IOException {
			vOut.putInt(this.verValue);
		}

		public void deserVerValue(MappedByteBuffer vIn) throws EOFException, IOException {
			this.verValue = vIn.getInt();
		}
		
		public void serGrapnInfo(MappedByteBuffer eOut) throws EOFException, IOException {}
		public void deserGraphInfo(MappedByteBuffer eIn) throws EOFException, IOException {}

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
		
		@Override
		public ArrayList<GraphRecord<Integer, Integer, Integer, Integer>> 
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

			ArrayList<GraphRecord<Integer, Integer, Integer, Integer>> result = 
				new ArrayList<GraphRecord<Integer, Integer, Integer, Integer>>();
			for (dstTid = 0; dstTid < tNum; dstTid++) {
				for (dstBid = 0; dstBid < bNum[dstTid]; dstBid++) {
					if (container[dstTid][dstBid] != null) {
						Integer[] tmpEdgeIds = new Integer[container[dstTid][dstBid].size()];
						container[dstTid][dstBid].toArray(tmpEdgeIds);
						local.updateLocMatrix(dstTid, dstBid, verId, tmpEdgeIds.length);
						CCGraphRecord graph = new CCGraphRecord();
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
		public MsgRecord<Integer>[] getMsg(int iteStyle) {
			CCMsgRecord[] result = new CCMsgRecord[this.edgeNum];
			for (int i = 0; i < this.edgeNum; i++) {
				result[i] = new CCMsgRecord();
				result[i].initialize(this.verId, this.edgeIds[i], this.verValue);
			}
			
			return result;
		}
	}
	
	public class CCMsgRecord extends MsgRecord<Integer> {
		
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
	public GraphRecord<Integer, Integer, Integer, Integer> getGraphRecord() {
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
