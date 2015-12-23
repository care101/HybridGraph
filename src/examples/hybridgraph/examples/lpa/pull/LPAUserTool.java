/**
 * copyright 2011-2016
 */
package hybridgraph.examples.lpa.pull;

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
 * LPAUserTool.java
 * Support for {@link LPAGraphRecord}, {@link LPAMsgRecord}.
 * 
 * @author 
 * @version 0.1
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class LPAUserTool extends UserTool<Integer, Integer, MsgBundle, Integer> {
	
	public class LPAGraphRecord extends GraphRecord<Integer, Integer, MsgBundle, Integer> {
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
		public ArrayList<GraphRecord<Integer, Integer, MsgBundle, Integer>> 
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

			ArrayList<GraphRecord<Integer, Integer, MsgBundle, Integer>> result = 
				new ArrayList<GraphRecord<Integer, Integer, MsgBundle, Integer>>();
			for (dstTid = 0; dstTid < tNum; dstTid++) {
				for (dstBid = 0; dstBid < bNum[dstTid]; dstBid++) {
					if (container[dstTid][dstBid] != null) {
						Integer[] tmpEdgeIds = new Integer[container[dstTid][dstBid].size()];
						container[dstTid][dstBid].toArray(tmpEdgeIds);
						local.updateLocMatrix(dstTid, dstBid, verId, tmpEdgeIds.length);
						LPAGraphRecord graph = new LPAGraphRecord();
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
		public MsgRecord<MsgBundle>[] getMsg(int iteStyle) {
			LPAMsgRecord[] result = new LPAMsgRecord[this.edgeNum];
			for (int i = 0; i < this.edgeNum; i++) {
				result[i] = new LPAMsgRecord();
				MsgBundle msgBundle = new MsgBundle();
				msgBundle.add(this.verValue);
				result[i].initialize(this.verId, this.edgeIds[i], msgBundle);
			}
			
			return result;
		}
	}
	
	public class LPAMsgRecord extends MsgRecord<MsgBundle> {
		
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
	public GraphRecord<Integer, Integer, MsgBundle, Integer> getGraphRecord() {
		return new LPAGraphRecord();
	}

	@Override
	public MsgRecord<MsgBundle> getMsgRecord() {
		return new LPAMsgRecord();
	}
	
	@Override
	public boolean isAccumulated() {
		return false;
	}
}
