/**
 * NeuSoft Termite System
 * copyright 2011-2016
 */
package cn.edu.neu.termite.examples.cluster.pull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.monitor.LocalStatistics;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.api.UserTool;
import org.apache.hama.myhama.comm.CommRouteTable;

/**
 * SemiCluster-UserTool.java
 * Support for {@link SCGraphRecord}, {@link SCMsgRecord}.
 * 
 * @author zhigang wang
 * @version 0.1
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class SCUserTool extends UserTool<SemiClusterSet, Double, MsgBundle, EdgeBackup> {
	public static final Log LOG = LogFactory.getLog(SCUserTool.class);
	
	private int maxClusters = 2;
	private int maxCapacity = 2;
	private double scoreFactor = 0.5d;
	
	public class SCGraphRecord 
		extends GraphRecord<SemiClusterSet, Double, MsgBundle, EdgeBackup> {
		private int maxClusters = 0;
		private int maxCapacity = 0;
		private double scoreFactor = 0.0d;
		
		public void setMetadata(int _maxClusters, int _maxCapacity, double _scoreFactor) {
			maxClusters = _maxClusters;
			maxCapacity = _maxCapacity;
			scoreFactor = _scoreFactor;
		}
		
		@Override
	    public void initGraphData(String vData, String eData) {
			int length = 0, begin = 0, end = 0;
			this.verId = Integer.valueOf(vData);
		        
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
	        Double[] edgeW = new Double[tmpEdgeId.size()];
	        Arrays.fill(edgeW, 0.01);
	        setEdges(tmpTransEdgeId, edgeW);
	        
	        this.graphInfo = new EdgeBackup();
	        this.graphInfo.setEdges(tmpEdgeId.size(), tmpTransEdgeId, edgeW);
	        
	        SemiCluster sc = new SemiCluster(maxClusters);
			sc.addVertex(this, scoreFactor);
			SemiClusterSet scList = new SemiClusterSet(maxClusters, maxCapacity);
			scList.add(sc);
			this.verValue = scList;
	    }
		
		@Override
		public void serVerId(MappedByteBuffer vOut) throws EOFException, IOException {
			vOut.putInt(this.verId);
		}

		public void deserVerId(MappedByteBuffer vIn) throws EOFException, IOException {
			this.verId = vIn.getInt();
		}

		public void serVerValue(MappedByteBuffer vOut) throws EOFException, IOException {
			this.verValue.setMetadata(maxClusters, maxCapacity);
			this.verValue.write(vOut, this.verId);
		}

		public void deserVerValue(MappedByteBuffer vIn) throws EOFException, IOException {
			this.verValue = new SemiClusterSet();
			this.verValue.setMetadata(maxClusters, maxCapacity);
			this.verValue.readFields(vIn, this.verId);
		}

		public void serGrapnInfo(MappedByteBuffer eOut) throws EOFException, IOException {
			this.graphInfo.write(eOut);
		}
		public void deserGraphInfo(MappedByteBuffer eIn) throws EOFException, IOException {
			this.graphInfo = new EdgeBackup();
			this.graphInfo.readFields(eIn);
		}

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
			return new SemiClusterSet(maxClusters, maxCapacity).getMaxByte();
		}
		
		@Override
		public int getGraphInfoByte() {
			return this.graphInfo.getByteSize();
		}
		
		/**
		 * 4 + 4 * this.edgeNum
		 * Here, outgoing edges have not weight information.
		 */
		@Override
		public int getEdgeByte() {
			return (4 + 4*this.edgeNum);
		}
		
		@Override
		public ArrayList<GraphRecord<SemiClusterSet, Double, MsgBundle, EdgeBackup>> 
    			decompose(CommRouteTable commRT, LocalStatistics local) {
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

			ArrayList<GraphRecord<SemiClusterSet, Double, MsgBundle, EdgeBackup>> result = 
				new ArrayList<GraphRecord<SemiClusterSet, Double, MsgBundle, EdgeBackup>>();
			for (dstTid = 0; dstTid < tNum; dstTid++) {
				for (dstBid = 0; dstBid < bNum[dstTid]; dstBid++) {
					if (conIds[dstTid][dstBid] != null) {
						Integer[] tmpEdgeIds = new Integer[conIds[dstTid][dstBid].size()];
						conIds[dstTid][dstBid].toArray(tmpEdgeIds);
						local.updateLocMatrix(dstTid, dstBid, verId, tmpEdgeIds.length);
						SCGraphRecord graph = new SCGraphRecord();
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
			SCMsgRecord[] result = new SCMsgRecord[this.edgeNum];
			for (int i = 0; i < this.edgeNum; i++) {
				result[i] = new SCMsgRecord();
				MsgBundle msgBundle = new MsgBundle();
				msgBundle.add(this.verValue);
				result[i].initialize(this.verId, this.edgeIds[i], msgBundle);
			}
			
			return result;
		}
	}
	
	public class SCMsgRecord extends MsgRecord<MsgBundle> {
		
		@Override
		public void combiner(MsgRecord<MsgBundle> msg) {
			this.msgValue.combine(msg.getMsgValue().getAll());
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
	public GraphRecord<SemiClusterSet, Double, MsgBundle, EdgeBackup> getGraphRecord() {
		SCGraphRecord sc = new SCGraphRecord();
		sc.setMetadata(maxClusters, maxCapacity, scoreFactor);
		return sc;
	}
	
	@Override
	public MsgRecord<MsgBundle> getMsgRecord() {
		SCMsgRecord msgRecord = new SCMsgRecord();
		return new SCMsgRecord();
	}
	
	@Override
	public boolean isAccumulated() {
		return false;
	}
}
