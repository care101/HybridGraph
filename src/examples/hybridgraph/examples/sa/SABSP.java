/**
 * copyright 2011-2016
 */
package hybridgraph.examples.sa;

import java.util.ArrayList;
import java.util.HashMap;
//import java.util.Random;
import java.util.Map.Entry;

import org.apache.hama.Constants.VBlockUpdateRule;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.util.Context;

import hybridgraph.examples.sa.SAUserTool.SAMsgRecord;

/**
 * SABSP.java implements {@link BSP}.
 * Note: 1) messages of this algorithm can only be concatenated.
 *       2) non-weighted directed graph.
 * 
 * Implement a simple but efficient simulating advertisement algorithm. 
 * The basic idea can refer to Zuhair Khayyat et al. 
 * "Mizan: A System for Dynamic Load Balancing in Large-scale Graph Processing", 
 * EuroSys 2013.
 * Here, we implement this algorithm based on LPA:
 * (1) an unique Integer, aId, is used to indicate an advertisement;
 * (2) each vertex maintain one advertisement 
 *     with the highest popularity among the vertex's incoming neighbors;
 * (3) a vertex value is initialized with the vertex id (aid), 
 *     denoting the advertisement a user likes;
 * (4) at each superstep, one vertex update its value using a candidate 
 *     advertisement which satisfies the following constraints: 
 *     1) a maximum number of the vertex's incoming neighbors have, 
 *        i.e., the highest popularity,
 *     and, 
 *     2) it is not equal with the vertex's current value, 
 *        or it popularity is more than the vertex's current value's.
 * 
 * @author 
 * @version 0.1
 */
public class SABSP extends BSP<Value, Integer, MsgBundle, Integer> {
	public static final String SOURCE = "source.vertex.id";
	private static int SourceVerId;
	private static int SourceBlkId;
	
	@Override
	public void taskSetup(
			Context<Value, Integer, MsgBundle, Integer> context) {
		SourceVerId = context.getBSPJobInfo().getInt(SOURCE, 2);
		SourceBlkId = context.getVBlockId(SourceVerId);
	}
	
	@Override
	public void vBlockSetup(
			Context<Value, Integer, MsgBundle, Integer> context) {
		//At the first superstep, only VBlock which SourceVertex belongs to 
		//will be processed, i.e., invoking update() for any vertex in that VBlock.
		//Otherwise, the condition of processing one VBLock is that 
		//there exist messages sent to its vertices.
		if (context.getSuperstepCounter() > 1) {
			context.setVBlockUpdateRule(VBlockUpdateRule.MSG_DEPEND);
		} else if (context.getVBlockId() == SourceBlkId) {
			context.setVBlockUpdateRule(VBlockUpdateRule.UPDATE);
		} else {
			context.setVBlockUpdateRule(VBlockUpdateRule.SKIP);
		}
	}
	
	@Override
	public void update(
			Context<Value, Integer, MsgBundle, Integer> context) {
		GraphRecord<Value, Integer, MsgBundle, Integer> graph = 
			context.getGraphRecord();
		MsgRecord<MsgBundle> msg = context.getReceivedMsgRecord();
		
		//At the first superstep, send source vertex's advertisement to its out-neighbors.
		if (context.getSuperstepCounter() == 1) {
			if (graph.getVerId() == SourceVerId) {
				context.setRespond();
			}
		} else if (msg!=null && (msg.getMsgValue()!=null) && (msg.getMsgValue().getAll()!=null)) {
			//Otherwise, update value and send new advertisement or not.
			Value canV = findNewValue(msg);
			if ((graph.getVerValue().getAdverId()!=canV.getAdverId()) ||
					(graph.getVerValue().getAdverIdNum()<canV.getAdverIdNum())) {
				graph.setVerValue(canV);
				context.setRespond();
				context.setVertexAgg(1.0f);
			}
		}
		
		context.voteToHalt();
	}
	
	@Override
	public MsgRecord<MsgBundle>[] getMessages(
			Context<Value, Integer, MsgBundle, Integer> context) {
		GraphRecord<Value, Integer, MsgBundle, Integer> graph = 
			context.getGraphRecord();
		SAMsgRecord[] result = new SAMsgRecord[graph.getEdgeNum()];
		
		int idx = 0;
		for (int eid: graph.getEdgeIds()) {
			result[idx] = new SAMsgRecord();
			MsgBundle msgBundle = new MsgBundle();
			msgBundle.add(graph.getVerValue().getAdverId());
			result[idx].initialize(graph.getVerId(), eid, msgBundle);
			idx++;
		}
		
		return result;
	}
	
	/**
	 * Find new candidate Value.
	 * @return
	 */
	private Value findNewValue(MsgRecord<MsgBundle> msg) {
		HashMap<Integer, Integer> recAIds = 
			new HashMap<Integer, Integer>(
					msg.getMsgValue().getAll().size()); //aId:count
		for (int aId: msg.getMsgValue().getAll()) {
			if (recAIds.containsKey(aId)) {
				int count = recAIds.get(aId);
				recAIds.put(aId, ++count);
			} else {
				recAIds.put(aId, 1);
			}
		} //compute the number of each aId received from its neighbors
		
		int max = 0;
		ArrayList<Integer> candidates = new ArrayList<Integer>();
		for (Entry<Integer, Integer> e: recAIds.entrySet()) {
			if (max < e.getValue()) {
				max = e.getValue();
				candidates.clear();
				candidates.add(e.getKey());
			} else if (max == e.getValue()) {
				candidates.add(e.getKey());
			}
		} //candidate aIds with maximum counter
		
		//normally, random choose a label from candidate labels,
		//but here, just select the label with the maximum label-value to 
		//perform deterministic computations
		int maxAid = 0;
		for (int label: candidates) {
			maxAid = Math.max(maxAid, label);
		}
		return new Value(maxAid, max);
		
/*		Random rd = new Random(); //random choose an aId from candidate labels
		int idx = 0;
		if (candidates.size() > 2) {
			idx = rd.nextInt(candidates.size()-1);
		}
		return new Value(candidates.get(idx), max);*/
	}
}
