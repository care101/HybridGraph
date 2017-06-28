/**
 * copyright 2011-2016
 */
package hybridgraph.examples.lpa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
//import java.util.Random;
import org.apache.hama.Constants.VBlockUpdateRule;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.util.Context;

import hybridgraph.examples.lpa.LPAUserTool.LPAMsgRecord;

/**
 * LPABSP.java implements {@link BSP}.
 * Note: 
 *   1) messages of this algorithm can only be concatenated;
 *   2) the input graph can be directed/undirected, but here we 
 *      use a directed graph.
 * 
 * Implement a simple but efficient community detection method 
 * based on the label propagation algorithm (LPA). 
 * This is used to find non-overlapping communities.
 * 
 * For more details of LPA, please refer to Usha Nandini Raghavan 
 * et al., "Near linear time algorithm to detect community structures 
 * in large-scale networks", Physical Review E, 2007, 76(3): 036106.
 * 
 * @author 
 * @version 0.1
 */
public class LPABSP extends BSP<Integer, Integer, MsgBundle, Integer> {
	
	@Override
	public void vBlockSetup(
			Context<Integer, Integer, MsgBundle, Integer> context) {
		context.setVBlockUpdateRule(VBlockUpdateRule.UPDATE);
	}
	
	@Override
	public void update(
			Context<Integer, Integer, MsgBundle, Integer> context) {
		GraphRecord<Integer, Integer, MsgBundle, Integer> graph = 
			context.getGraphRecord();
		MsgRecord<MsgBundle> msg = context.getReceivedMsgRecord();
		boolean isUpdated = false;
		
		//at the 1st superstep, initialize the value to its id and then 
		//broadcast the value to all outer neighbors at the 2nd superstep
		if (context.getSuperstepCounter() == 1) {
			graph.setVerValue(graph.getVerId());
			isUpdated = true;
		} else if (msg!=null && (msg.getMsgValue()!=null) && (msg.getMsgValue().getAll()!=null)) {
			int newLabel = findLabel(msg);
			if (graph.getVerValue() != newLabel) {
				graph.setVerValue(newLabel);
				isUpdated = true;
			}
		}
		
		if (isUpdated) {
			context.setVertexAgg(1.0f);
		}
		
		if ((context.getSuperstepCounter()>1) 
				&& (context.getJobAgg()==0.0f)) {
			context.voteToHalt();//no update, LPA converges
		} else {
			context.setRespond();//always broadcast messages
		}
	}
	
	@Override
	public MsgRecord<MsgBundle>[] getMessages(
			Context<Integer, Integer, MsgBundle, Integer> context) {
		GraphRecord<Integer, Integer, MsgBundle, Integer> graph = 
			context.getGraphRecord();
		LPAMsgRecord[] result = new LPAMsgRecord[graph.getEdgeNum()];
		int idx = 0;
		for (int eid: graph.getEdgeIds()) {
			result[idx] = new LPAMsgRecord();
			MsgBundle msgBundle = new MsgBundle();
			msgBundle.add(graph.getVerValue());
			result[idx].initialize(graph.getVerId(), eid, msgBundle);
			idx++;
		}
		
		return result;
	}
	
	private int findLabel(MsgRecord<MsgBundle> msg) {
		HashMap<Integer, Integer> clusteredLabels = 
			new HashMap<Integer, Integer>(
					msg.getMsgValue().getAll().size()); //labelId:count
		for (int label: msg.getMsgValue().getAll()) {
			if (clusteredLabels.containsKey(label)) {
				int count = clusteredLabels.get(label);
				clusteredLabels.put(label, ++count);
			} else {
				clusteredLabels.put(label, 1);
			}
		}//compute the number of each label received from its neighbors
		
		int max = 0;
		ArrayList<Integer> candidates = new ArrayList<Integer>();
		for (Entry<Integer, Integer> e: clusteredLabels.entrySet()) {
			if (max < e.getValue()) {
				max = e.getValue();
				candidates.clear();
				candidates.add(e.getKey());
			} else if (max == e.getValue()) {
				candidates.add(e.getKey());
			}
		}//candidate labels with maximum counter
		
		//normally, random choose a label from candidate labels,
		//but here, just select the label with the maximum label-value to 
		//perform deterministic computations
		max = 0;
		for (int label: candidates) {
			max = Math.max(max, label);
		}
		return max;
		
		/*Random rd = new Random(); //random choose a label from candidates
		int idx = 0;
		if (candidates.size() > 2) {
			idx = rd.nextInt(candidates.size()-1);
		}
		return candidates.get(idx);*/
	}
}
