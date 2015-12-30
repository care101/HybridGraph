/**
 * copyright 2011-2016
 */
package hybridgraph.examples.lpa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.hama.Constants.VBlockUpdateRule;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.util.Context;

import hybridgraph.examples.lpa.LPAUserTool.LPAMsgRecord;

/**
 * LPABSP.java implements {@link BSP}.
 * This algorithm cannot use Combiner.
 * 
 * Implement a simple but efficient community detection method 
 * based on the label propagation algorithm (LPA). 
 * This is used to find non-overlapping communities.
 * 
 * The details of LPA can refer to Usha Nandini Raghavan et al. 
 * "Near linear time algorithm to detect community structures 
 * in large-scale networks", 
 * Physical Review E, 2007, 76(3): 036106.
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
			Context<Integer, Integer, MsgBundle, Integer> context) 
				throws Exception {
		GraphRecord<Integer, Integer, MsgBundle, Integer> graph = 
			context.getGraphRecord();
		MsgRecord<MsgBundle> msg = context.getReceivedMsgRecord();
		
		/** first superstep, just send its value to all outer neighbors */
		if (context.getSuperstepCounter() == 1) {
			graph.setVerValue(graph.getVerId());
		} else if (msg != null) {
			graph.setVerValue(findLabel(msg));
		}
		
		context.setRespond(); //always active per iteration
	}
	
	@Override
	public MsgRecord<MsgBundle>[] getMessages(
			Context<Integer, Integer, MsgBundle, Integer> context) 
				throws Exception {
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
		HashMap<Integer, Integer> recLabels = 
			new HashMap<Integer, Integer>(
					msg.getMsgValue().getAll().size()); //labelId:count
		for (int label: msg.getMsgValue().getAll()) {
			if (recLabels.containsKey(label)) {
				int count = recLabels.get(label);
				recLabels.put(label, ++count);
			} else {
				recLabels.put(label, 1);
			}
		}//compute the number of each label received from its neighbors
		
		int max = 0;
		ArrayList<Integer> cands = new ArrayList<Integer>();
		for (Entry<Integer, Integer> e: recLabels.entrySet()) {
			if (max < e.getValue()) {
				max = e.getValue();
				cands.clear();
				cands.add(e.getKey());
			} else if (max == e.getValue()) {
				cands.add(e.getKey());
			}
		}//candidate labels with maximum counter
		
		//random choose a label from candidate labels
		Random rd = new Random();
		int idx = 0;
		if (cands.size() > 2) {
			idx = rd.nextInt(cands.size()-1);
		}
		return cands.get(idx);
	}
}
