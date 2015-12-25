/**
 * copyright 2011-2016
 */
package hybridgraph.examples.lpa.pull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.hama.Constants.Opinion;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.util.GraphContextInterface;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import hybridgraph.examples.lpa.pull.LPAUserTool.LPAGraphRecord;
import hybridgraph.examples.lpa.pull.LPAUserTool.LPAMsgRecord;

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
	public static final Log LOG = LogFactory.getLog(LPABSP.class);
	
	@Override
	public Opinion processThisBucket(int _bucId, int _iteNum) {
		return Opinion.YES;
	}
	
	@Override
	public void update(
			GraphContextInterface<Integer, Integer, MsgBundle, Integer> context) 
				throws Exception {
		LPAGraphRecord graph = (LPAGraphRecord)context.getGraphRecord();
		LPAMsgRecord msg = (LPAMsgRecord)context.getReceivedMsgRecord();
		
		/** first superstep, just send its value to all outer neighbors */
		if (context.getIteCounter() == 1) {
			graph.setVerValue(graph.getVerId());
		} else if (msg != null) {
			graph.setVerValue(findLabel(msg));
		}
		
		context.setRespond(); //always active per iteration
	}
	
	@Override
	public MsgRecord<MsgBundle>[] getMessages(
			GraphContextInterface<Integer, Integer, MsgBundle, Integer> context) 
				throws Exception {
		LPAGraphRecord graph = (LPAGraphRecord)context.getGraphRecord();
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
	
	private int findLabel(LPAMsgRecord msg) {
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
