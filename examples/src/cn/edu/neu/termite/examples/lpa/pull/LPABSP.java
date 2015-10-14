/**
 * NeuSoft Termite System
 * copyright 2011-2016
 */
package cn.edu.neu.termite.examples.lpa.pull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.hama.Constants.Opinion;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.util.GraphContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.edu.neu.termite.examples.lpa.pull.LPAUserTool.LPAGraphRecord;
import cn.edu.neu.termite.examples.lpa.pull.LPAUserTool.LPAMsgRecord;

/**
 * LPABSP.java implements {@link BSP}.
 * Note: the input graph should be an undirected graph. This algorithm cannot use Combiner.
 * 
 * Implement a simple but efficient community detection method 
 * based on the label propagation algorithm (LPA). This is used to find 
 * non-overlapping communities.
 * 
 * The details of LPA can refer to Usha Nandini Raghavan et al. 
 * "Near linear time algorithm to detect community structures in large-scale networks", 
 * Physical Review E, 2007, 76(3): 036106.
 * 
 * @author zhigang wang
 * @version 0.1
 */
public class LPABSP extends BSP {
	public static final Log LOG = LogFactory.getLog(LPABSP.class);
	
	private LPAGraphRecord graph;
	private LPAMsgRecord msg;
	
	@Override
	public Opinion processThisBucket(int _bucId, int _iteNum) {
		return Opinion.YES;
	}
	
	@Override
	public void compute(GraphContext context) throws Exception {
		graph = (LPAGraphRecord)context.getGraphRecord();
		msg = (LPAMsgRecord)context.getMsgRecord();
		
		if (context.getIteCounter() == 1) {
			/** first superstep, just send its value to all outer neighbors */
			graph.setVerValue(graph.getVerId());
		} else if (msg != null) {
			graph.setVerValue(findLabel());
		}
		
		context.setUpdate(); //always active per iteration
	}
	
	private int findLabel() {
		HashMap<Integer, Integer> recLabels = 
			new HashMap<Integer, Integer>(msg.getMsgValue().getAll().size()); //labelId:count
		for (int label: msg.getMsgValue().getAll()) {
			if (recLabels.containsKey(label)) {
				int count = recLabels.get(label);
				recLabels.put(label, ++count);
			} else {
				recLabels.put(label, 1);
			}
		} //compute the number of each label received from its neighbors
		
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
		} //candidate labels with maximum counter
		
		Random rd = new Random(); //random choose a label from candidate labels
		int idx = 0;
		if (cands.size() > 2) {
			idx = rd.nextInt(cands.size()-1);
		}
		return cands.get(idx);
	}
}
