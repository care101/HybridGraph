/**
 * NeuSoft Termite System
 * copyright 2011-2016
 */
package cn.edu.neu.termite.examples.sa.pull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.hama.Constants.Opinion;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.util.GraphContext;
import org.apache.hama.myhama.util.SuperStepContext;
import org.apache.hama.myhama.util.TaskContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.edu.neu.termite.examples.sa.pull.SAUserTool.SAGraphRecord;
import cn.edu.neu.termite.examples.sa.pull.SAUserTool.SAMsgRecord;

/**
 * SABSP.java implements {@link BSP}.
 * Note: this algorithm cannot use Combiner.
 * 
 * Implement a simple but efficient simulating advertisement method. 
 * The idea can refer to Zuhair Khayyat et al. 
 * "Mizan: A System for Dynamic Load Balancing in Large-scale Graph Processing", EuroSys 2013.
 * Here, we implement this application based on LPA:
 * (1) an unique Integer is used to indicate an advertisement;
 * (2) each vertex only maintain one advertisement;
 * (3) each vertex is initialized with its vertex id (aid), denoting the advertisement it has;
 * (4) at each iteration, each vertex adopts a label that: 
 *     1) a maximum number of its neighbors have,
 *     2) the aId is not equal with its current aId, or the number is more than current number;
 * 
 * @author zhigang wang
 * @version 0.1
 */
public class SABSP extends BSP {
	public static final Log LOG = LogFactory.getLog(SABSP.class);
	public static final String SOURCE = "source.vertex.id";
	private static int SourceVerId;
	private static int SourceBucId;
	
	private SAGraphRecord graph;
	private SAMsgRecord msg;
	
	@Override
	public void taskSetup(TaskContext context) {
		BSPJob job = context.getBSPJob();
		SourceVerId = job.getInt(SOURCE, 2);
		LOG.info(SOURCE + "=" + SourceVerId);
	}
	
	@Override
	public void superstepSetup(SuperStepContext context) {
		BSPJob job = context.getBSPJob();
		SourceBucId = (SourceVerId - job.getLocMinVerId()) 
			/ job.getLocHashBucLen();
	}
	
	@Override
	public Opinion processThisBucket(int _bucId, int _iteNum) {
		if (_iteNum > 1) {
			return Opinion.MSG_DEPEND;
		}
		if (_bucId == SourceBucId) {
			return Opinion.YES;
		} else {
			return Opinion.NO;
		}
	}
	
	@Override
	public void compute(GraphContext context) throws Exception {
		graph = (SAGraphRecord)context.getGraphRecord();
		msg = (SAMsgRecord)context.getMsgRecord();
		
		if (context.getIteCounter() == 1) {
			// First SuperStep, send source vertex's advertisement to its out-neighbors.
			if (graph.getVerId() == SourceVerId) {
				update(context, graph.getVerValue());
			}
		} else if (msg != null) {
			// Other SuperStep, update value and send new advertisement or not.
			Value canV = findNewValue();
			if ((graph.getVerValue().getAdverId() != canV.getAdverId()) ||
					(graph.getVerValue().getAdverIdNum() < canV.getAdverIdNum())) {
				update(context, canV);
			}
		}
		
		context.voteToHalt();
	}
	
	private void update(GraphContext context, Value verValue) {
		graph.setVerValue(verValue);
		context.setUpdate();
	}
	
	/**
	 * Find new candidate Value.
	 * @return
	 */
	private Value findNewValue() {
		HashMap<Integer, Integer> recAIds = 
			new HashMap<Integer, Integer>(msg.getMsgValue().getAll().size()); //aId:count
		for (int aId: msg.getMsgValue().getAll()) {
			if (recAIds.containsKey(aId)) {
				int count = recAIds.get(aId);
				recAIds.put(aId, ++count);
			} else {
				recAIds.put(aId, 1);
			}
		} //compute the number of each aId received from its neighbors
		
		int max = 0;
		ArrayList<Integer> cands = new ArrayList<Integer>();
		for (Entry<Integer, Integer> e: recAIds.entrySet()) {
			if (max < e.getValue()) {
				max = e.getValue();
				cands.clear();
				cands.add(e.getKey());
			} else if (max == e.getValue()) {
				cands.add(e.getKey());
			}
		} //candidate aIds with maximum counter
		
		Random rd = new Random(); //random choose an aId from candidate labels
		int idx = 0;
		if (cands.size() > 2) {
			idx = rd.nextInt(cands.size()-1);
		}
		return new Value(cands.get(idx), max);
	}
}
