/**
 * copyright 2011-2016
 */
package hybridgraph.examples.pagerank;

import org.apache.hama.Constants.VBlockUpdateRule;
//import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.util.Context;

import hybridgraph.examples.pagerank.PageRankUserTool.PRMsgRecord;

/**
 * PageRankBSP.java implements {@link BSP}.
 * 
 * For more details, please refer to  
 * "Pregel: A System for Large-Scale Graph Processing", SIGMOG 2010.
 * 
 * @author 
 * @version 0.1
 */
public class PageRankBSP extends BSP<Double, Integer, Double, Integer> {
	private static double FACTOR = 0.85f;
	private static double RandomRate;
	private double value = 0.0d;
	
	@Override
	public void taskSetup(
			Context<Double, Integer, Double, Integer> context) {
		//BSPJob job = context.getBSPJobInfo();
		//RandomRate = (1-FACTOR) / (float)job.getGloVerNum();
		RandomRate = 0.15;
	}
	
	@Override
	public void vBlockSetup(
			Context<Double, Integer, Double, Integer> context) {
		context.setVBlockUpdateRule(VBlockUpdateRule.UPDATE);
	}
	
	@Override
	public void update(
			Context<Double, Integer, Double, Integer> context) 
				throws Exception {
		GraphRecord<Double, Integer, Double, Integer> graph = 
			context.getGraphRecord();
		MsgRecord<Double> msg = context.getReceivedMsgRecord();
		value = 0.0d;
		
		if (context.getSuperstepCounter() == 1) {
			//value = RandomRate;
			value = 1.0;
		} else if (msg != null){
			value = RandomRate + msg.getMsgValue()*FACTOR;
		} else {
			//value = RandomRate;
			value = 1.0;
		}
		
		if (graph.getGraphInfo() > 0) {
			graph.setVerValue(value/graph.getGraphInfo());
		} else {
			graph.setVerValue(value);
		}
		context.setRespond();
	}
	
	@Override
	public MsgRecord<Double>[] getMessages(
			Context<Double, Integer, Double, Integer> context) 
				throws Exception {
		GraphRecord<Double, Integer, Double, Integer> graph = 
			context.getGraphRecord();
		PRMsgRecord[] result = new PRMsgRecord[graph.getEdgeNum()];
		int idx = 0;
		for (int eid: graph.getEdgeIds()) {
			result[idx] = new PRMsgRecord();
			result[idx].initialize(graph.getVerId(), eid, graph.getVerValue());
			idx++;
		}
		return result;
	}
}
