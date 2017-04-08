/**
 * copyright 2011-2016
 */
package hybridgraph.examples.pagerank;

import org.apache.hama.Constants.VBlockUpdateRule;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.util.Context;

import hybridgraph.examples.pagerank.PageRankUserTool.PRMsgRecord;

/**
 * PageRankBSP.java implements {@link BSP}.
 * 
 * For u, its PageRank score 
 * PR(u) = (1-d)*(1/#vertices) + d*sum(PR(v)/v.outDegree), 
 * where, d = 0.85, and v belongs to N_{in}(u), i.e., the 
 * in-neighbor set of u. After enough iterations, the sume 
 * of all vertices's scores is supposed to be 1.0.
 * 
 * For more details, please refer to  
 * "Pregel: A System for Large-Scale Graph Processing", SIGMOG 2010.
 * 
 * @author 
 * @version 0.1
 */
public class PageRankBSP extends BSP<Double, Integer, Double, Integer> {
	private static double d = 0.85f;
	private static double random = 0.15f;
	private double value = 0.0d;
	
	@Override
	public void taskSetup(
			Context<Double, Integer, Double, Integer> context) {
		random = (1-d) * (1.0f/context.getBSPJobInfo().getNumTotalVertices());
	}
	
	@Override
	public void vBlockSetup(
			Context<Double, Integer, Double, Integer> context) {
		context.setVBlockUpdateRule(VBlockUpdateRule.UPDATE);
	}
	
	@Override
	public void update(
			Context<Double, Integer, Double, Integer> context) {
		GraphRecord<Double, Integer, Double, Integer> graph = 
			context.getGraphRecord();
		MsgRecord<Double> msg = context.getReceivedMsgRecord();
		value = 0.0d;
		
		if (context.getSuperstepCounter() == 1) {
			value = random;
		} else if (msg != null){
			value = random + msg.getMsgValue()*d;
		} else {
			value = random;
		}
		
		if (graph.getGraphInfo() > 0) {
			graph.setVerValue(value/graph.getGraphInfo());
		} else {
			graph.setVerValue(value);
		}
		context.setRespond();
		context.setVertexAgg((float)value); //compute the sum of PageRank scores
	}
	
	@Override
	public MsgRecord<Double>[] getMessages(
			Context<Double, Integer, Double, Integer> context) {
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
