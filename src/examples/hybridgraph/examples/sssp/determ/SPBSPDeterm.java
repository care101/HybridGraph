/**
 * copyright 2011-2016
 */
package hybridgraph.examples.sssp.determ;

import org.apache.hama.Constants.VBlockUpdateRule;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.util.Context;

import hybridgraph.examples.sssp.determ.SPUserToolDeterm.SPMsgRecordDeterm;

/**
 * SPBSPDeterm.java implements {@link BSP}.
 * Note: Compute shortest distance on a weighted directed graph. The 
 *       deterministic edge weights can guarantee deterministic computations. 
 *       We use this deterministic implementation to test the performance 
 *       of our fault-tolerance mechanisms. 
 * 
 * For more details, please refer to 
 * "Pregel: A System for Large-Scale Graph Processing", SIGMOG 2010.
 * 
 * @author
 * @version 0.1
 */
public class SPBSPDeterm extends BSP<Double, Double, Double, Integer> {
	public static final String SOURCE = "source.vertex.id";
	private static int SourceVerId;
	private static int SourceBlkId;
	
	@Override
	public void taskSetup(
			Context<Double, Double, Double, Integer> context) {
		SourceVerId = context.getBSPJobInfo().getInt(SOURCE, 2); 
		SourceBlkId = context.getVBlockId(SourceVerId);
	}
	
	@Override
	public void vBlockSetup(
			Context<Double, Double, Double, Integer> context) {
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
			Context<Double, Double, Double, Integer> context) {
		GraphRecord<Double, Double, Double, Integer> graph = 
			context.getGraphRecord();
		MsgRecord<Double> msg = context.getReceivedMsgRecord();
		
		if (context.getSuperstepCounter() == 1) {
			//At the first superstep, set the source vertex value as 0.
			if (graph.getVerId() == SourceVerId) {
				graph.setVerValue(0.0);
				context.setRespond();
			}
		} else {
			//Otherwise, update vertex value based on received messages.
			//Received messages have been combined into a single one.
			double recMsgValue = msg.getMsgValue();
			if (recMsgValue < graph.getVerValue()) {
				graph.setVerValue(recMsgValue);
				context.setRespond();
				context.setVertexAgg(1.0f);
			} //The updated value should be broadcasted to neighbors.
		}
		//Deactive itself. An inactive vertex becomes active automically 
		//if it receives messages at the next superstep.
		context.voteToHalt();
	}
	
	@Override
	public MsgRecord<Double>[] getMessages(
			Context<Double, Double, Double, Integer> context) {
		GraphRecord<Double, Double, Double, Integer> graph = 
			context.getGraphRecord();
		SPMsgRecordDeterm[] result = new SPMsgRecordDeterm[graph.getEdgeNum()];
		Integer[] eids = graph.getEdgeIds();
		Double[] eweights = graph.getEdgeWeights();
		for (int idx = 0; idx < graph.getEdgeNum(); idx++) {
			result[idx] = new SPMsgRecordDeterm();
			result[idx].initialize(graph.getVerId(), eids[idx], 
					graph.getVerValue()+eweights[idx]);
		}
		
		return result;
	}
}
