/**
 * copyright 2011-2016
 */
package hybridgraph.examples.sssp;

import java.util.Random;

import org.apache.hama.Constants.VBlockUpdateRule;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.util.Context;

import hybridgraph.examples.sssp.SPUserTool.SPMsgRecord;

/**
 * SSSPBSP.java implements {@link BSP}.
 * Note: without combiner.
 * 
 * The original Pregel-based variant of this algorithm was proposed in 
 * "Pregel: A System for Large-Scale Graph Processing", SIGMOG 2010.
 * 
 * @author
 * @version 0.1
 */
public class SPBSP extends BSP<Double, Double, Double, Integer> {
	public static final String SOURCE = "source.vertex.id";
	private static int SourceVerId;
	private static int SourceBlkId;
	private static Random rd = new Random();
	
	@Override
	public void taskSetup(
			Context<Double, Double, Double, Integer> context) {
		SourceVerId = context.getBSPJobInfo().getInt(SOURCE, 2);
		SourceBlkId = context.getVBlockId(SourceVerId);
	}
	
	@Override
	public void vBlockSetup(
			Context<Double, Double, Double, Integer> context) {
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
			Context<Double, Double, Double, Integer> context) 
				throws Exception {
		GraphRecord<Double, Double, Double, Integer> graph = 
			context.getGraphRecord();
		MsgRecord<Double> msg = context.getReceivedMsgRecord();
		
		if (context.getSuperstepCounter() == 1) {
			//first superstep, set the source vertex value 0 and send new messages.
			if (graph.getVerId() == SourceVerId) {
				graph.setVerValue(0.0);
				context.setRespond();
			}
		} else {
			//otherwise, receive messages, processing and then send new messages.
			double recMsgValue = msg.getMsgValue();
			if (recMsgValue < graph.getVerValue()) {
				graph.setVerValue(recMsgValue);
				context.setRespond();
			}
		}
		context.voteToHalt();
	}
	
	@Override
	public MsgRecord<Double>[] getMessages(
			Context<Double, Double, Double, Integer> context) 
				throws Exception {
		GraphRecord<Double, Double, Double, Integer> graph = 
			context.getGraphRecord();
		SPMsgRecord[] result = new SPMsgRecord[graph.getEdgeNum()];
		int idx = 0;
		for (int eid: graph.getEdgeIds()) {
			result[idx] = new SPMsgRecord();
			result[idx].initialize(graph.getVerId(), eid, 
					graph.getVerValue()+rd.nextDouble());
			idx++;
		}
		return result;
	}
}
