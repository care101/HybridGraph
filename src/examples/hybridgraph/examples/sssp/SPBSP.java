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
 * SPBSP.java implements {@link BSP}.
 * Note: Compute shortest distance on a non-weighted directed graph. 
 *       Edge weights required to generate messages are given in a 
 *       random manner. Thus, the computation is not deterministic. 
 *       We use this implementation because the input graph size can 
 *       be largely reduced so that compared systems (e.g., Giraph) 
 *       can run normally. 
 * 
 * For more details, please refer to 
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
		SPMsgRecord[] result = new SPMsgRecord[graph.getEdgeNum()];
		int idx = 0;
		//Generate a message based on a random edge weight and then send it to a neighbor. 
		for (int eid: graph.getEdgeIds()) {
			result[idx] = new SPMsgRecord();
			result[idx].initialize(graph.getVerId(), eid, 
					graph.getVerValue()+rd.nextDouble());
			idx++;
		}
		return result;
	}
}
