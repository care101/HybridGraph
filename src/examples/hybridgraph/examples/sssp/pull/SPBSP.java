/**
 * copyright 2011-2016
 */
package hybridgraph.examples.sssp.pull;

import java.util.Random;

import hybridgraph.examples.sssp.pull.SPUserTool.SPGraphRecord;
import hybridgraph.examples.sssp.pull.SPUserTool.SPMsgRecord;

import org.apache.hama.Constants.Opinion;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.util.GraphContext;
import org.apache.hama.myhama.util.GraphContextInterface;
import org.apache.hama.myhama.util.TaskContext;
import org.apache.hama.myhama.util.SuperStepContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


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
	public static final Log LOG = LogFactory.getLog(SPBSP.class);
	public static final String SOURCE = "source.vertex.id";
	private static int SourceVerId;
	private static int SourceBucId;
	private static Random rd = new Random();
	
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
	public void update(
			GraphContextInterface<Double, Double, Double, Integer> context) 
				throws Exception {
		SPGraphRecord graph = (SPGraphRecord)context.getGraphRecord();
		SPMsgRecord msg = (SPMsgRecord)context.getReceivedMsgRecord();
		
		if (context.getIteCounter() == 1) {
			//first superStep, set the source vertex value 0 and send new messages.
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
			GraphContextInterface<Double, Double, Double, Integer> context) 
				throws Exception {
		SPGraphRecord graph = (SPGraphRecord)context.getGraphRecord();
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
