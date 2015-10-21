/**
 * copyright 2011-2016
 */
package hybridgraph.examples.sssp.pull;

import hybridgraph.examples.sssp.pull.SPUserTool.SPGraphRecord;
import hybridgraph.examples.sssp.pull.SPUserTool.SPMsgRecord;

import org.apache.hama.Constants.Opinion;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.util.GraphContext;
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
public class SPBSP extends BSP {
	public static final Log LOG = LogFactory.getLog(SPBSP.class);
	public static final String SOURCE = "source.vertex.id";
	private static int SourceVerId;
	private static int SourceBucId;
	
	private SPGraphRecord graph;
	private SPMsgRecord msg;
	
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
		graph = (SPGraphRecord)context.getGraphRecord();
		msg = (SPMsgRecord)context.getMsgRecord();
		
		if (context.getIteCounter() == 1) {
			// First SuperStep, set the source vertex value 0 and send new messages.
			if (graph.getVerId() == SourceVerId) {
				update(context, 0);
			}
		} else {
			// Other SuperStep, receive messages, processing and then send new messages.
			double recMsgValue = msg.getMsgValue();
			if (recMsgValue < graph.getVerValue()) {
				update(context, recMsgValue);
			}
		}
		context.voteToHalt();
	}
	
	private void update(GraphContext context, double verValue) {
		graph.setVerValue(verValue);
		context.setUpdate();
	}
}
