/**
 * copyright 2011-2016
 */
package hybridgraph.examples.pagerank.pull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.Constants.Opinion;
//import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecordInterface;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.api.MsgRecordInterface;
import org.apache.hama.myhama.util.GraphContextInterface;

import hybridgraph.examples.pagerank.pull.PageRankUserTool.PRMsgRecord;

/**
 * PageRankBSP.java implements {@link BSP}.
 * Note: with combiner.
 * 
 * The original Pregel-based variant of this algorithm was proposed in 
 * "Pregel: A System for Large-Scale Graph Processing", SIGMOG 2010.
 * 
 * @author 
 * @version 0.1
 */
public class PageRankBSP extends BSP<Double, Integer, Double, Integer> {
	public static final Log LOG = LogFactory.getLog(PageRankBSP.class);
	private static double FACTOR = 0.85f;
	private static double RandomRate;
	private double value = 0.0d;
	
	@Override
	public void taskSetup(
			GraphContextInterface<Double, Integer, Double, Integer> context) {
		//BSPJob job = context.getBSPJobInfo();
		//RandomRate = (1 - FACTOR) / (float)job.getGloVerNum();
		RandomRate = 0.15;
	}
	
	@Override
	public Opinion processThisBucket(int bucketId, int curSuperStepNum) {
		return Opinion.YES;
	}
	
	@Override
	public void update(
			GraphContextInterface<Double, Integer, Double, Integer> context) 
				throws Exception {
		GraphRecordInterface<Double, Integer, Double, Integer> graph = 
			context.getGraphRecord();
		MsgRecordInterface<Double> msg = context.getReceivedMsgRecord();
		value = 0.0d;
		
		if (context.getIteCounter() == 1) {
			//value = RandomRate;
			value = 10.0;
		} else if (msg != null){
			value = RandomRate + msg.getMsgValue()*FACTOR;
		} else {
			//value = RandomRate;
			value = 10.0;
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
			GraphContextInterface<Double, Integer, Double, Integer> context) 
				throws Exception {
		GraphRecordInterface<Double, Integer, Double, Integer> graph = 
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
