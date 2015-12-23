/**
 * copyright 2011-2016
 */
package hybridgraph.examples.pagerank.pull;

import hybridgraph.examples.pagerank.pull.PageRankUserTool.PRGraphRecord;
import hybridgraph.examples.pagerank.pull.PageRankUserTool.PRMsgRecord;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.Constants.Opinion;
//import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.util.GraphContext;
import org.apache.hama.myhama.util.TaskContext;


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
public class PageRankBSP extends BSP {
	public static final Log LOG = LogFactory.getLog(PageRankBSP.class);
	private static double FACTOR = 0.85f;
	private static double RandomRate;
	
	private PRGraphRecord graphRecord;
	private PRMsgRecord msgRecord;
	private double value = 0.0d;
	
	@Override
	public void taskSetup(TaskContext context) {
		//BSPJob job = context.getBSPJob();
		//RandomRate = (1 - FACTOR) / (float)job.getGloVerNum();
		RandomRate = 0.15;
	}
	
	@Override
	public Opinion processThisBucket(int bucketId, int curSuperStepNum) {
		return Opinion.YES;
	}
	
	@Override
	public void compute(GraphContext context) throws Exception {
		graphRecord = (PRGraphRecord)context.getGraphRecord();
		msgRecord = (PRMsgRecord)context.getMsgRecord();
		value = 0.0d;
		
		if (context.getIteCounter() == 1) {
			//value = RandomRate;
			value = 10.0;
		} else if (msgRecord != null){
			value = RandomRate + msgRecord.getMsgValue() * FACTOR;
		} else {
			//value = RandomRate;
			value = 10.0;
		}
		
		if (graphRecord.getGraphInfo() > 0) {
			graphRecord.setVerValue(value/graphRecord.getGraphInfo());
			if (graphRecord.getVerId() == 0) {
			}
		} else {
			graphRecord.setVerValue(value);
		}
		context.setUpdate();
	}
}
