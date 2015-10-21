/**
 * copyright 2011-2016
 */
package hybridgraph.examples.cc.pull;

import hybridgraph.examples.cc.pull.CCUserTool.CCGraphRecord;
import hybridgraph.examples.cc.pull.CCUserTool.CCMsgRecord;

import org.apache.hama.Constants.Opinion;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.util.GraphContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * CCBSP.java implements {@link BSP}.
 * Note: input graph should be an undirected graph, and this implementation uses Combiner.
 * 
 * Implementation of the HCC algorithm that identifies connected components and
 * assigns each vertex its "component identifier" (the smallest vertex id in the
 * component)
 * 
 * The idea behind the algorithm is very simple: propagate the smallest vertex
 * id along the edges to all vertices of a connected component. The number of
 * supersteps necessary is equal to the length of the maximum diameter of all
 * components + 1
 * 
 * The original Hadoop-based variant of this algorithm was proposed by Kang,
 * Charalampos, Tsourakakis and Faloutsos in
 * "PEGASUS: Mining Peta-Scale Graphs", 2010
 * 
 * http://www.cs.cmu.edu/~ukang/papers/PegasusKAIS.pdf
 * 
 * @author 
 * @version 0.1
 */

public class CCBSP extends BSP {
	public static final Log LOG = LogFactory.getLog(CCBSP.class);
	
	private CCGraphRecord graph;
	private CCMsgRecord msg;
	
	@Override
	public Opinion processThisBucket(int _bucId, int _iteNum) {
		return Opinion.MSG_DEPEND;
	}
	
	@Override
	public void compute(GraphContext context) throws Exception {
		graph = (CCGraphRecord)context.getGraphRecord();
		msg = (CCMsgRecord)context.getMsgRecord();
		
		if (context.getIteCounter() == 1) {
			graph.setVerValue(graph.getVerId());
			update(context); //first superstep, just send its value to all outer neighbors.
		} else {
			int recMsgValue = msg.getMsgValue();
			if (recMsgValue < graph.getVerValue()) {
				graph.setVerValue(recMsgValue);
				update(context);
			}
		}
		context.voteToHalt();
	}
	
	@SuppressWarnings("unchecked")
	private void update(GraphContext context) {
		context.setUpdate();
	}
}
