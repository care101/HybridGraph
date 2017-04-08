/**
 * copyright 2011-2016
 */
package hybridgraph.examples.cc;

import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.util.Context;

import hybridgraph.examples.cc.CCUserTool.CCMsgRecord;

/**
 * CCBSP.java implements {@link BSP}.
 * Note: input graph should be undirected.
 * 
 * Implementation of the HCC algorithm that identifies connected components and
 * assigns each vertex its "component identifier" (the smallest vertex id in the
 * component)
 * 
 * The idea behind the algorithm is very simple: propagate the smallest vertex
 * id along the edges to all vertices of a connected component.
 * 
 * The original Hadoop-based variant of this algorithm was proposed by Kang,
 * Charalampos, Tsourakakis and Faloutsos in
 * "PEGASUS: Mining Peta-Scale Graphs", 2010.
 * http://www.cs.cmu.edu/~ukang/papers/PegasusKAIS.pdf
 * 
 * @author 
 * @version 0.1
 */

public class CCBSP extends BSP<Integer, Integer, Integer, Integer> {
	
	@Override
	public void update(
			Context<Integer, Integer, Integer, Integer> context) {
		GraphRecord<Integer, Integer, Integer, Integer> graph = 
			context.getGraphRecord();
		MsgRecord<Integer> msg = context.getReceivedMsgRecord();
		
		//At the first superstep, just send its value to all outgoing neighbors.
		if (context.getSuperstepCounter() == 1) {
			graph.setVerValue(graph.getVerId());
			context.setRespond();
		} else {
			int recMsgValue = msg.getMsgValue();
			if (recMsgValue < graph.getVerValue()) {
				graph.setVerValue(recMsgValue);
				context.setRespond();
			}
		}
		context.voteToHalt();
	}
	
	@Override
	public MsgRecord<Integer>[] getMessages(
			Context<Integer, Integer, Integer, Integer> context) {
		GraphRecord<Integer, Integer, Integer, Integer> graph = 
			context.getGraphRecord();
		CCMsgRecord[] result = new CCMsgRecord[graph.getEdgeNum()];
		int idx = 0;
		for (int eid: graph.getEdgeIds()) {
			result[idx] = new CCMsgRecord();
			result[idx].initialize(graph.getVerId(), eid, graph.getVerValue());
			idx++;
		}
		
		return result;
	}
}
