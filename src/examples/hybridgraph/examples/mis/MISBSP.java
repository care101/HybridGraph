/**
 * copyright 2011-2016
 */
package hybridgraph.examples.mis;

import hybridgraph.examples.mis.MISUserTool.MISMsgRecord;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hama.Constants.VBlockUpdateRule;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.util.Context;

/**
 * MISBSP.java implements {@link BSP}.
 * Note: 1) input graph is non-weighted undirected;
 *       2) any vertex v without neighbouring vertices is linked to itself.
 * 
 * Find maximal independent sets in a given undirected graph. This job consists 
 * of several phases, each of which can be further partitioned into two supersteps: 
 *   si0: find a new vertex u that should be added into Maximal Independent Sets "S";
 *   si1: put neighbouring vertices of u into "NotInS".
 * The algorithm converges when each vertex is in "S" or "NotInS". 
 * 
 * For more details, please refer to 
 * "Catch the Wind: Graph Workload Balancing on Cloud", Shang et al., ICDE2013, 
 * "Distributed computing: a locality-sensitive approach", Peleg D., SIAM, 2000,
 * and a Hama implementation "http://blog.csdn.net/xin_jmail/article/details/32101483".
 * 
 * @author 
 * @version 0.1
 */

public class MISBSP extends BSP<Integer, Integer, Integer, Integer> {
	//private static final Log LOG = LogFactory.getLog(MISBSP.class);
	
	@Override
	public void vBlockSetup(
			Context<Integer, Integer, Integer, Integer> context) {
		context.setVBlockUpdateRule(VBlockUpdateRule.MSG_ACTIVE_DEPENDED);
	}
	
	@Override
	public void update(
			Context<Integer, Integer, Integer, Integer> context) {
		GraphRecord<Integer, Integer, Integer, Integer> graph = 
			context.getGraphRecord();
		MsgRecord<Integer> msg = context.getReceivedMsgRecord();
		int superstep = context.getSuperstepCounter();
		
		//At the 1st superstep, initialize value to 0->unknown.
		if (superstep == 1) {
			graph.setVerValue(0);
			context.setRespond();
		} else {
			switch(superstep%2) {
			case 0: 
				/**
				 * A vertex v is put into the maximal independent set (S), if:
				 *  1) v's status is unknown, i.e., v.value is 0;
				 *  2) for any neighbouring vertex u of v, either a) or b) is true: 
				 *     a) u's status is unknown and u.id>v.id;
				 *     b) u has been put into NotInS (u.value is 2);
				 *     We can check a) and b) by broadcasting u.id if u is unknown, 
				 *     or Integer.MAX_VALUE if u is NotInS.
				 * If v is put into S, v needs to notify its every neighbouring vertex 
				 * u that u must be put into NotInS. Otherwise, v is still unknown and 
				 * it will be computed in remaining iterations. Fewer messages.
				 */
				if (graph.getVerValue() == 0) {
					if ((msg==null) || (graph.getVerId()<=msg.getMsgValue())) {
						graph.setVerValue(1); //v is put into S
						context.setRespond(); //value is 1, broadcast 2
						context.voteToHalt();
					}
				} else {
					//v with known status (1 or 2) may receive messages, but do nothing.
					context.voteToHalt(); 
				}
				break;
			case 1: 
				if (msg != null) {
					/**
					 * If v is firstly put into NotInS, it must notifies all neighbouring 
					 * vertex u that its status has changed. Otherwise, u may be always 
					 * active when (v,u) \in E and v.id<=u.id. Many messages.
					 */
					if (graph.getVerValue() != 2) {
						graph.setVerValue(2); //v is put into NotInS
						context.setRespond(); //value is 2, broadcast Integer.MAX_VALUE
					}
					context.voteToHalt();
				} else {
					context.setRespond(); //value is 0, broadcast v.id
				}
				break;
			}
		}
	}
	
	@Override
	public MsgRecord<Integer>[] getMessages(
			Context<Integer, Integer, Integer, Integer> context) {
		GraphRecord<Integer, Integer, Integer, Integer> graph = 
			context.getGraphRecord();
		MISMsgRecord[] result = new MISMsgRecord[graph.getEdgeNum()];
		int msgValue = -1;
		switch(graph.getVerValue()) {
		case 0: msgValue = graph.getVerId(); break; //for condition-a
		case 1: msgValue = 2; break; //put neighbouring vertex into NoInS
		case 2: msgValue = Integer.MAX_VALUE; break; //for condition-b
		}
		
		int idx = 0;
		for (int eid: graph.getEdgeIds()) {
			result[idx] = new MISMsgRecord();
			result[idx].initialize(graph.getVerId(), eid, msgValue);
			idx++;
		}
		
		return result;
	}
	
	@Override
	public int estimateNumberOfMessages(
			Context<Integer, Integer, Integer, Integer> context) {
		return context.getDegree();
	}
}
