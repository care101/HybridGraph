/**
 * copyright 2011-2016
 */
package hybridgraph.examples.mm;

import hybridgraph.examples.mm.MMBipartiteUserTool.MMBipartiteMsgRecord;

import java.util.ArrayList;
import java.util.Random;

import org.apache.hama.Constants;
import org.apache.hama.Constants.VBlockUpdateRule;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.util.Context;
import org.apache.hama.myhama.util.Null;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;

/**
 * MMBipartiteBSP.java implements {@link BSP}.
 * Note: 
 *   1) Messages of this algorithm can only be concatenated;
 *   2) The input bipartite graph is non-weighted and undirected. 
 *      Left vertex => v.id is odd; Right vertex => v.id is even.
 * 
 * Perform Random Maximal Matching (also called "edge dominating set") over a non-weighted 
 * undirected bipartite graph. This job consists of several phases, each of which can be 
 * further partitioned into four supersteps, i.e., for any vertex which has not been matched: 
 *  si0, invitation
 *    =>Left vertex broadcasts invitation messages along outgoing edges.
 *  si1, acceptance
 *    =>Right vertex randomly selects a received message to accept the invitation. 
 *      An acceptance message is sent back to the corresponding left vertex.
 *  si2, confirmation
 *    =>Left vertex randomly selects an acceptance to confirm the matching. 
 *    =>A confirmation message is sent back to the corresponding right vertex.
 *    =>Left vertex marks itself as matched (voteToHalt()).
 *  si3, marking
 *    =>Right vertex marks iteself as matched if it receives the confirmation message. 
 *    =>Each right vertex can receive at most one confirmation message.
 *    =>A new edge is found. 
 * When no new edge is found, MM converges.
 * 
 * For more details of MM, please refer to Grzegorz Malewicz's paper titled 
 * "Pregel: A System for Large-Scale Graph Processing", SIGMOD 2010.
 * 
 * @author 
 * @version 0.1
 */
public class MMBipartiteBSP 
		extends BSP<MMBipartiteValue, Null, BipartiteMsgBundle, Null> {
	//private static final Log LOG = LogFactory.getLog(MMBipartiteBSP.class);
	
	@Override
	public void superstepSetup(
			Context<MMBipartiteValue, Null, BipartiteMsgBundle, Null> context) {
		int usedsuperstep = context.getSuperstepCounter() - 1; //counter starts from 1
		switch(usedsuperstep%4) {
		case 0: 
			context.setUseEdgesInPush(true); //load edges for push
			break;
		case 1: 
		case 2:
		case 3: 
			context.setUseEdgesInPush(false); //no edge is required for push
			break;
		}
	}
	
	@Override
	public void vBlockSetup(
			Context<MMBipartiteValue, Null, BipartiteMsgBundle, Null> context) {
		context.setVBlockUpdateRule(VBlockUpdateRule.MSG_ACTIVE_DEPENDED);
	}
	
	@Override
	public void update(
			Context<MMBipartiteValue, Null, BipartiteMsgBundle, Null> context) {
		GraphRecord<MMBipartiteValue, Null, BipartiteMsgBundle, Null> graph = 
			context.getGraphRecord();
		MsgRecord<BipartiteMsgBundle> msg = context.getReceivedMsgRecord();
		int usedsuperstep = context.getSuperstepCounter() - 1; //counter starts from 1
		
		/**
		 * When launching another phase (the (i+1)-th phase), get the number of newly 
		 * found edges in the previous phase (i.e., superstep si3 at the i-th phase, i>1). 
		 * If no edge is found, MM converges.
		 */
		if ((usedsuperstep>0) && ((usedsuperstep%4)==0) 
				&& (context.getJobAgg()==0.0f)) {
			context.voteToHalt();
			return;
		}
		
		if (graph.getVerValue().isMatched()) {
			context.voteToHalt();
			return;
		}
				
		switch(usedsuperstep%4) {
		case 0: 
			graph.getVerValue().setValueFlag(-1); //reset
			if (isOdd(graph.getVerId())) {
				context.setRespond(); 
			} //left vertex broadcasts invitation messages
			break;
		case 1: 
			if (msg != null) {
				graph.getVerValue().setValueFlag(randomSelectVert(msg)); 
				context.setRespond(); 
			} //right vertex accepts one of invitations
			break;
		case 2: 
			if (msg != null) {
				graph.getVerValue().setMatchedFlag((byte)1);
				graph.getVerValue().setValueFlag(randomSelectVert(msg)); 
				context.setRespond();
				context.voteToHalt();
			} //left vertex confirms the acceptance
			break;
		case 3: 
			if (msg != null) {
				graph.getVerValue().setMatchedFlag((byte)1);
				context.voteToHalt();
				context.setVertexAgg(1.0f); //find an edge
			} //right vertex confirms the matching
			break;
		}
		
		/*if (msg != null) {
			LOG.info("    msg=" + msg.getMsgValue().get());
		}*/
	}
	
	@Override
	public MsgRecord<BipartiteMsgBundle>[] getMessages(
			Context<MMBipartiteValue, Null, BipartiteMsgBundle, Null> context) {
		GraphRecord<MMBipartiteValue, Null, BipartiteMsgBundle, Null> graph = 
			context.getGraphRecord();
		MMBipartiteMsgRecord[] result = null;
		int msgValue = graph.getVerId();
		if (graph.getVerValue().value() == -1) {
			result = new MMBipartiteMsgRecord[graph.getEdgeNum()];
			int idx = 0;
			for (int eid: graph.getEdgeIds()) {
				result[idx] = new MMBipartiteMsgRecord();
				BipartiteMsgBundle msgBundle = new BipartiteMsgBundle();
				msgBundle.add(msgValue);
				result[idx].initialize(graph.getVerId(), eid, msgBundle);
				idx++;
			}
		} else {
			int targetId = graph.getVerValue().value();
			boolean generateMsg = false;
			if (context.getIteStyle() == Constants.STYLE.PULL) {
				//PULL: Is the edge linking to targetId maintained in this Eblosk?
				for (int eid: graph.getEdgeIds()) {
					if (eid == targetId) {
						generateMsg = true;
						break;
					}
				}
			} else {
				generateMsg = true; //PUSH
			}
			
			if (generateMsg) {
				result = new MMBipartiteMsgRecord[1];
				result[0] = new MMBipartiteMsgRecord();
				BipartiteMsgBundle msgBundle = new BipartiteMsgBundle();
				msgBundle.add(msgValue);
				result[0].initialize(graph.getVerId(), targetId, msgBundle);
			}
		}
		
		return result;
	}
	
	@Override
	public int estimateNumberOfMessages(
			Context<MMBipartiteValue, Null, BipartiteMsgBundle, Null> context) {
		GraphRecord<MMBipartiteValue, Null, BipartiteMsgBundle, Null> graph = 
			context.getGraphRecord();
		
		if (graph.getVerValue().value() == -1) {
			return context.getDegree();
		} else {
			return 1;
		}
	}
	
	/**
	 * Randomly select one vertex. Non-deterministic version. 
	 * @param msg
	 * @return
	 */
	private int randomSelectVert(MsgRecord<BipartiteMsgBundle> msg) {
		Random rd = new Random();
		ArrayList<Integer> invitations = msg.getMsgValue().get();
		int idx = 0;
		if (invitations.size() > 1) {
			idx = rd.nextInt(invitations.size()-1);
		}
		int selectedVert = invitations.get(idx);
		return selectedVert;
	}
	
	/**
	 * Select the vertex with the biggest id. 
	 * Deterministic version for testing.
	 * @param msg
	 * @return
	 */
	@SuppressWarnings("unused")
	private int maxSelectVert(MsgRecord<BipartiteMsgBundle> msg) {
		int selectedVert = -1;
		for (int invitation: msg.getMsgValue().get()) {
			selectedVert = Math.max(selectedVert, invitation);
		}
		return selectedVert;
	}
	
	private boolean isOdd(int id) {
		if ((id%2) == 0) {
			return false;
		} else {
			return true;
		}
	}
}
