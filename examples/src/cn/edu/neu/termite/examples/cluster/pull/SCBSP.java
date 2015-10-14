/**
 * NeuSoft Termite System
 * copyright 2011-2016
 */
package cn.edu.neu.termite.examples.cluster.pull;

import java.util.Iterator;

import org.apache.hama.Constants.Opinion;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.util.GraphContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.edu.neu.termite.examples.cluster.pull.SCUserTool.SCGraphRecord;
import cn.edu.neu.termite.examples.cluster.pull.SCUserTool.SCMsgRecord;

/**
 * SCBSP.java implements {@link BSP}.
 * Note: the input graph should be an undirected graph. This algorithm cannot use Combiner.
 * 
 * The original Pregel-based variant of this algorithm was proposed in 
 * "Pregel: A System for Large-Scale Graph Processing", SIGMOG 2010.
 * 
 * The input to the algorithm is an undirected weighted graph and the output 
 * is a set of clusters with each vertex potentially belonging to multiple
 * clusters.
 * 
 * A semi-cluster is assigned a score S=(I-f*B)/(V(V-1)/2), where I is the sum
 * of weights of all internal edges, B is the sum of weights of all boundary
 * edges, V is the number of vertices in the semi-cluster, f is a user-specified
 * boundary edge score factor with a value between 0 and 1. 
 * 
 * Each vertex maintains a list containing a maximum number of  semi-clusters, 
 * sorted by score. The lists gets greedily updated in an iterative manner.
 * 
 * The algorithm finishes when the semi-cluster lists don't change or after a
 * maximum number of iterations. 
 * 
 * The implementation is referred to:
 * https://github.com/grafos-ml/okapi/blob/master/src/main/java/
 * ml/grafos/okapi/graphs/SemiClustering.java
 * 
 * @author zhigang wang
 * @version 0.1
 */
public class SCBSP extends BSP {
	public static final Log LOG = LogFactory.getLog(SCBSP.class);
	
	private int maxClusters = 2;
	private int maxCapacity = 2;
	private double scoreFactor = 0.5d;
	private SCGraphRecord graph;
	private SCMsgRecord msg;
	
	@Override
	public Opinion processThisBucket(int _bucId, int _iteNum) {
		return Opinion.YES;
	}
	
	@Override
	public void compute(GraphContext context) throws Exception {
		graph = (SCGraphRecord)context.getGraphRecord();
		msg = (SCMsgRecord)context.getMsgRecord();
		
		if (context.getIteCounter() == 1) {
			//first superstep, just send its value to all outer neighbors.
			//do nothing, since vertex value has been initialized during loading graph data.
		} else if (msg != null) {
			//graph.getVerValue().clear();
			for (SemiClusterSet clusterSet: msg.getMsgValue().getAll()) {
				for (SemiCluster cluster: clusterSet) {
					boolean contains = cluster.getVertices().contains(graph.getVerId());
			        if (!contains && cluster.getVertices().size() < maxCapacity) {
			          SemiCluster newCluster = new SemiCluster(cluster);
			          newCluster.addVertex(graph, scoreFactor);
			          graph.getVerValue().add(newCluster);
			        } else if (contains) {
			          graph.getVerValue().add(cluster);
			        }
				}
			}
			
			Iterator<SemiCluster> iterator = graph.getVerValue().iterator();
		    while(graph.getVerValue().size()>maxClusters) {
		    	iterator.next();
		    	iterator.remove();
		    }
		}
		
		//graph.getVerValue().setMetadata(maxClusters, maxCapacity);
		context.setUpdate(); //always active per iteration
	}
}
