/**
 * copyright 2011-2016
 */
package hybridgraph.examples.mm;

import org.apache.hadoop.fs.Path;

import org.apache.hama.HamaConfiguration;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.io.KeyValueInputFormat;
import org.apache.hama.myhama.io.TextBSPFileOutputFormat;

/**
 * MMBipartiteDriver.java
 * A driven program is used to submit the Random Maximal Matching (MM) 
 * computation job using the STYLE.PULL model.
 * Note: a non-weighted undirected bipartite graph.
 * 
 * @author 
 * @version 0.1
 */
public class MMBipartitePullDriver {
	
	public static void main(String[] args) throws Exception {
		//check parameters
		if (args.length < 6) {
			StringBuffer sb = 
				new StringBuffer("\nUsage of PULL-based Random Maximal Matching (MM):");
			sb.append("\n(*)required parameter");
			sb.append("\n[*]optional parameter");
			sb.append("\n   (1)input directory on HDFS (undirected bipartite graph)"); 
			sb.append("\n   (2)output directory on HDFS"); 
			sb.append("\n   (3)#tasks(int)");
			sb.append("\n   (4)#iterations(int)");
			sb.append("\n   (5)#vertices(int)");
			sb.append("\n   (6)#buckets(int)");
			
			sb.append("\n   [7]msg_pack for style.PULL(int, 10^4 default)");
			
			System.out.println(sb.toString());
			System.exit(-1);
		}

		//configurate the job
		HamaConfiguration conf = new HamaConfiguration();
		BSPJob bsp = new BSPJob(conf, MMBipartitePullDriver.class);
		bsp.setJobName("Random Maximal Matching");
		bsp.setPriority(Constants.PRIORITY.NORMAL);		

		bsp.setBspClass(MMBipartiteBSP.class);
		bsp.setUserToolClass(MMBipartiteUserTool.class);
		bsp.setInputFormatClass(KeyValueInputFormat.class);
		bsp.setOutputFormatClass(TextBSPFileOutputFormat.class);
		
		KeyValueInputFormat.addInputPath(bsp, new Path(args[0]));
		TextBSPFileOutputFormat.setOutputPath(bsp, new Path(args[1]));
		bsp.setNumBspTask(Integer.parseInt(args[2]));
		bsp.setNumSuperStep(Integer.parseInt(args[3]));
		bsp.setNumTotalVertices(Integer.valueOf(args[4]));
		bsp.setNumBucketsPerTask(Integer.valueOf(args[5]));
		
		bsp.setBspStyle(Constants.STYLE.PULL);
		bsp.setGraphDataOnDisk(true);
		bsp.setMiniSuperStep(true);
		
		//=======================//
		//  optional parameters  //
		//=======================//
		if (args.length >= 7) {
			bsp.setMsgPackSize(Integer.valueOf(args[6]));
		}
		
		//submit the job
		bsp.waitForCompletion(true);
	}
}
