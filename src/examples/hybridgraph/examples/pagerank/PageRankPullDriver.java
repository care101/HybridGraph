/**
 * copyright 2011-2016
 */
package hybridgraph.examples.pagerank;

import org.apache.hadoop.fs.Path;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.io.KeyValueInputFormat;
import org.apache.hama.myhama.io.TextBSPFileOutputFormat;

/**
 * PageRankDriver.java
 * A driven program is used to submit the pagerank computation job.
 * 
 * @author 
 * @version 0.1
 */
public class PageRankPullDriver {
	
	public static void main(String[] args) throws Exception {
		//check parameters
		if (args.length < 6) {
			StringBuffer sb = new StringBuffer("Usage of PULL-based PageRank:"); 
			sb.append("\n  (1)input directory on HDFS"); 
			sb.append("\n  (2)output directory on HDFS"); 
			sb.append("\n  (3)#tasks(int)");
			sb.append("\n  (4)#iterations(int)");
			sb.append("\n  (5)#vertices");
			sb.append("\n  (6)#buckets");
			
			sb.append("\n  [7]msg_pack for style.PULL(int, 10^4 default)");
			sb.append("\n  [8]checkpoint policy:");
			sb.append("\n       0 => NONE, default");
			sb.append("\n       1 => CompleteRecovery");
			sb.append("\n       2 => ConfinedRecoveryLogMsg");
			sb.append("\n       3 => ConfinedRecoveryLogVert");
			sb.append("\n  [9]checkpoint interval (int, -1 default)");
			sb.append("\n  [10]failure iteration (int, -1 default)");
			sb.append("\n  [11]#failed tasks (int, 0 default)");
			
			System.out.println(sb.toString());
			System.exit(-1);
		}
		
		//configurate job
		HamaConfiguration conf = new HamaConfiguration();
		BSPJob bsp = new BSPJob(conf, PageRankPullDriver.class);
		bsp.setPriority(Constants.PRIORITY.NORMAL);
		bsp.setJobName("PageRank");
		
		bsp.setBspClass(PageRankBSP.class);
		bsp.setUserToolClass(PageRankUserTool.class);
		bsp.setInputFormatClass(KeyValueInputFormat.class);
		bsp.setOutputFormatClass(TextBSPFileOutputFormat.class);
		
		KeyValueInputFormat.addInputPath(bsp, new Path(args[0]));
		TextBSPFileOutputFormat.setOutputPath(bsp, new Path(args[1]));
		bsp.setNumBspTask(Integer.parseInt(args[2]));
		bsp.setNumSuperStep(Integer.parseInt(args[3]));
		bsp.setNumTotalVertices(Integer.valueOf(args[4]));
		bsp.setNumBucketsPerTask(Integer.valueOf(args[5]));
		
		bsp.setGraphDataOnDisk(true);
		bsp.setBspStyle(Constants.STYLE.Pull);
		bsp.useGraphInfoInUpdate(true);
		
		
		//=======================//
		//  optional parameters  //
		//=======================//
		if (args.length >= 7) {
			bsp.setMsgPackSize(Integer.valueOf(args[6]));
		}
		
		if (args.length >= 8) {
			bsp.setCheckPointPolicy(
					Constants.CheckPoint.Policy.values()[Integer.valueOf(args[7])]);
		}
		
		if (args.length >= 9) {
			bsp.setCheckPointInterval(Integer.valueOf(args[8]));
		}
		if (args.length >= 10) {
			bsp.setFailedIteration(Integer.valueOf(args[9]));
		}
		if (args.length >= 11) {
			bsp.setNumOfFailedTasks(Integer.valueOf(args[10]));
		}
		
		//submit the job
		bsp.waitForCompletion(true);
	}
}
