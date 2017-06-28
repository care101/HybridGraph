/**
 * copyright 2011-2016
 */
package hybridgraph.examples.sssp;

import org.apache.hadoop.fs.Path;

import org.apache.hama.HamaConfiguration;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.io.KeyValueInputFormat;
import org.apache.hama.myhama.io.TextBSPFileOutputFormat;

/**
 * SPDriver.java
 * A driven program is used to submit the single source shortest distance 
 * computation job using the STYLE.PULL (i.e., b-pull) model.
 * Note: Compute shortest distance on a non-weighted directed graph. 
 *       Edge weights required to generate messages are given in a 
 *       random manner. Thus, the computation is not deterministic. 
 *       We use this implementation because the input graph size can 
 *       be largely reduced so that compared systems (e.g., Giraph) 
 *       can run normally. 
 * 
 * @author 
 * @version 0.1
 */
public class SSSPPullDriver {
	
	public static void main(String[] args) throws Exception {
		//check parameters
		if (args.length < 7) {
			StringBuffer sb = 
				new StringBuffer("\nUsage of PULL-based Shortest Distance:");
			sb.append("\n(*)required parameter");
			sb.append("\n[*]optional parameter");
			sb.append("\n   (1)input directory on HDFS"); 
			sb.append("\n   (2)output directory on HDFS"); 
			sb.append("\n   (3)#tasks(int)");
			sb.append("\n   (4)#iterations(int)");
			sb.append("\n   (5)#vertices(int)");
			sb.append("\n   (6)#buckets(int)");
			sb.append("\n   (7)source vertex id(int)");
			
			sb.append("\n   [8]msg_pack for style.Pull(int, 10^4 default)");
			sb.append("\n   [9]checkpoint policy:");
			sb.append("\n      0 => NONE, default");
			sb.append("\n      1 => CompleteRecoveryDynCkp");
			sb.append("\n      2 => ConfinedRecoveryLogMsg");
			sb.append("\n      3 => ConfinedRecoveryLogVert");
			sb.append("\n   [10]checkpoint interval (int, 0 default)");
			sb.append("\n   [11]failure iteration (int, 0 default)");
			sb.append("\n   [12]#failed tasks (int, 0 default)");
			
			System.out.println(sb.toString());
			System.exit(-1);
		}

		//configurate the job
		HamaConfiguration conf = new HamaConfiguration();
		BSPJob bsp = new BSPJob(conf, SSSPPullDriver.class);
		bsp.setJobName("Single Source Shortest Distance");
		bsp.setPriority(Constants.PRIORITY.NORMAL);

		bsp.setBspClass(SPBSP.class);
		bsp.setUserToolClass(SPUserTool.class);
		bsp.setInputFormatClass(KeyValueInputFormat.class);
		bsp.setOutputFormatClass(TextBSPFileOutputFormat.class);
		
		KeyValueInputFormat.addInputPath(bsp, new Path(args[0]));
		TextBSPFileOutputFormat.setOutputPath(bsp, new Path(args[1]));
		bsp.setNumBspTask(Integer.valueOf(args[2]));
		bsp.setNumSuperStep(Integer.parseInt(args[3]));
		bsp.setNumTotalVertices(Integer.valueOf(args[4]));
		bsp.setNumBucketsPerTask(Integer.valueOf(args[5]));
		
		bsp.setBspStyle(Constants.STYLE.PULL);
		bsp.setGraphDataOnDisk(true);
		
		//set the source vertex id
		bsp.setInt(SPBSP.SOURCE, Integer.valueOf(args[6]));
		
		
		//=======================//
		//  optional parameters  //
		//=======================//
		if (args.length >= 8) {
			bsp.setMsgPackSize(Integer.valueOf(args[7]));
		}
		
		if (args.length >= 9) {
			bsp.setCheckPointPolicy(
					Constants.CheckPoint.Policy.values()[Integer.valueOf(args[8])]);
		}
		
		if (args.length >= 10) {
			bsp.setCheckPointInterval(Integer.valueOf(args[9]));
		}
		
		if (args.length >= 11) {
			bsp.setFailedIteration(Integer.valueOf(args[10]));
		}
		
		if (args.length >= 12) {
			bsp.setNumOfFailedTasks(Integer.valueOf(args[11]));
		}
		
		
		//submit the job
		bsp.waitForCompletion(true);
	}
}
