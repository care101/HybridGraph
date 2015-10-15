/**
 * NeuSoft Termite System
 * copyright 2011-2016
 */
package cn.edu.neu.termite.examples.sssp.pull;

import org.apache.hadoop.fs.Path;

import org.apache.hama.HamaConfiguration;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.io.KeyValueInputFormat;
import org.apache.hama.myhama.io.TextBSPFileOutputFormat;

/**
 * SSSPDriver.java
 * A driven program is used to submit the 
 * single source shortest distance computation job.
 * 
 * @author zhigang wang
 * @version 0.1
 */
public class SSSPPullDriver {
	
	public static void main(String[] args) throws Exception {
		// check the input parameters
		if (args.length != 8) {
			StringBuffer sb = 
				new StringBuffer("the sssp job must be given arguments(8):");; 
			sb.append("\n  [1] input directory on HDFS"); 
			sb.append("\n  [2] output directory on HDFS"); 
			sb.append("\n  [3] #task(int)");
			sb.append("\n  [4] #iteration(int)");
			sb.append("\n  [5] #vertex(int)");
			sb.append("\n  [6] #buckets(int)");
			sb.append("\n  [7] msg_pack for style.Pull(int, 10^4 default)");
			sb.append("\n  [8] source vertex id(int)");
			
			System.out.println(sb.toString());
			System.exit(-1);
		}

		// set the job configuration
		HamaConfiguration conf = new HamaConfiguration();
		BSPJob bsp = new BSPJob(conf, SSSPPullDriver.class);
		bsp.setJobName("SSSP-Range");
		bsp.setBspClass(SPBSP.class);
		bsp.setPriority(Constants.PRIORITY.NORMAL);
		bsp.setUserToolClass(SPUserTool.class);
		bsp.setInputFormatClass(KeyValueInputFormat.class);
		bsp.setOutputFormatClass(TextBSPFileOutputFormat.class);
		
		KeyValueInputFormat.addInputPath(bsp, new Path(args[0]));
		TextBSPFileOutputFormat.setOutputPath(bsp, new Path(args[1]));
		bsp.setNumBspTask(Integer.valueOf(args[2]));
		bsp.setNumSuperStep(Integer.parseInt(args[3]));
		bsp.setNumTotalVertices(Integer.valueOf(args[4]));
		bsp.setNumBucketsPerTask(Integer.valueOf(args[5]));
		
		bsp.setBspStyle(Constants.STYLE.Pull);
		  bsp.setMsgPackSize(Integer.valueOf(args[6]));
		
		// set the source vertex id
		bsp.setInt(SPBSP.SOURCE, Integer.valueOf(args[7]));
		
		// submit the job
		bsp.waitForCompletion(true);
	}
}
