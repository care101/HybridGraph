/**
 * NeuSoft Termite System
 * copyright 2011-2016
 */
package cn.edu.neu.termite.examples.sa.push;

import org.apache.hadoop.fs.Path;

import org.apache.hama.HamaConfiguration;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.io.KeyValueInputFormat;
import org.apache.hama.myhama.io.TextBSPFileOutputFormat;

/**
 * SADriver.java
 * A driven program is used to submit the simulate advertisement job.
 * 
 * @author zhigang wang
 * @version 0.2
 */
public class SAPushDriver {
	
	public static void main(String[] args) throws Exception {
		// check the input parameters
		if (args.length != 9) {
			StringBuffer sb = 
				new StringBuffer("the sa job must be given arguments(9):");
			sb.append("\n  [1] input directory on HDFS"); 
			sb.append("\n  [2] output directory on HDFS"); 
			sb.append("\n  [3] #task(int)");
			sb.append("\n  [4] #iteration(int)"); 
			sb.append("\n  [5] #vertex(int)");
			sb.append("\n  [6] #buckets(int)");
			sb.append("\n  [7] send_buffer for style.Push(int, 10^4 default)");
			sb.append("\n  [8] receive_buffer for style.Push(int, 10^4 default)");
			sb.append("\n  [9] source vertex id(int)");
			
			System.out.println(sb.toString());
			System.exit(-1);
		}

		//set the job configuration
		HamaConfiguration conf = new HamaConfiguration();
		BSPJob bsp = new BSPJob(conf, SAPushDriver.class);
		bsp.setJobName("SA");
		bsp.setBspClass(SABSP.class);
		bsp.setPriority(Constants.PRIORITY.NORMAL);		
		bsp.setUserToolClass(SAUserTool.class);
		bsp.setInputFormatClass(KeyValueInputFormat.class);
		bsp.setOutputFormatClass(TextBSPFileOutputFormat.class);
		
		KeyValueInputFormat.addInputPath(bsp, new Path(args[0]));
		TextBSPFileOutputFormat.setOutputPath(bsp, new Path(args[1]));
		bsp.setNumBspTask(Integer.parseInt(args[2]));
		bsp.setNumSuperStep(Integer.parseInt(args[3]));
		bsp.setNumTotalVertices(Integer.valueOf(args[4]));
		bsp.setNumBucketsPerTask(Integer.valueOf(args[5]));
		
		bsp.setBspStyle(Constants.STYLE.Push);
		  bsp.setMsgSendBufSize(Integer.valueOf(args[6]));
		  bsp.setMsgRecBufSize(Integer.valueOf(args[7]));
		
		// set the source vertex id
		bsp.setInt(SABSP.SOURCE, Integer.valueOf(args[8]));
		
		//submit the job
		bsp.waitForCompletion(true);
	}
}
