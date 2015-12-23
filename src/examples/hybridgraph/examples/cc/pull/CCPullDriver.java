/**
 * copyright 2011-2016
 */
package hybridgraph.examples.cc.pull;

import org.apache.hadoop.fs.Path;

import org.apache.hama.HamaConfiguration;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.io.KeyValueInputFormat;
import org.apache.hama.myhama.io.TextBSPFileOutputFormat;

/**
 * CCDriver.java
 * A driven program is used to submit the connected component computation job.
 * 
 * @author 
 * @version 0.1
 */
public class CCPullDriver {
	
	public static void main(String[] args) throws Exception {
		//check the input parameters
		if (args.length != 7) {
			StringBuffer sb = 
				new StringBuffer("the connected component job must be given arguments(7):"); 
			sb.append("\n  [1] input directory on HDFS"); 
			sb.append("\n  [2] output directory on HDFS");
			sb.append("\n  [3] #tasks(int)");
			sb.append("\n  [4] #iteration(int)");
			sb.append("\n  [5] #vertex");
			sb.append("\n  [6] #bucket");
			sb.append("\n  [7] msg_pack for style.Pull(int, 10^4 default)");
			
			System.out.println(sb.toString());
			System.exit(-1);
		}

		//set the job configuration
		HamaConfiguration conf = new HamaConfiguration();
		BSPJob bsp = new BSPJob(conf, CCPullDriver.class);
		bsp.setJobName("CC-Range");
		bsp.setBspClass(CCBSP.class);
		bsp.setPriority(Constants.PRIORITY.NORMAL);		
		bsp.setUserToolClass(CCUserTool.class);
		bsp.setInputFormatClass(KeyValueInputFormat.class);
		bsp.setOutputFormatClass(TextBSPFileOutputFormat.class);
		
		KeyValueInputFormat.addInputPath(bsp, new Path(args[0]));
		TextBSPFileOutputFormat.setOutputPath(bsp, new Path(args[1]));
		bsp.setNumBspTask(Integer.parseInt(args[2]));
		bsp.setNumSuperStep(Integer.parseInt(args[3]));
		bsp.setNumTotalVertices(Integer.valueOf(args[4]));
		bsp.setNumBucketsPerTask(Integer.valueOf(args[5]));
		
		bsp.setBspStyle(Constants.STYLE.Pull);
		  bsp.setMsgPackSize(Integer.valueOf(args[6]));
		
		bsp.setGraphDataOnDisk(true);
		
		//submit the job
		bsp.waitForCompletion(true);
	}
}
