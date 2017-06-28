/**
 * copyright 2011-2016
 */
package hybridgraph.examples.mis;

import org.apache.hadoop.fs.Path;

import org.apache.hama.HamaConfiguration;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.io.KeyValueInputFormat;
import org.apache.hama.myhama.io.TextBSPFileOutputFormat;

/**
 * MISDriver.java
 * A driven program is used to submit the Maximal Independent Sets (MIS) 
 * computation job using the style.PULL model.
 * Note: a non-weighted undirected graph.
 * 
 * @author 
 * @version 0.1
 */
public class MISPullDriver {
	
	public static void main(String[] args) throws Exception {
		//check paramaters
		if (args.length < 6) {
			StringBuffer sb = 
				new StringBuffer("\nUsage of PULL-based Maximal Independent Sets (MIS):");
			sb.append("\n(*)required parameter");
			sb.append("\n[*]optional parameter");
			sb.append("\n   (1) input directory on HDFS (undirected graph)"); 
			sb.append("\n   (2) output directory on HDFS");
			sb.append("\n   (3) #tasks(int)");
			sb.append("\n   (4) #iterations(int)");
			sb.append("\n   (5) #vertices(int)");
			sb.append("\n   (6) #buckets(int)");
			sb.append("\n   [7] msg_pack for style.Pull(int, 10^4 default)");
			
			System.out.println(sb.toString());
			System.exit(-1);
		}

		//configurate the job
		HamaConfiguration conf = new HamaConfiguration();
		BSPJob bsp = new BSPJob(conf, MISPullDriver.class);
		bsp.setJobName("Maximal Independent Sets");
		bsp.setPriority(Constants.PRIORITY.NORMAL);		

		bsp.setBspClass(MISBSP.class);
		bsp.setUserToolClass(MISUserTool.class);
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
