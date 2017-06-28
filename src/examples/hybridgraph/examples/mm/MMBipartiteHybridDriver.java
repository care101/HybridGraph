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
 * computation job using the STYLE.Hybrid model.
 * Note: a non-weighted undirected bipartite graph.
 * 
 * @author 
 * @version 0.1
 */
public class MMBipartiteHybridDriver {
	
	public static void main(String[] args) throws Exception {
		//check parameters
		if (args.length < 6) {
			StringBuffer sb = 
				new StringBuffer("\nUsage of Hybrid-based Random Maximal Matching (MM):");
			sb.append("\n(*)required parameter");
			sb.append("\n[*]optional parameter");
			sb.append("\n   (1)input directory on HDFS (undirected bipartite graph)"); 
			sb.append("\n   (2)output directory on HDFS"); 
			sb.append("\n   (3)#tasks(int)");
			sb.append("\n   (4)#iterations(int)");
			sb.append("\n   (5)#vertices(int)");
			sb.append("\n   (6)#buckets(int)");
			
			sb.append("\n   [7]msg_pack for style.PULL(int, 10^4 default)");
			sb.append("\n   [8]send_buffer for style.PUSH(int, 10^4 default)");
			sb.append("\n   [9]receive_buffer for style.PUSH(int, 10^4 default)");
			sb.append("\n   [10]startIteStyle(int: 0=>PUSH, 1=>PULL)");
			sb.append("\n   [11]simulate style.PUSH(int: 0=>No, 1=>Yes, 0 default)");
			
			System.out.println(sb.toString());
			System.exit(-1);
		}

		//configurate the job
		HamaConfiguration conf = new HamaConfiguration();
		BSPJob bsp = new BSPJob(conf, MMBipartiteHybridDriver.class);
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
		
		bsp.setBspStyle(Constants.STYLE.Hybrid);
		bsp.setGraphDataOnDisk(true);
		bsp.setMiniSuperStep(true);
		
		//=======================//
		//  optional parameters  //
		//=======================//
		if (args.length >= 7) {
			bsp.setMsgPackSize(Integer.valueOf(args[6]));
		}
		if (args.length >= 8) {
			bsp.setMsgSendBufSize(Integer.valueOf(args[7]));
		}
		if (args.length >= 9) {
			bsp.setMsgRecBufSize(Integer.valueOf(args[8]));
		}
		if (args.length >= 10) {
			bsp.setStartIteStyle(Constants.STYLE.values()[Integer.valueOf(args[9])]);
		}
		if (args.length >= 11) {
			bsp.setSimulatePUSH(Integer.valueOf(args[10]));
		}
		
		//submit the job
		bsp.waitForCompletion(true);
	}
}
