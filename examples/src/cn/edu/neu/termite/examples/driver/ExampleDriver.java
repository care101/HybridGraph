package cn.edu.neu.termite.examples.driver;

import org.apache.hadoop.util.ProgramDriver;

import cn.edu.neu.termite.examples.cc.pull.CCPullDriver;
import cn.edu.neu.termite.examples.cluster.pull.SCPullDriver;
import cn.edu.neu.termite.examples.lpa.pull.LPAPullDriver;
import cn.edu.neu.termite.examples.lpa.push.LPAPushDriver;
import cn.edu.neu.termite.examples.pagerank.pull.PageRankPullDriver;
import cn.edu.neu.termite.examples.pagerank.push.PageRankPushDriver;
import cn.edu.neu.termite.examples.sa.hybrid.SAHybridDriver;
import cn.edu.neu.termite.examples.sa.pull.SAPullDriver;
import cn.edu.neu.termite.examples.sa.push.SAPushDriver;
import cn.edu.neu.termite.examples.sssp.hybrid.SSSPHybridDriver;
import cn.edu.neu.termite.examples.sssp.pull.SSSPPullDriver;
import cn.edu.neu.termite.examples.sssp.push.SSSPPushDriver;

public class ExampleDriver {
	
	public static void main(String argv[]){
	    int exitCode = -1;
	    ProgramDriver pgd = new ProgramDriver();
	    
	    try {
	    	/** PageRank */
	    	pgd.addClass("pr.pull", PageRankPullDriver.class, 
            	"\tpagerank by pull (2013-10-12)");
	    	pgd.addClass("pr.push", PageRankPushDriver.class, 
				"\tpagerank by push (2015-06-09)");
	    	
	    	/** Connected Components */
	    	pgd.addClass("cc.pull", CCPullDriver.class, 
    			"\tconnected components by pull (2013-10-12)");
	    	
	    	/** Label Propagation */
	    	pgd.addClass("lpa.pull", LPAPullDriver.class, 
				"\tlabel propagation algorithm by pull (2014-06-20)");
	    	pgd.addClass("lpa.push", LPAPushDriver.class, 
				"\tlabel propagation algorithm by push (2014-06-27)");
	    	
	    	/** Simulate Advertisements */
	    	pgd.addClass("sa.pull", SAPullDriver.class, 
				"\tsimulate advertisements by pull (2014-07-15)");
	    	pgd.addClass("sa.push", SAPushDriver.class, 
				"\tsimulate advertisements by push (2015-06-14)");
	    	pgd.addClass("sa.hybrid", SAHybridDriver.class, 
				"\tsimulate advertisements by hybrid (2015-06-14)");
	    	
	    	/** Semi-Cluster */
	    	pgd.addClass("sc.pull", SCPullDriver.class, 
				"\tsemi-cluster by pull (2014-11-04)");
	    	
	    	/** Single Source Shortest Path/Distance */
	    	pgd.addClass("sssp.pull", SSSPPullDriver.class, 
    			"\tsingle source shortest path by pull (2013-10-12)");
	    	pgd.addClass("sssp.push", SSSPPushDriver.class, 
				"\tsingle source shortest path by push (2015-06-09)");
	    	pgd.addClass("sssp.hybrid", SSSPHybridDriver.class, 
				"\tsingle source shortest path by hybrid (2015-06-10)");
	    	
	    	pgd.driver(argv);
	    	exitCode = 0;
	    } catch(Throwable e) {
	    	e.printStackTrace();
	    }
	    
	    System.exit(exitCode);
	}
}
