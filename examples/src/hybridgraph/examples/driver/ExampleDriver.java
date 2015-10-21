package hybridgraph.examples.driver;

import hybridgraph.examples.cc.pull.CCPullDriver;
import hybridgraph.examples.lpa.pull.LPAPullDriver;
import hybridgraph.examples.pagerank.pull.PageRankPullDriver;
import hybridgraph.examples.sa.hybrid.SAHybridDriver;
import hybridgraph.examples.sa.pull.SAPullDriver;
import hybridgraph.examples.sssp.hybrid.SSSPHybridDriver;
import hybridgraph.examples.sssp.pull.SSSPPullDriver;

import org.apache.hadoop.util.ProgramDriver;


public class ExampleDriver {
	
	public static void main(String argv[]){
	    int exitCode = -1;
	    ProgramDriver pgd = new ProgramDriver();
	    
	    try {
	    	/** PageRank */
	    	pgd.addClass("pr.pull", PageRankPullDriver.class, 
            	"\tpagerank by pull (2013-10-12)");
	    	
	    	/** Connected Components */
	    	pgd.addClass("cc.pull", CCPullDriver.class, 
    			"\tconnected components by pull (2013-10-12)");
	    	
	    	/** Label Propagation */
	    	pgd.addClass("lpa.pull", LPAPullDriver.class, 
				"\tlabel propagation algorithm by pull (2014-06-20)");
	    	
	    	/** Simulate Advertisements */
	    	pgd.addClass("sa.pull", SAPullDriver.class, 
				"\tsimulate advertisements by pull (2014-07-15)");
	    	pgd.addClass("sa.hybrid", SAHybridDriver.class, 
				"\tsimulate advertisements by hybrid (2015-06-14)");
	    	
	    	/** Single Source Shortest Path/Distance */
	    	pgd.addClass("sssp.pull", SSSPPullDriver.class, 
    			"\tsingle source shortest path by pull (2013-10-12)");
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
