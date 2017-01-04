package hybridgraph.examples.driver;

import hybridgraph.examples.cc.CCPullDriver;
import hybridgraph.examples.lpa.LPAPullDriver;
import hybridgraph.examples.pagerank.PageRankPullDriver;
import hybridgraph.examples.pagerank.PageRankHybridDriver;
import hybridgraph.examples.sa.SAHybridDriver;
import hybridgraph.examples.sa.SAPullDriver;
import hybridgraph.examples.sssp.SSSPHybridDriver;
import hybridgraph.examples.sssp.SSSPPullDriver;

import org.apache.hadoop.util.ProgramDriver;


public class ExampleDriver {
	
	public static void main(String argv[]){
	    int exitCode = -1;
	    ProgramDriver pgd = new ProgramDriver();
	    
	    try {
	    	/** PageRank */
	    	pgd.addClass("pr.pull", PageRankPullDriver.class, 
            	"\tpagerank by pull (10/12/2013)");
		pgd.addClass("pr.hybrid", PageRankHybridDriver.class, 
		"\tpagerank by hybrid (01/04/2017)");
	    	
	    	/** Connected Components */
	    	pgd.addClass("cc.pull", CCPullDriver.class, 
    			"\tconnected components by pull (10/12/2013)");
	    	
	    	/** Label Propagation */
	    	pgd.addClass("lpa.pull", LPAPullDriver.class, 
				"\tlabel propagation algorithm by pull (06/20/2014)");
	    	
	    	/** Simulate Advertisements */
	    	pgd.addClass("sa.pull", SAPullDriver.class, 
				"\tsimulate advertisements by pull (07/15/2014)");
	    	pgd.addClass("sa.hybrid", SAHybridDriver.class, 
				"\tsimulate advertisements by hybrid (06/14/2015)");
	    	
	    	/** Single Source Shortest Path/Distance */
	    	pgd.addClass("sssp.pull", SSSPPullDriver.class, 
    			"\tsingle source shortest path by pull (10/12/2013)");
	    	pgd.addClass("sssp.hybrid", SSSPHybridDriver.class, 
				"\tsingle source shortest path by hybrid (06/10/2015)");
	    	
	    	pgd.driver(argv);
	    	exitCode = 0;
	    } catch(Throwable e) {
	    	e.printStackTrace();
	    }
	    
	    System.exit(exitCode);
	}
}
