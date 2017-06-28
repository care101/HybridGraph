package hybridgraph.examples.driver;

import hybridgraph.examples.cc.CCPullDriver;
import hybridgraph.examples.lpa.LPAPullDriver;
import hybridgraph.examples.mis.MISHybridDriver;
import hybridgraph.examples.mis.MISPullDriver;
import hybridgraph.examples.mm.MMBipartiteHybridDriver;
import hybridgraph.examples.mm.MMBipartitePullDriver;
import hybridgraph.examples.pagerank.PageRankPullDriver;
import hybridgraph.examples.sa.SAHybridDriver;
import hybridgraph.examples.sa.SAPullDriver;
import hybridgraph.examples.sssp.SSSPHybridDriver;
import hybridgraph.examples.sssp.SSSPPullDriver;
import hybridgraph.examples.sssp.determ.SSSPHybridDriverDeterm;

import org.apache.hadoop.util.ProgramDriver;


public class ExampleDriver {
	
	public static void main(String argv[]){
	    int exitCode = -1;
	    ProgramDriver pgd = new ProgramDriver();
	    
	    try {
	    	/** PageRank */
	    	pgd.addClass("pr.pull", PageRankPullDriver.class, 
            	"\tpagerank by pull (10/12/2013)");
	    	
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
	    	pgd.addClass("sssp.hybrid.determ", SSSPHybridDriverDeterm.class, 
				"\tsingle source shortest path by hybrid (determinstic) (04/18/2017)");
	    	
	    	/** Maximal Independent Sets (MIS) */
	    	pgd.addClass("mis.pull", MISPullDriver.class, 
    			"\tmaximal independent sets by pull (05/19/2017)");
	    	pgd.addClass("mis.hybrid", MISHybridDriver.class, 
				"\tmaximal independent sets by hybrid (05/19/2017)");
	    	
	    	/** Random Maximal Matching (MM) */
	    	pgd.addClass("mm.pull", MMBipartitePullDriver.class, 
				"\trandom maximal matching by pull (05/26/2017)");
	    	pgd.addClass("mm.hybrid", MMBipartiteHybridDriver.class, 
				"\trandom maximal matching by hybrid (05/25/2017)");
	    	
	    	pgd.driver(argv);
	    	exitCode = 0;
	    } catch(Throwable e) {
	    	e.printStackTrace();
	    }
	    
	    System.exit(exitCode);
	}
}
