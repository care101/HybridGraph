package org.apache.hama.myhama.io;

import java.util.ArrayList;

/**
 * Parse the string argument as edges (edge ids [and weights]) 
 * around the matches of the given characters.
 * @author root
 *
 */
public class EdgeParser {
	
	/**
	 * Parse the string argument as target vertex ids of edges 
	 * around the matches of the given character.
	 * @param eData
	 * @param c
	 * @return
	 */
	public Integer[] parseEdgeIdArray(String eData, char c) {
        ArrayList<Integer> tmpEdgeId = new ArrayList<Integer>();
    	char edges[] = eData.toCharArray();
        int begin = 0, end = 0;
        for(end = 0; end < edges.length; end++) {
            if(edges[end] != c) {
                continue;
            }
            tmpEdgeId.add(Integer.valueOf(
            		new String(edges, begin, end-begin)));
            begin = ++end;
        }
        tmpEdgeId.add(Integer.valueOf(
        		new String(edges, begin, end-begin)));
        
        Integer[] edgeIds = new Integer[tmpEdgeId.size()];
        tmpEdgeId.toArray(edgeIds);
        
        return edgeIds;
	}
}
