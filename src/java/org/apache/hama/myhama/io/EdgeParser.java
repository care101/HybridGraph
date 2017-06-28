package org.apache.hama.myhama.io;

import java.util.ArrayList;

/**
 * Parse the string argument as edges (edge ids [and weights]) 
 * around the matches of the given characters.
 * @author root
 *
 */
public class EdgeParser {
	
	public class IntDoubleEdgeSet {
		Integer[] ids;
		Double[] weights;
		
		public IntDoubleEdgeSet(Integer[] _ids, Double[] _weights) {
			this.ids = _ids;
			this.weights = _weights;
		}
		
		public Integer[] getEdgeIds() {
			return this.ids;
		}
		
		public Double[] getEdgeWeights() {
			return this.weights;
		}
	}
	
	private boolean isEven(int id) {
		if ((id%2) == 0) {
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * Parse the string argument as target vertex ids of edges around the 
	 * matches of the given character. A Bipartite Graph is achieved by 
	 * the following rules:
	 *  1) if v.id%2 is 0, any edge with e.target.id%2==0 is removed;
	 *  2) if v.id%2 is 1, any edge with e.target.id%2==1 is removed.
	 *  
	 * @param eData
	 * @param c
	 * @param s
	 * @return
	 */
	public Integer[] parseEdgeIdArrayFilterBipartiteGraph(String eData, char c, int s) {
		if (eData.equals("")) {
			return null;
		}
		
		boolean sIsEven = isEven(s);
		
        ArrayList<Integer> tmpEdgeId = new ArrayList<Integer>();
    	char edges[] = eData.toCharArray();
        int begin = 0, end = 0, eid = 0;
        for(end = 0; end < edges.length; end++) {
            if(edges[end] != c) {
                continue;
            }
            eid = Integer.valueOf(new String(edges, begin, end-begin));
            if (isEven(eid) != sIsEven) {
            	tmpEdgeId.add(eid);
            }
            begin = ++end;
        }
        eid = Integer.valueOf(new String(edges, begin, end-begin));
        if (isEven(eid) != sIsEven) {
        	tmpEdgeId.add(eid);
        }
        
        if (tmpEdgeId.size() == 0) {
        	return null;
        } else {
            Integer[] edgeIds = new Integer[tmpEdgeId.size()];
            tmpEdgeId.toArray(edgeIds);
            return edgeIds;
        }
	}
	
	/**
	 * Parse the string argument as target vertex ids of edges 
	 * around the matches of the given character. Edges linking 
	 * to the source vertex id will be removed.
	 * @param eData
	 * @param c
	 * @param s
	 * @return
	 */
	public Integer[] parseEdgeIdArrayFilterSourceVert(String eData, char c, int s) {
		if (eData.equals("")) {
			return null;
		}
		
        ArrayList<Integer> tmpEdgeId = new ArrayList<Integer>();
    	char edges[] = eData.toCharArray();
        int begin = 0, end = 0, eid = 0;
        for(end = 0; end < edges.length; end++) {
            if(edges[end] != c) {
                continue;
            }
            eid = Integer.valueOf(new String(edges, begin, end-begin));
            if (eid != s) {
            	tmpEdgeId.add(eid);
            }
            begin = ++end;
        }
        eid = Integer.valueOf(new String(edges, begin, end-begin));
        if (eid != s) {
        	tmpEdgeId.add(eid);
        }
        
        if (tmpEdgeId.size() == 0) {
        	return null;
        } else {
            Integer[] edgeIds = new Integer[tmpEdgeId.size()];
            tmpEdgeId.toArray(edgeIds);
            return edgeIds;
        }
	}
	
	/**
	 * Parse the string argument as target vertex ids of edges 
	 * around the matches of the given character. If eData is "", 
	 * null is returned.
	 * @param eData
	 * @param c
	 * @return
	 */
	public Integer[] parseEdgeIdArray(String eData, char c) {
		if (eData.equals("")) {
			return null;
		}
		
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
	
	/**
	 * Parse the string argument as a group of target vertex id && 
	 * edge weight around the matches of the given character. If 
	 * eData is "", eids and weights in {@link IntDoubleEdgeSet} are null.
	 * @param eData
	 * @return
	 */
	public IntDoubleEdgeSet parseEdgeIdWeightArray(String eData, char c) {
		if (eData.equals("")) {
			return new IntDoubleEdgeSet(null, null);
		}
		
        ArrayList<Integer> tmpEdgeId = new ArrayList<Integer>();
        ArrayList<Double> tmpEdgeWeight = new ArrayList<Double>();
        boolean isId = true;
    	char edges[] = eData.toCharArray();
        int begin = 0, end = 0;
        for(end = 0; end < edges.length; end++) {
            if(edges[end] != c) {
                continue;
            }
            if (isId) {
                tmpEdgeId.add(Integer.valueOf(
                		new String(edges, begin, end-begin)));
            } else {
            	tmpEdgeWeight.add(Double.valueOf(
                		new String(edges, begin, end-begin)));
            }
            isId = !isId;

            begin = ++end;
        }
        tmpEdgeWeight.add(Double.valueOf(
        		new String(edges, begin, end-begin)));
        
        Integer[] edgeIds = new Integer[tmpEdgeId.size()];
        tmpEdgeId.toArray(edgeIds);
        Double[] edgeWeights = new Double[tmpEdgeWeight.size()];
        tmpEdgeWeight.toArray(edgeWeights);
        
        return new IntDoubleEdgeSet(edgeIds, edgeWeights);
	}
}
