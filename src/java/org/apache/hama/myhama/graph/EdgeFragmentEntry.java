package org.apache.hama.myhama.graph;

/**
 * EdgeFragmentEntry used when decomposing a new 
 * {@link GraphRecord} during loading.
 * @author root
 *
 * @param <W>
 */
public class EdgeFragmentEntry<V,W,M,I> extends EdgeFragment<V,W,M,I> {
	private int srcBid=-1, dstTid=-1, dstBid=-1;
	
	public EdgeFragmentEntry(int _vid,  
			int _srcBid, int _dstTid, int _dstBid) {
		super();
		
		verId = _vid;
		srcBid = _srcBid;
		dstTid = _dstTid;
		dstBid = _dstBid;
	}
	
	public void initialize(Integer[] _edgeIds, W[] _edgeWeights) {
		edgeIds = _edgeIds;
		edgeWeights = _edgeWeights;
		edgeNum = _edgeIds==null? 0:edgeIds.length;
	}
	
	/**
	 * Return the source vertex id.
	 * @return
	 */
	public int getVerId() {
		return this.verId;
	}
	
	public Integer[] getEdgeIds() {
		return this.edgeIds;
	}
	
	public W[] getEdgeWeights() {
		return this.edgeWeights;
	}
	
	public int getSrcBid() {
		return this.srcBid;
	}
	
	public int getDstTid() {
		return this.dstTid;
	}
	
	public int getDstBid() {
		return this.dstBid;
	}
}
