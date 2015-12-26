/**
 * Termite System
 * copyright 2012-2010
 */
package org.apache.hama.myhama.api;

/**
 * GraphRecordInterface.
 * This class defines operations used in {@link BSP}.update 
 * and {@link BSP}.getMessages().
 * 
 * @author 
 * @version 0.1
 */
public interface GraphRecordInterface<V, W, M, I> {
	
	public int getVerId();
	
	public void setVerValue(V _verValue);
	
	public V getVerValue();
	
	public I getGraphInfo();
	
	public int getEdgeNum();
	
	public Integer[] getEdgeIds();
	
	public W[] getEdgeWeights();
}
