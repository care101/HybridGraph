package org.apache.hama.myhama.api;

public interface MsgRecordInterface<M> {
	
	public int getSrcVerId();
	
	public int getDstVerId();
	
	public M getMsgValue();
	
	/** 
	 * Get the number of real message values in this {@link MsgRecordInterface}.
	 * Only used in style.Pull.
	 * Normally, for algorithms with Combiner, such as Shortest Path, 
	 * return 1 as default if Combiner is enable. 
	 * That means one {@link MsgRecordInterface} only stores one single message value.
	 * However, for algorithms without Combiner, such as Simulate Advertisements, 
	 * user should override this function, return #message_values, 
	 * since multiple message values may shared the same target vertex id and 
	 * be combined into this {@link MsgRecordInterface}.
	 * @return
	 */
	public int getNumOfMsgValues();
}
