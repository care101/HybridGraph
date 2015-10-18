package org.apache.hama.myhama.api;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;

public abstract class MsgRecord<M> {
	protected int srcId = -1;
	/** only used in the target vertex end */
	protected boolean isValid = false;
	
	/** exchange via network */
	protected int dstId = -1;
	protected M msgValue;
	
	public MsgRecord() {
		
	}
	
	/**
	 * Process a new message by {@link GraphRecord.getMsg()}
	 * @param srcId
	 * @param dstId
	 * @param msgValue
	 */
	public void initialize(int srcId, int dstId, M msgValue) {
		this.srcId = srcId;
		this.dstId = dstId;
		this.msgValue = msgValue;
		this.isValid = true;
	}
	
	public int getSrcVerId() {
		return this.srcId;
	}
	
	public int getDstVerId() {
		return this.dstId;
	}
	
	public M getMsgValue() {
		return this.msgValue;
	}
	
	public boolean isValid() {
		return this.isValid;
	}
	
	public void reset() {
		this.isValid = false;
		this.msgValue = null;
	}
	
	/**
	 * Collect received messages.
	 * @param msg
	 */
	public synchronized void collect(MsgRecord<M> msg) {
		if (this.isValid) {
			combiner(msg);
		} else {
			this.isValid = true;
			this.dstId = msg.getDstVerId();
			this.msgValue = msg.getMsgValue();
		}
	}
	
	
	//=====================
	//    User-Defined
	//=====================
	
	/**
	 * Combine the message "msg" to itself.
	 * 
	 * For example, for PageRank, the value of "msg" 
	 * can be added into itself, then "msg" is deleted.
	 * 
	 * For LPA, the value cannot be added directly. 
	 * However, it still can be added in an @ArrayList, 
	 * in order to share a single target vertex id.
	 * 
	 * Anyway, this function must be implemented by users.
	 * But the specific logic is user-defined.
	 **/
	public abstract void combiner(MsgRecord<M> msg);
	
    /**
     * Serialize the content of {@link MsgRecord} 
     * as the binary byte stream for RPC communication. 
     * @param out
     * @throws IOException
     */
    public abstract void serialize(DataOutputStream out) throws IOException;
    
    /**
     * Deserialize the content of {@link MsgRecord}
     * from the binary byte stream for RPC communication.
     * @param in
     * @throws IOException
     */
    public abstract void deserialize(DataInputStream in) throws IOException;
    
	/**
	 * Set the size of one message record in bytes.
	 * For example, if only the dstId and mssgValue are valid, 
	 * and dstId is Integer, mssgValue is Float,
	 * then BytesLength is equal with 4 + 4 = 8.
	 */
	public int getMsgByte() {
		return 0;
	}
	
	/** 
	 * Get the number of real message values in this {@link MsgRecord}.
	 * Only used in style.Pull.
	 * Normally, for algorithms with Combiner, such as Shortest Path, 
	 * return 1 as default if Combiner is enable. 
	 * That means one {@link MsgRecord} only stores one single message value.
	 * However, for algorithms without Combiner, such as Simulate Advertisements, 
	 * user should override this function, return #message_values, 
	 * since multiple message values may shared the same target vertex id and 
	 * be combined into this {@link MsgRecord}.
	 * @return
	 */
	public int getNumOfMsgValues() {
		return 1;
	}
	
	   /**
     * Serialize the content of {@link MessageRecord} 
     * as the binary byte stream for local disk.
     * @param MappedByteBuffer out
     * @throws IOException
     */
    public void serialize(MappedByteBuffer out) throws IOException {}
    
    /**
     * Deserialize the content of {@link MessageRecord}
     * from the binary byte stream for local disk.
     * @param MappedByteBuffer in
     * @throws IOException
     */
    public void deserialize(MappedByteBuffer in) throws IOException {}
}
