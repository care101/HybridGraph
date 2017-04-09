/**
 * copyright 2012-2010
 */
package org.apache.hama.ipc;

import java.io.Closeable;

import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPRPCProtocolVersion;
import org.apache.hama.monitor.JobInformation;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.comm.MsgPack;
import org.apache.hama.myhama.comm.SuperStepCommand;

/**
 * CommunicationRPCProtocol.
 * 
 * @author 
 * @version 0.1
 */
public interface CommunicationServerProtocol<V, W, M, I> 
		extends BSPRPCProtocolVersion,
		Closeable, Constants {
	public int parId = 0;
	
	/**
	 * Receive messages from source vertices.
	 * Used in style.Push.
	 * 
	 * @param srcParId
	 * @param pack
	 * @return #messagesOnDisk for push
	 * @throws Exception
	 */
	public long recMsgData(int srcParId, MsgPack<V, W, M, I> pack);
	
	/**
	 * Obtain {@link MsgRecord} from tasks which contain edges.
	 * These messages will be put into {@link MsgDataServer}.
	 * @param _toTaskId id of task to which messages are sent
	 * @param _toBlkId local id of {@link VBlock} to which messages are sent 
	 * @param _iteNum
	 * @return
	 */
	public MsgPack<V, W, M, I> obtainMsgData(int _toTaskId, int _toBlkId, int _iteNum);
	
	/**
	 * Set route table information and then quit the synchronization barrier 
	 * initiated by {@link MasterProtocol}.buildRouteTable().
	 * @param jobInfo
	 */
	public void setRouteTable(JobInformation jobInfo);
	
	/**
	 * Update route table information kept on surviving tasks when launching 
	 * new tasks to replace failed tasks.
	 * @param jobInfo
	 */
	public void updateRouteTable(JobInformation jobInfo);
	
	/**
	 * Set {@link JobInformation} and then quit the synchronization barrier 
	 * initiated by {@link MasterProtocol}.registerTask().
	 * @param jobInfo
	 */
	public void setRegisterInfo(JobInformation jobInfo);
	
	/**
	 * Set command for the next superstep and then quit the synchronization barrier 
	 * initiated by {@link MasterProtocol}.finishSuperStep().
	 * @param SuperStepCommand ssc
	 */
	public void setNextSuperStepCommand(SuperStepCommand ssc);
	
	/**
	 * Quit a synchronization barrier initiated by 
	 * {@link MasterProtocol}.sync() or .beginSuperStep().
	 */
	public void quit();
}
