package org.apache.hama.myhama.comm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hama.Constants;
import org.apache.hama.Constants.CommandType;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class SuperStepCommand implements Writable {
	private CommandType commandType;
	private float jobAgg;
	private ArrayList<Integer>[] route;
	
	private Constants.STYLE preIteStyle;
	private Constants.STYLE curIteStyle;
	private boolean estimatePullByte;
	
	private int iteNum;
	private int ckpVersion;
	
	//local variable
	private double metricQ;
	private double iteTime;
	private HashSet<Integer> failedTaskIds;
	
	/**
	 * Skip the read operation of logged messages when recovering 
	 * failures with ConfinedRecoveryLogMsg. This flag is true only 
	 * for the first recovery superstep when pre=PUSH, because no 
	 * message is archived into the PULL directory at the previous 
	 * iteration. Instead of directly loading messages, in this case, 
	 * outgoing messages are re-generated using PULL and then returned. 
	 */
	private boolean skipMsgLoad = false;
	
	/**
	 * True if failures are detected by {@link JobInProgress}. This flag 
	 * is set to true only by {@link JobInProgress}.recoveryRegisterTask(), 
	 * in order to tell surviving tasks to save the inturrupted program 
	 * status and/or do some preprocessing work.
	 */
	private boolean findError = false;
	
	public SuperStepCommand() {
		this.findError = false;
		this.skipMsgLoad = false;
	}
	
	/**
	 * Set the runtime of the current superstep, 
	 * excluding the cost of archiving a checkpoint.
	 * @param time
	 */
	public void setIterationTime(double time) {
		iteTime = time;
	}
	
	/**
	 * Get the runtime of the current superstep, 
	 * excluding the cost of archiving a checkpoint.
	 * @return
	 */
	public double getIterationTime() {
		return iteTime;
	}
	
	/**
	 * Reduce the storage space requirement by releasing the memory 
	 * space of some variables, such as route table (ArrayList[]).
	 */
	public void compact() {
		route = null;
	}
	
	/**
	 * Copy some variables provided by the given command. 
	 * "jobAgg" is one of these parameters because some algrorithms, 
	 * such as LPA, vote to halt based on the global aggregator value. 
	 * "estimatePullByte" is also required so that the system can 
	 * correctly collect bytes under PUSH/PULL to decide which model 
	 * is more efficient at the next iteration.
	 * @param command
	 */
	public void copy(SuperStepCommand command) {
		jobAgg = command.getJobAgg();
		preIteStyle = command.getPreIteStyle();
		curIteStyle = command.getCurIteStyle();
		setEstimatePullByte(command.isEstimatePullByte());
	}
	
	/**
	 * Set the superstep counter.
	 * @param num
	 */
	public void setIteNum(int num) {
		iteNum = num;
	}
	
	/**
	 * Get the current superstep counter.
	 * @return
	 */
	public int getIteNum() {
		return iteNum;
	}
	
	public void setAvailableCheckPointVersion(int version) {
		ckpVersion = version;
	}
	
	public int getAvailableCheckPointVersion() {
		return ckpVersion;
	}
	
	public void setFailedTaskIds(HashSet<Integer> _failed) {
		failedTaskIds = new HashSet();
		for (int id: _failed) {
			failedTaskIds.add(id);
		}
	}
	
	public HashSet<Integer> getFailedTaskIds() {
		return failedTaskIds;
	}
	
	public void setMetricQ(double _q) {
		this.metricQ = _q;
	}
	
	public double getMetricQ() {
		return this.metricQ;
	}
	
	/**
	 * Get the command of the next superstep.
	 * @return
	 */
	public CommandType getCommandType() {
		return this.commandType;
	}
	
	/**
	 * Set the command of the next superstep.
	 * @param commandType
	 */
	public void setCommandType(CommandType commandType) {
		this.commandType = commandType;
	}

	public float getJobAgg() {
		return this.jobAgg;
	}
	
	public void setJobAgg(float jobAgg) {
		this.jobAgg = jobAgg;
	}
	
	public void setRealRoute(ArrayList<Integer>[] _route) {
		this.route = _route;
	}
	
	public ArrayList<Integer>[] getRealRoute() {
		return this.route;
	}
	
	/**
	 * Set the iterative computation style at the next superstep.
	 * @param _preStyle
	 * @param _curStyle
	 */
	public void setIteStyle(Constants.STYLE _preStyle, Constants.STYLE _curStyle) {
		this.preIteStyle = _preStyle;
		this.curIteStyle = _curStyle;
	}
	
	/**
	 * Adjust the style command when restarting computations in failure recovery. 
	 * This function is called only for the first recovery superstep when pre=PUSH, 
	 * in order to prepare messages for the current superstep. This is because in 
	 * our checkpoint, outgoing messages are not archived for efficiency. 
	 * The adjusting rules are listed below:
	 * (1)pre=PULL & cur=PULL, OK;
	 * (2)pre=PULL & cur=PUSH, OK;
	 * (3)pre=PUSH & cur=PULL, =>(1);
	 * (4)pre=PUSH & cur=PUSH, =>(2);
	 */
	public void adjustIteStyle(Constants.CheckPoint.Policy policy) {
		if (preIteStyle == Constants.STYLE.PUSH) {
			preIteStyle = Constants.STYLE.PULL;
			if (policy == Constants.CheckPoint.Policy.ConfinedRecoveryLogMsg) {
				//skip read operations since no message is archived into the PULL 
				//directory at the previous PUSH iteration
				skipMsgLoad = true;
			}
		}
	}
	
	public void setFindError(boolean flag) {
		findError = flag;
	}
	
	public boolean findError() {
		return findError;
	}
	
	/**
	 * If true is returned, directly load outgoing messages from the PULL directory 
	 * on local disks when using CompleteRecoveryMsgLog to recover failures. Otherwise, 
	 * skip the loading operation. Instead, messages are re-generated. 
	 * @return
	 */
	public boolean skipMsgLoad() {
		return skipMsgLoad;
	}
	
	/**
	 * Return the iterative computation style of the next superstep.
	 * @return
	 */
	public Constants.STYLE getCurIteStyle() {
		return this.curIteStyle;
	}
	
	/**
	 * Return the iterative computation style of the previous/current superstep.
	 * @return
	 */
	public Constants.STYLE getPreIteStyle() {
		return this.preIteStyle;
	}
	
	public void setEstimatePullByte(boolean flag) {
		this.estimatePullByte = flag;
	}
	
	public boolean isEstimatePullByte() {
		return this.estimatePullByte;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		commandType = WritableUtils.readEnum(in, CommandType.class);
		jobAgg = in.readFloat();
		preIteStyle = WritableUtils.readEnum(in, Constants.STYLE.class);
		curIteStyle = WritableUtils.readEnum(in, Constants.STYLE.class);
		estimatePullByte = in.readBoolean();
		skipMsgLoad = in.readBoolean();
		findError = in.readBoolean();
		iteNum = in.readInt();
		ckpVersion = in.readInt();
		
		int len = in.readInt();
		route = new ArrayList[len];
		for (int i = 0; i < len; i++) {
			route[i] = new ArrayList<Integer>();
			int size = in.readInt();
			for (int j = 0; j < size; j++) {
				route[i].add(in.readInt());
			}
		}
		
		len = in.readInt();
		if (len > 0) {
			failedTaskIds = new HashSet<Integer>();
			for (int i = 0; i < len; i++) {
				failedTaskIds.add(in.readInt());
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeEnum(out, commandType);
		out.writeFloat(jobAgg);
		WritableUtils.writeEnum(out, preIteStyle);
		WritableUtils.writeEnum(out, curIteStyle);
		out.writeBoolean(estimatePullByte);
		out.writeBoolean(skipMsgLoad);
		out.writeBoolean(findError);
		out.writeInt(iteNum);
		out.writeInt(ckpVersion);
		
		out.writeInt(route.length);
		for (int i = 0; i < route.length; i++) {
			out.writeInt(route[i].size());
			for (int tid: route[i]) {
				out.writeInt(tid);
			}
		}
		
		if (failedTaskIds != null) {
			out.writeInt(failedTaskIds.size());
			for (int id: failedTaskIds) {
				out.writeInt(id);
			}
		} else {
			out.writeInt(0);
		}
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.commandType);
		
		sb.append("\t"); 
		sb.append(this.curIteStyle);
		
		sb.append("\t");
		sb.append(iteTime);
		
		sb.append("\t"); 
		sb.append(this.jobAgg);
		
		sb.append("\t");
		sb.append(metricQ);
		
		if (failedTaskIds != null) {
			sb.append("\t[");
			for (Integer id: failedTaskIds) {
				sb.append(id.toString());
				sb.append(",");
			}
			sb.deleteCharAt(sb.length()-1); //delete the last ","
			sb.append("]");
		}
		
		return sb.toString();
	}
}
