package org.apache.hama.monitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants.CommandType;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.comm.SuperStepCommand;

public class JobInformation implements Writable {
	private BSPJob job;
	private int taskNum = 0;
	private int verNum = 0;
	private long edgeNum = 0L;
	private int verMinId = Integer.MAX_VALUE, verMaxId = 0;
	private int[] verMinIds, verMaxIds;
	private int[] taskIds, ports;
	private String[] hostNames;
	
	private int blkNumOfJob = 0; //total #VBlocks of this job
	private int[] blkNumOfTasks;  //#VBlocks of each task
	private int[] blkLenOfTasks;  //#vertices in one VBlock at each task
	/** the beginning global idx of VBlocks at each task */
	private int[] headBlkIdxOfTasks; 
	
	private String ckpJobDir; //checkpoint directory for this job
	
	//=====================================
	// Only available in JobInProgress
	//=====================================
	
	private int[] verNumOfBlks;
	/** number of responding source vertices of each VBlock */
	private int[] resVerNumOfBlks;
	/** dependency relationship among all VBlocks with responding vertices */
	private boolean[][] resDependMatrix;
	
	/** commands at previous iterations */
	private ArrayList<SuperStepCommand> commands;
	/** commands at recovery iterations */
	private ArrayList<SuperStepCommand> recoveryCommands;
	/** the most recent checkpoint version */
	private int ckpVersion;
		
	public JobInformation() {
		
	}
	
	public JobInformation (BSPJob _job, int _taskNum) {
		this.job = _job;
		this.taskNum = _taskNum;
		this.taskIds = new int[_taskNum];
		this.verMinIds = new int[_taskNum];
		this.verMaxIds = new int[_taskNum];
		this.ports = new int[_taskNum];
		this.hostNames = new String[_taskNum];
		
		this.blkNumOfJob = 0;
		this.blkNumOfTasks = new int[_taskNum];
		this.blkLenOfTasks = new int[_taskNum];
		this.headBlkIdxOfTasks = new int[_taskNum];
		
		commands = new ArrayList<SuperStepCommand>(); 
		recoveryCommands = new ArrayList<SuperStepCommand>();
		ckpVersion = -1;
	}
	
	/**
	 * The first phase for initializing some global variables.
	 * Only invoked by {@link JobInProgress} at Master.
	 * @param taskId
	 * @param tInfo
	 */
	public synchronized void buildInfo(int taskId, TaskInformation tInfo) {
		this.taskIds[taskId] = tInfo.getTaskId();
		this.verMinIds[taskId] = tInfo.getVerMinId();
		this.ports[taskId] = tInfo.getPort();
		this.hostNames[taskId] = tInfo.getHostName();
	}
	
	/**
	 * Only invoked by {@link JobInProgress} at the Master.
	 * @param _verNum
	 * @param _ckpDir
	 */
	public void initAftBuildingInfo(int _verNum, String _ckpJobDir) {
		initVerIdAndNums(_verNum);
		ckpJobDir = _ckpJobDir;
		
		for (int i = 0; i < taskNum; i++) {
			this.blkNumOfTasks[i] = this.job.getNumBucketsPerTask();
			this.blkLenOfTasks[i] = 
				(int)Math.ceil((double)
						(verMaxIds[i]-verMinIds[i]+1)/this.blkNumOfTasks[i]);
			this.blkNumOfJob += this.blkNumOfTasks[i];
		}
		
		int idxCounter = 0;
		for (int i = 0; i < this.taskNum; i++) {
			this.headBlkIdxOfTasks[i] = idxCounter;
			idxCounter += this.blkNumOfTasks[i];
		}
		
		this.verNumOfBlks = new int[this.blkNumOfJob];
		this.resVerNumOfBlks = new int[this.blkNumOfJob];
		this.resDependMatrix = new boolean[this.blkNumOfJob][this.blkNumOfJob];
	}
	
	/**
	 * The second phase for initializing remaining global variables 
	 * after graph data have been loaded onto local disks/memory.
	 * Only invoked by {@link JobInProgress} at Master.
	 * @param taskId
	 * @param tInfo
	 */
	public synchronized void registerInfo(int taskId, TaskInformation tInfo) {
		this.edgeNum += tInfo.getEdgeNum();
		
		int beginId = this.headBlkIdxOfTasks[taskId];
		int[] localVerNum = tInfo.getVerNumBlks();
		boolean[][] localMatrix = tInfo.getRespondDependency();
		for (int i = 0; i < this.blkNumOfTasks[taskId]; i++) {
			this.verNumOfBlks[i+beginId] = localVerNum[i];
			this.resVerNumOfBlks[i+beginId] = 0;
			this.resDependMatrix[i+beginId] = localMatrix[i];
		}
	}
	
	private void initVerIdAndNums(int _verNum) {
		int[] tmpMin = new int[taskNum], tmpId = new int[taskNum];
		for (int i = 0; i < taskNum; i++) {
			tmpMin[i] = verMinIds[i];
			tmpId[i] = taskIds[i];
		}

		for (int i = 0, swap = 0; i < taskNum; i++) {
			for (int j = i + 1; j < taskNum; j++) {
				if (tmpMin[i] > tmpMin[j]) {
					swap = tmpMin[j]; tmpMin[j] = tmpMin[i]; tmpMin[i] = swap;
					swap = tmpId[j]; tmpId[j] = tmpId[i]; tmpId[i] = swap;
				}
			}
		}

		for (int i = 1; i < taskNum; i++) {
			verMaxIds[tmpId[i-1]] = tmpMin[i] - 1;
		}
		verMaxIds[tmpId[taskNum-1]] = _verNum + verMinIds[tmpId[0]] - 1;
		this.verMinId = tmpMin[0];
		this.verMaxId = verMaxIds[tmpId[taskNum-1]];
		this.verNum = _verNum;
	}
	
	/**
	 * Get the checkpoint directory for this job.
	 * Only valid if {@link initAftBuildingInfo} has been invoked.
	 */
	public String getCheckPointDirForJob() {
		return this.ckpJobDir;
	}
	
	/**
	 * If {@link CommandType} is CHECKPOINT in the finishSuperStep() 
	 * function at the i-th iteration, the most recent available 
	 * checkpoint version is set to i.
	 * @param version
	 */
	public void setAvailableCheckPoint(int version) {
		ckpVersion = version;
	}
	
	/**
	 * Get the most recent available checkpoint version. 
	 * -1 indicates that no checkpoint exists.
	 * @return
	 */
	public int getAvailableCheckPoint() {
		return ckpVersion;
	}
	
	public int getVerNum() {
		return this.verNum;
	}
	
	public long getEdgeNum() {
		return this.edgeNum;
	}
	
	public int getVerMinId() {
		return this.verMinId;
	}
	
	public int getVerMaxId() {
		return this.verMaxId;
	}
	
	public int[] getTaskIds() {
		return taskIds;
	}
	
	public int[] getVerMinIds() {
		return verMinIds;
	}
	
	public int[] getVerMaxIds() {
		return verMaxIds;
	}
	
	public int getVerMaxId(int _taskId) {
		return verMaxIds[_taskId];
	}
	
	public int[] getPorts() {
		return ports;
	}
	
	public String[] getHostNames() {
		return hostNames;
	}
	
	public int getBlkNumOfJob() {
		return this.blkNumOfJob;
	}
	
	public int[] getBlkNumOfTasks() {
		return this.blkNumOfTasks;
	}
	
	public int getBlkNumOfTasks(int taskId) {
		return this.blkNumOfTasks[taskId];
	}
	
	public int getBlkLenOfTasks(int taskId) {
		return this.blkLenOfTasks[taskId];
	}
	
	/**
	 * Get the local VBlock index on the given task, for a vertex vid.
	 * @param taskId
	 * @param vId
	 * @return
	 */
	public int getLocalBlkIdx(int tid, int vid) {
		return (vid-this.verMinIds[tid])/this.blkLenOfTasks[tid];
	}
	
	/**
	 * Get the global VBlock index of this job.
	 * @param _dstTid
	 * @param _dstBid
	 * @return
	 */
	public int getGlobalBlkIdx(int _dstTid, int _dstBid) {
		return (this.headBlkIdxOfTasks[_dstTid] + _dstBid);
	}
	
	public void archiveCommand(SuperStepCommand command) {
		command.compact();
		commands.add(command);
	}
	
	public SuperStepCommand getCommand(int curIteNum) {
		return commands.get(curIteNum-1);
	}
	
	public void archiveRecoveryCommand(SuperStepCommand command) {
		command.compact();
		recoveryCommands.add(command);
	}
	
	public Double getQ(int curIteNum) {
		if (curIteNum < 1) {
			return 0.0;
		} else {
			return commands.get(curIteNum-1).getMetricQ();
		}
	}
	
	/**
	 * Only invoked by {@link JobInProgress} at Master.
	 * @param taskId
	 * @param nums
	 */
	public synchronized void updateRespondVerNumOfBlks(int taskId, int[] nums) {
		int beginId = this.headBlkIdxOfTasks[taskId];
		for (int i = 0; i < this.blkNumOfTasks[taskId]; i++) {
			this.resVerNumOfBlks[i+beginId] = nums[i];
		}
	}
	
	/**
	 * Return the actual communication route table for the given task. 
	 * This table will be used to guide the pull operations, 
	 * to avoid sending useless pulling requests.
	 * Only invoked by {@link JobInProgress} at Master.
	 * @param _dstTid
	 * @return
	 */
	public ArrayList<Integer>[] getActualRouteTable(int _dstTid, 
			CommandType commandType) {
		int beginId = this.headBlkIdxOfTasks[_dstTid];
		int dstBlkIdx = 0;
		ArrayList<Integer>[] route = new ArrayList[this.blkNumOfTasks[_dstTid]];
		
		for (int i = 0; i < this.blkNumOfTasks[_dstTid]; i++) {
			route[i] = new ArrayList<Integer>();
			dstBlkIdx = beginId + i;
			
			for (int srcTid = 0; srcTid < this.taskNum; srcTid++) {
				int headBlkIdx = this.headBlkIdxOfTasks[srcTid];
				int srcBlkIdx = 0;
				boolean find = false;
				for (int j = 0; j < this.blkNumOfTasks[srcTid]; j++) {
					srcBlkIdx = headBlkIdx + j;
					if ((commandType==CommandType.RECOVER) || 
							commandType==CommandType.RECOVERED || 
							(this.resVerNumOfBlks[srcBlkIdx]>0 
							&& this.resDependMatrix[srcBlkIdx][dstBlkIdx])) {
						//has updated, && has edges.
						find = true;
						break;
					}
				}
				if (find) {
					route[i].add(srcTid);
				}
			}//loop all send sides
		}//loop all target VBlocks at the given task(receiver side)
		
		return route;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("\n");
		
		sb.append("\nDetailInfo: iteration\tcommand\taggregator\tmodel" +
				"\truntime\tmetric_Q\tfailed");
		for (int index = 0; index < commands.size(); index++) {
			sb.append("\n   ite[" + index + "]  ");
			sb.append(commands.get(index));
		}
		
		if (recoveryCommands.size() > 0) {
			sb.append("\n\nRecoveryInfo: iteration\tcommand\taggregator\tmodel" +
			"\truntime\tmetric_Q\tfailed");
			double time = 0.0;
			for (int index = 0; index < recoveryCommands.size(); index++) {
				sb.append("\n   ite[" + index + "]  ");
				sb.append(recoveryCommands.get(index));
				time += recoveryCommands.get(index).getIterationTime();
			}
			sb.append("\n   TimeOfRecomputation = ");
			sb.append(time);
			sb.append(" sec");
		}
		
		return sb.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.verNum = in.readInt();
		this.edgeNum = in.readLong();
		this.verMinId = in.readInt();
		this.verMaxId = in.readInt();
		this.blkNumOfJob = in.readInt();
		this.ckpJobDir = Text.readString(in);
		
		this.taskNum = in.readInt();
		this.taskIds = new int[taskNum];
		this.verMinIds = new int[taskNum];
		this.verMaxIds = new int[taskNum];
		this.ports = new int[taskNum];
		this.hostNames = new String[taskNum];
		this.blkNumOfTasks = new int[this.taskNum];
		this.blkLenOfTasks = new int[this.taskNum];
		this.headBlkIdxOfTasks = new int[this.taskNum];
		for (int i = 0; i < taskNum; i++) {
			taskIds[i] = in.readInt();
			verMinIds[i] = in.readInt();
			verMaxIds[i] = in.readInt();
			ports[i] = in.readInt();
			hostNames[i] = Text.readString(in);
			
			this.blkNumOfTasks[i] = in.readInt();
			this.blkLenOfTasks[i] = in.readInt();
			this.headBlkIdxOfTasks[i] = in.readInt();
		}
		
		this.ckpVersion = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.verNum);
		out.writeLong(this.edgeNum);
		out.writeInt(this.verMinId);
		out.writeInt(this.verMaxId);
		out.writeInt(this.blkNumOfJob);
		Text.writeString(out, this.ckpJobDir);
		
		out.writeInt(this.taskNum);
		for (int i = 0; i < taskNum; i++) {
			out.writeInt(taskIds[i]);
			out.writeInt(verMinIds[i]);
			out.writeInt(verMaxIds[i]);
			out.writeInt(ports[i]);
			Text.writeString(out, hostNames[i]);
			
			out.writeInt(this.blkNumOfTasks[i]);
			out.writeInt(this.blkLenOfTasks[i]);
			out.writeInt(this.headBlkIdxOfTasks[i]);
		}
		
		out.writeInt(this.ckpVersion);
	}
}
