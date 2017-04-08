/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.ipc.MasterProtocol;
import org.apache.hama.ipc.WorkerProtocol;
import org.apache.hama.myhama.util.LocalFileOperation;
import org.apache.hama.myhama.util.TaskReportContainer;
import org.apache.log4j.LogManager;


/**
 * A Groom Server (shortly referred to as groom) is a process that performs bsp
 * tasks assigned by BSPMaster. Each groom contacts the BSPMaster, and it takes
 * assigned tasks and reports its status by means of periodical piggybacks with
 * BSPMaster. Each groom is designed to run with HDFS or other distributed
 * storages. Basically, a groom server and a data node should be run on one
 * physical node.
 */
public class GroomServer implements Runnable, WorkerProtocol, 
		BSPTaskTrackerProtocol {
  public static final Log LOG = LogFactory.getLog(GroomServer.class);
  private volatile static int REPORT_INTERVAL = 1 * 1000;
  Configuration conf;

  // Constants
  static enum State {
    NORMAL, COMPUTE, SYNC, BARRIER, STALE, INTERRUPTED, DENIED
  };

  // Running States and its related things
  volatile boolean initialized = false;
  volatile boolean running = true;
  volatile boolean shuttingDown = false;
  boolean justInited = true;
  GroomServerStatus status = null;

  // Attributes
  InetSocketAddress peerAddress;
  String groomServerName;
  String localHostname;
  InetSocketAddress bspMasterAddr;

  // FileSystem
  // private LocalDirAllocator localDirAllocator;
  Path systemDirectory = null;
  FileSystem systemFS = null;

  //change in version-0.2.4 change the "maxCurrentTasks=1" to "maxTasks=5"
  private int maxTaskSlot = 1;
  private int usedTaskSlot=0;
  
  //the BSPPeerForJob to JobID
  LaunchThread launchT = new LaunchThread();
  public Map<BSPJobID,LocalJobInProgress> runningJips = null;
  
  Map<BSPJobID, String> GroomRPCForJob=new HashMap<BSPJobID, String>();

  private String rpcServer;
  private Server workerServer;
  MasterProtocol masterClient;

  InetSocketAddress taskReportAddress;
  Server taskReportServer = null;
  
  /** generate a port number for every newly assigned task */
  private AtomicInteger taskCommPortCounter;
  
  public GroomServer(Configuration conf) throws IOException {
    LOG.info("groom start");
    this.conf = conf;

    String mode = conf.get("bsp.master.address");
    if (!mode.equals("local")) {
      bspMasterAddr = BSPMaster.getAddress(conf);
    }

    // FileSystem local = FileSystem.getLocal(conf);
    // this.localDirAllocator = new LocalDirAllocator("bsp.local.dir");
  }

  public synchronized void initialize() throws IOException {
	taskCommPortCounter = new AtomicInteger(0);
    if (this.conf.get(Constants.PEER_HOST) != null) {
      this.localHostname = conf.get(Constants.PEER_HOST);
    }

    if (localHostname == null) {
      this.localHostname = DNS.getDefaultHost(
          conf.get("bsp.dns.interface", "default"),
          conf.get("bsp.dns.nameserver", "default"));
    }
    // check local disk
    checkLocalDir(getLocalDir());

    this.maxTaskSlot = conf.getInt(Constants.MAX_TASKS, 1);
    LOG.info("max task slots is : " + this.maxTaskSlot);
    
    this.runningJips = new TreeMap<BSPJobID,LocalJobInProgress>();
    
    this.conf.set(Constants.PEER_HOST, localHostname);
    this.conf.set(Constants.GROOM_RPC_HOST, localHostname);
    int peerPort = conf
        .getInt(Constants.PEER_PORT, Constants.DEFAULT_PEER_PORT);
    peerAddress = new InetSocketAddress(localHostname, peerPort);

    int rpcPort = -1;
    String rpcAddr = null;
    if (false == this.initialized) {
      rpcAddr = conf.get(Constants.GROOM_RPC_HOST,
          Constants.DEFAULT_GROOM_RPC_HOST);
      rpcPort = conf.getInt(Constants.GROOM_RPC_PORT,
          Constants.DEFAULT_GROOM_RPC_PORT);
      if (-1 == rpcPort || null == rpcAddr)
        throw new IllegalArgumentException("Error rpc address " + rpcAddr
            + " port" + rpcPort);
      this.workerServer = RPC.getServer(this, rpcAddr, rpcPort, conf);
      this.workerServer.start();
      this.rpcServer = rpcAddr + ":" + rpcPort;

      LOG.info("Worker rpc server --> " + rpcServer);
    }

    @SuppressWarnings("deprecation")
    String address = NetUtils.getServerAddress(conf,
        "bsp.groom.report.bindAddress", "bsp.groom.report.port",
        "bsp.groom.report.address");
    InetSocketAddress socAddr = NetUtils.createSocketAddr(address);
    String bindAddress = socAddr.getHostName();
    int tmpPort = socAddr.getPort();

    // RPC initialization
    // TODO numHandlers should be a ..
    this.taskReportServer = RPC.getServer(this, bindAddress, tmpPort, 10,
        false, this.conf);

    this.taskReportServer.start();

    // get the assigned address
    this.taskReportAddress = taskReportServer.getListenerAddress();
    this.conf.set("bsp.groom.report.address", taskReportAddress.getHostName()
        + ":" + taskReportAddress.getPort());
    LOG.info("GroomServer up at: " + this.taskReportAddress);
    
    this.groomServerName = "groomd_" + getPeerName().replace(':', '_');
    
    LOG.info("Starting groom: " + this.groomServerName);

    DistributedCache.purgeCache(this.conf);

    // establish the communication link to bsp master
    this.masterClient = (MasterProtocol) RPC.waitForProxy(MasterProtocol.class,
        MasterProtocol.versionID, bspMasterAddr, conf);
    // enroll in bsp master
    if (-1 == rpcPort || null == rpcAddr)
      throw new IllegalArgumentException("Error rpc address " + rpcAddr
          + " port" + rpcPort);
    if (!this.masterClient.register(new GroomServerStatus(groomServerName,
            getPeerName(), GroomRPCForJob, cloneAndResetRunningTaskStatuses(), 
            0, maxTaskSlot, usedTaskSlot, 0, this.rpcServer))) {
          LOG.error("There is a problem in establishing communication"
              + " link with BSPMaster");
          throw new IOException("There is a problem in establishing"
              + " communication link with BSPMaster.");
        }
    this.initialized = true;
    this.running = true;
    this.launchT.start();
  }

  /**
   * @return the string as host:port of this Peer
   */
  private String getPeerName() {
	  if (peerAddress == null) {
		  return null;
	  } else {
		  return peerAddress.getHostName() + ":" + peerAddress.getPort();
	  }
  }
  
  /** Return the port at which the tasktracker bound to */
  public synchronized InetSocketAddress getTaskTrackerReportAddress() {
    return taskReportAddress;
  }

  private class LaunchThread extends Thread {
	  final BlockingQueue<Directive> buffer = 
		  new LinkedBlockingQueue<Directive>();
	  
	  public void put(Directive directive) {
		  try {
			  buffer.put(directive);
	      } catch (InterruptedException ie) {
	    	  LOG.error("Unable to put directive into queue.", ie);
	    	  Thread.currentThread().interrupt();
	      }
	  }

	  @Override
	  public void run() {
		  while (running) {
			  try {
				  Directive directive = buffer.take();
				  ArrayList<GroomServerAction> actions = directive.getActionList();
				    LOG.debug("Got Response from BSPMaster with "
				        + ((actions != null) ? actions.size() : 0) + " actions");
				    
				  // perform actions
				  if (actions != null) {
					  for (GroomServerAction action : actions) {
						  if (action instanceof LaunchTaskAction) {
							  startNewTask((LaunchTaskAction) action, directive);
						  } else {
							  // TODO Use the cleanup thread
							  // tasksToCleanup.put(action);
							  KillTaskAction killAction = (KillTaskAction) action;
							  TaskInProgress tip = 
								  getTaskInProgress(killAction.getTaskID());
							  if (tip != null) {
								  //LOG.warn(killAction.getTaskID() + " is killed!");
								  tip.taskStatus.setRunState(TaskStatus.State.KILLED);
								  tip.killAndCleanup(true);
							  }
						  }
				      }
				  }
			  } catch (InterruptedException ie) {
				  LOG.error("Unable to retrieve directive from the queue.", ie);
				  Thread.currentThread().interrupt();
			  } catch (Exception e) {
				  if(running) {
					  LOG.error("Fail to execute directive.", e);
				  }
			  }
		  }
	  }
  }
  
  private TaskInProgress getTaskInProgress(TaskAttemptID tid) {
	  TaskInProgress tip = null;
	  for (LocalJobInProgress jip: runningJips.values()) {
		  if (jip.containTask(tid)) {
			  tip = jip.getTaskInProgress(tid);
			  break;
		  }
	  }
	  
	  return tip;
  }
  
  private List<TaskInProgress> getAllRunningTaskInProgress() {
	  List<TaskInProgress> result = new ArrayList<TaskInProgress>();
	  for (LocalJobInProgress jip: runningJips.values()) {
		  result.addAll(jip.getAllTaskInProgress());
	  }
	  return result;
  }
  
  @Override
  public void dispatch(BSPJobID jobId, Directive directive) throws IOException {
	  this.launchT.put(directive);
  }

  private static void checkLocalDir(String localDir)
      throws DiskErrorException {
    boolean writable = false;
    LOG.info(localDir);

    if (localDir != null) {
    	try {
            DiskChecker.checkDir(new File(localDir));
            writable = true;
    	} catch (DiskErrorException e) {
    		LOG.warn("BSP Processor local " + e.getMessage());
    	}
    }

    if (!writable)
      throw new DiskErrorException("local directory is not writable");
  }
  
  private String getLocalDir() {
    return conf.get("bsp.local.dir");
  }
  
  /**
   * Delete all files in the given directory. 
   * The given directory will be preserved.
   * @param dir
   */
  private void emptyDir(String dir) {
	  try {
		  File rootDir = new File(dir);
		  if (rootDir.exists()) {
			  LocalFileOperation localFileOpt = new LocalFileOperation();
			  File[] localDirs = rootDir.listFiles();
			  for (int i = 0; i < localDirs.length; i++) {
				  localFileOpt.deleteDir(localDirs[i]);
			  }
		  }
	  } catch (Exception e) {
		  LOG.error("[deleteLocalFiles]", e);
	  }
  }

  private void startCleanupThreads() throws IOException {

  }
  
  private void clearTask(TaskInProgress tip) throws Exception {
	  BSPJobID jobId = tip.getJobId();
	  TaskAttemptID taskId = tip.getStatus().getTaskId();
	  if (!runningJips.containsKey(jobId)) {
		  return;
	  } else {
		  boolean jobIsDone = 
			  runningJips.get(jobId).removeTaskInProgress(taskId);
		  if (jobIsDone) {
			  runningJips.get(jobId).close();
			  runningJips.remove(jobId);
		  }
	  }
	  
	  usedTaskSlot--;
  }

  public State offerService() throws Exception {
	  while (running && !shuttingDown) {
		  try {
	    	  List<TaskInProgress> runningList = getAllRunningTaskInProgress();
	    	  List<TaskStatus> reportList = new ArrayList<TaskStatus>();
	    	  int count = runningList.size();
	    	  for (int i = 0; i < count; i++) {
	    		  TaskInProgress tip = runningList.get(i);
	    		  TaskStatus taskStatus = tip.getStatus();
	    		  
	    		  /** 
	    		   * Only tasks with UNASSIGNED/RUNNING will be repeatedly 
	    		   * reported. Tasks with other statuses are reported only 
	    		   * once and then deleted. 
	    		   * */
	    		  switch(taskStatus.getRunState()) {
	    		  case UNASSIGNED: break;
	    		  case RUNNING: 
	    			  if (!tip.runner.isAlive()) {
		    			  tip.runtimeError();
		    			  LOG.error(taskStatus.getTaskId()+" is dead (failed task)");
		    			  clearTask(tip);
		    		  }
	    			  break;
	    		  case SUCCEEDED: 
	    			  LOG.info(taskStatus.getTaskId()+" is done");
	    			  clearTask(tip);
	    			  break;
	    		  case FAILED: 
	    			  LOG.error(taskStatus.getTaskId()+" fails");
	    			  if (tip.runner.isAlive()) {
	    					tip.killAndCleanup(true);
	    			  }
	    			  clearTask(tip);
	    			  break;
	    		  case KILLED: 
	    			  LOG.warn(taskStatus.getTaskId()+" is killed by TaskTracker");
	    			  clearTask(tip);
	    			  break;
	    		  }
	    		  
	    		  reportList.add(taskStatus);
	    	  }
	    	  GroomServerStatus gss = new GroomServerStatus(groomServerName, 
	    			  getPeerName(), GroomRPCForJob, reportList, 0,
	    			  maxTaskSlot, usedTaskSlot, 0, this.rpcServer);
	    	  try {
	    		  boolean ret = masterClient.report(new Directive(gss));
	    		  if(!ret){
	    			  LOG.error("fail to renew");
	    		  }
	    	  } catch (Exception ioe) {
	    		  LOG.error("Fail to communicate with BSPMaster for reporting.", ioe);
	    	  }
	    	  Thread.sleep(REPORT_INTERVAL);
	      } catch (InterruptedException ie) {
	    	  LOG.error(ie);
	      }
	      
	      try {
	    	  if (justInited) {
	    		  String dir = masterClient.getSystemDir();
	    		  if (dir == null) {
	    			  LOG.error("Fail to get system directory.");
	    			  throw new IOException("Fail to get system directory.");
	    		  }
	    		  systemDirectory = new Path(dir);
	    		  systemFS = systemDirectory.getFileSystem(conf);
	    	  }
	    	  justInited = false;
	      } catch (DiskErrorException de) {
	    	  String msg = "Exiting groom server for disk error:\n"
	    		  + StringUtils.stringifyException(de);
	    	  LOG.error(msg);
	    	  return State.STALE;
	      } catch (RemoteException re) {
	    	  return State.DENIED;
	      } catch (Exception except) {
	    	  String msg = "Caught exception: "
	    		  + StringUtils.stringifyException(except);
	    	  LOG.error(msg);
	      }
	  }
	  return State.NORMAL;
  }

  private void startNewTask(LaunchTaskAction action, Directive directive) {
    Task t = action.getTask();
    BSPJob jobConf = null;
    try {
      jobConf = new BSPJob(t.getJobID(), t.getJobFile());
    } catch (IOException e1) {
      LOG.error("startNewTask-jobConf"+e1);
    }

    TaskInProgress tip = new TaskInProgress(t, jobConf, this.groomServerName);
    try {
      localizeJob(tip, directive);
    } catch (Throwable e) {
      String msg = ("Error initializing " + tip.getTask().getTaskID() 
    		  + ":\n" + StringUtils.stringifyException(e));
      LOG.warn(msg);
    }
  }

  private void localizeJob(TaskInProgress tip, Directive directive) 
  		throws IOException {
	  Task task = tip.getTask();
	  conf.addResource(task.getJobFile());
	  String dir = getLocalTaskDir(task.getJobID(), task.getTaskID());
    
	  Path localJobFile = new Path(dir + "/job.xml");

	  addJob(task.getJobID(), localJobFile, tip, directive);
	  BSPJob jobConf = null;
    
	  Path localJarFile = new Path(dir + "/job.jar");
	  systemFS.copyToLocalFile(new Path(task.getJobFile()), localJobFile);

	  HamaConfiguration conf = new HamaConfiguration();
	  conf.addResource(localJobFile);
	  jobConf = new BSPJob(conf, task.getJobID().toString());

	  Path jarFile = new Path(jobConf.getJar());
	  jobConf.setJar(localJarFile.toString());

	  if (jarFile != null) {
		  systemFS.copyToLocalFile(jarFile, localJarFile);

          // also unjar the job.jar files in workdir
          File workDir = new File(
              new File(localJobFile.toString()).getParent(), "work");
          if (!workDir.mkdirs()) {
        	  if (!workDir.isDirectory()) {
        		  throw new IOException("Mkdirs failed to create "
        				  + workDir.toString());
        	  }
          }
          RunJar.unJar(new File(localJarFile.toString()), workDir);
	  }
	  launchTaskForJob(tip, jobConf);
  }

  private void launchTaskForJob(TaskInProgress tip, BSPJob jobConf) {
	  try {
		  tip.setJobConf(jobConf);
		  tip.launchTask();
	  } catch (Throwable ie) {
		  tip.taskStatus.setRunState(TaskStatus.State.FAILED);
		  String error = StringUtils.stringifyException(ie);
		  LOG.info(error);
	  }
  }

  private void addJob(BSPJobID jobId, Path localJobFile,
      TaskInProgress tip, Directive directive) {
    synchronized (runningJips) {
      if (!runningJips.containsKey(jobId)) {
        //create a new BSPPeerForJob for a new job
        try{
        	LocalJobInProgress jip = 
        		new LocalJobInProgress(jobId, GroomServer.this);
        	runningJips.put(jobId, jip);
        	
        	GroomRPCForJob.put(jobId, "no-used");      	
        }catch(IOException e){
        	LOG.error("Failed to create a BSPPeerForJob for a new job" 
        			+ jobId.toString());
        }
        
      }
    }
  }

  private synchronized List<TaskStatus> cloneAndResetRunningTaskStatuses() {
    List<TaskStatus> result = new ArrayList<TaskStatus>();
    for (TaskInProgress tip : getAllRunningTaskInProgress()) {
      TaskStatus status = tip.getStatus();
      result.add((TaskStatus) status.clone());
    }
    return result;
  }

  public void run() {
    try {
      initialize();
      startCleanupThreads();     
      boolean denied = false;
      while (running && !shuttingDown && !denied) {

        boolean staleState = false;
        try {
          while (running && !staleState && !shuttingDown && !denied) {
            try {
              State osState = offerService();
              if (osState == State.STALE) {
                staleState = true;
              } else if (osState == State.DENIED) {
                denied = true;
              }
            } catch (Exception e) {
              if (!shuttingDown) {
                LOG.info("Lost connection to BSP Master [" + bspMasterAddr
                    + "].  Retrying...", e);
                try {
                  Thread.sleep(5000);
                } catch (InterruptedException ie) {
                }
              }
            }
          }
        } finally {
          // close();
        }
        if (shuttingDown) {
          return;
        }
        LOG.warn("Reinitializing local state");
        initialize();
      }
    } catch (IOException ioe) {
      LOG.error("Got fatal exception while reinitializing GroomServer: "
          + StringUtils.stringifyException(ioe));
      return;
    }
  }

  public synchronized void shutdown() throws IOException {
	  LOG.info("shutdown the GroomServer daemon process, please wait...");
	  shuttingDown = true;
	  close();
  }
  
  public synchronized void close() throws IOException {
	  this.running = false;
	  this.initialized = false;
	  this.launchT.stop();
	  
	  for (Entry<BSPJobID,LocalJobInProgress> e : runningJips.entrySet()){
		  e.getValue().close();
	  }
	  
	  emptyDir(getLocalDir());
	  this.workerServer.stop();
	  RPC.stopProxy(masterClient);
	  if (taskReportServer != null) {
		  taskReportServer.stop();
		  taskReportServer = null;
	  }
	  LOG.info("stop RPC server on groom server successfully");
  }

  public static Thread startGroomServer(final GroomServer hrs) {
    return startGroomServer(hrs, "regionserver" + hrs.groomServerName);
  }

  public static Thread startGroomServer(final GroomServer hrs, final String name) {
    Thread t = new Thread(hrs);
    t.setName(name);
    t.start();
    return t;
  }

  // /////////////////////////////////////////////////////
  // TaskInProgress maintains all the info for a Task that
  // lives at this GroomServer. It maintains the Task object,
  // its TaskStatus, and the BSPTaskRunner.
  // /////////////////////////////////////////////////////
  class TaskInProgress {
    Task task;
    BSPJobID jobId;
    public BSPJob jobConf;
    BSPJob localJobConf;
    BSPTaskRunner runner;
    volatile boolean done = false;
    volatile boolean wasKilled = false;
    private TaskStatus taskStatus;

    public TaskInProgress(Task task, BSPJob jobConf, String groomServer) {
      this.task = task;
      this.jobConf = jobConf;
      this.localJobConf = null;
      this.taskStatus = new TaskStatus(task.getJobID(), task.getTaskID(), 0.0f,
          TaskStatus.State.UNASSIGNED, "running", groomServer,
          TaskStatus.Phase.STARTING);
    }

    private void localizeTask(Task task) throws IOException {
      String dir = getLocalTaskDir(task.getJobID(), task.getTaskID());
      Path localJobFile = new Path(dir + "/job.xml");
      Path localJarFile = new Path(dir + "/job.jar");

      String jobFile = task.getJobFile();
      systemFS.copyToLocalFile(new Path(jobFile), localJobFile);
      task.setJobFile(localJobFile.toString());

      localJobConf = new BSPJob(task.getJobID(), localJobFile.toString());
      localJobConf.set("bsp.task.id", task.getTaskID().toString());
      String jarFile = localJobConf.getJar();
      task.setBSPJob(localJobConf);
      if (jarFile != null) {
        systemFS.copyToLocalFile(new Path(jarFile), localJarFile);
        localJobConf.setJar(localJarFile.toString());
      }

      LOG.debug("localizeTask : " + localJobConf.getJar());
      LOG.debug("localizeTask : " + localJobFile.toString());
    }

    public synchronized void setJobConf(BSPJob jobConf) {
      this.jobConf = jobConf;
    }

    public synchronized BSPJob getJobConf() {
      return localJobConf;
    }

    public BSPJobID getJobId() {
    	return this.jobId;
    }
    
    public void launchTask() throws IOException {
      localizeTask(task);
      taskStatus.setRunState(TaskStatus.State.RUNNING);
      jobId=localJobConf.getJobID();
      
      this.runner = task.createRunner(GroomServer.this);
      this.runner.start();
      synchronized (this) {
          runningJips.get(jobId).addTaskInProgress(task.getTaskAttemptId(), this);
          usedTaskSlot++;
      }
    }

    /**
     * This task has run on too long, and should be killed.
     */
    public synchronized void killAndCleanup(boolean wasFailure)
        throws IOException {
      runner.kill();
    }

    /**
     */
    public Task getTask() {
      return task;
    }

    public void updateProgress(TaskReportContainer report) {
    	//LOG.info(taskReportContainer.getCurrentProgress());
    	taskStatus.setProgress(report.getCurrentProgress());
    	taskStatus.setUsedMemory(report.getUsedMemory());
    	taskStatus.setTotalMemory(report.getTotalMemory());
    	
    	if (report.isDone() && 
    			taskStatus.getRunState()!=TaskStatus.State.FAILED) {
    		taskStatus.setRunState(TaskStatus.State.SUCCEEDED);
    	}
    }
    
    public void runtimeError() throws IOException {
    	LOG.error(task.getTaskAttemptId() + " throws exception");
		taskStatus.setRunState(TaskStatus.State.FAILED);
		/*if (this.runner.isAlive()) {
			killAndCleanup(true);
		}*/
    }
    
    /**
     */
    public synchronized TaskStatus getStatus() {
      return taskStatus;
    }

    /**
     */
    public TaskStatus.State getRunState() {
      return taskStatus.getRunState();
    }

    public boolean wasKilled() {
      return wasKilled;
    }

    @Override
    public boolean equals(Object obj) {
      return (obj instanceof TaskInProgress)
          && task.getTaskID().equals(
              ((TaskInProgress) obj).getTask().getTaskID());
    }

    @Override
    public int hashCode() {
      return task.getTaskID().hashCode();
    }
  }

  public boolean isRunning() {
    return running;
  }

  public static GroomServer constructGroomServer(
      Class<? extends GroomServer> groomServerClass, final Configuration conf2) {
    try {
      Constructor<? extends GroomServer> c = groomServerClass
          .getConstructor(Configuration.class);
      return c.newInstance(conf2);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " + "Master: "
          + groomServerClass.toString(), e);
    }
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(WorkerProtocol.class.getName())) {
      return WorkerProtocol.versionID;
    } else if (protocol.equals(BSPTaskTrackerProtocol.class.getName())) {
      return BSPTaskTrackerProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to GroomServer: " + protocol);
    }
  }

  /**
   * The main() for child processes.
   */
  public static class Child{

	public static void main(String[] args) throws Throwable {
      //LOG.info("Child starting");

      HamaConfiguration defaultConf = new HamaConfiguration();
      // report address
      String host = args[0];
      int port = Integer.parseInt(args[1]);
      InetSocketAddress address = new InetSocketAddress(host, port);
      TaskAttemptID taskid = TaskAttemptID.forName(args[2]);

      // //////////////////
      BSPTaskTrackerProtocol umbilical = (BSPTaskTrackerProtocol) RPC.getProxy(
          BSPTaskTrackerProtocol.class, BSPTaskTrackerProtocol.versionID, address,
          defaultConf);

      Task task = umbilical.getTask(taskid);
      defaultConf.addResource(new Path(task.getJobFile()));
      BSPJob job = new BSPJob(task.getJobID(), task.getJobFile());

      try {
        // use job-specified working directory
        FileSystem.get(job.getConf()).setWorkingDirectory(
            job.getWorkingDirectory());       
        
        task.run(job, task, umbilical, args[3]); // run the task
      } catch (Exception e) {
    	  LOG.error("task.run()", e);
    	  umbilical.runtimeError(job.getJobID(), task.taskId);
      } finally {
        RPC.stopProxy(umbilical);
        MetricsContext metricsContext = MetricsUtil.getContext("mapred");
        metricsContext.close();
        // Shutting down log4j of the child-vm...
        // This assumes that on return from Task.run()
        // there is no more logging done.
        LogManager.shutdown();
      }
    }
  }

  @Override
  public Task getTask(TaskAttemptID taskid) throws IOException {
    TaskInProgress tip = getTaskInProgress(taskid);
    if (tip != null) {
      return tip.getTask();
    } else {
      return null;
    }
  }
  
  @Override
  public String getHostName() {
	  return this.conf.get(Constants.PEER_HOST, Constants.DEFAULT_PEER_HOST);
  }
  
  @Override
  public int getLocalTaskNumber(BSPJobID jobId) {
	  return runningJips.get(jobId).getLocalTaskNumber();
  }

  @Override
  public void ping(BSPJobID jobId, TaskAttemptID taskId, 
		  TaskReportContainer report) {
	  //LOG.info(taskId + " enter ping");
	  runningJips.get(jobId).updateProgress(taskId, report);
	  //LOG.info(taskId + " leave ping");
  }
  
  @Override
  public void runtimeError(BSPJobID jobId, TaskAttemptID taskId) {
	  try {
		  //LOG.info("runtimeError of " + taskId);
		  runningJips.get(jobId).runtimeError(taskId);
	  } catch (Exception e1) {
		  LOG.error("fail to report error!", e1);
	  }
  }
  
	@Override
	public String getLocalJobDir(BSPJobID jobId) throws IOException {
		Path path = 
			new BSPJob(new HamaConfiguration()).getLocalPath(jobId.toString());
		return path.toString();
	}
	
	@Override
	public String getLocalTaskDir(BSPJobID jobId, TaskAttemptID taskId) 
		throws IOException {
		Path path = 
			new BSPJob(new HamaConfiguration()).getLocalPath(
					jobId.toString() + "/" + taskId);
		return path.toString();
	}
	
	@Override
	public int getPort() {
		return taskCommPortCounter.incrementAndGet();
	}
}
