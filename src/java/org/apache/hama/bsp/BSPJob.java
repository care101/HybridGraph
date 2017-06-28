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

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.UserTool;
import org.apache.hama.myhama.io.InputFormat;
import org.apache.hama.myhama.io.OutputFormat;
import org.apache.hama.Constants;


/**
 * A BSP job configuration.
 * 
 * BSPJob is the primary interface for a user to describe a BSP job 
 * to the Hama BSP framework for execution.
 */
public class BSPJob extends BSPJobContext {
	public static final Log LOG = LogFactory.getLog(BSPJob.class);
	
  public static enum JobState {
    DEFINE, RUNNING
  };

  private JobState state = JobState.DEFINE;
  private BSPJobClient jobClient;
  private RunningJob info;

  public BSPJob() throws IOException {
    this(new HamaConfiguration());
  }

  public BSPJob(HamaConfiguration conf) throws IOException {
    super(conf, null);
    jobClient = new BSPJobClient(conf);
  }

  public BSPJob(HamaConfiguration conf, String jobName) 
  		throws IOException {
    this(conf);
    setJobName(jobName);
  }

  public BSPJob(BSPJobID jobID, String jobFile) throws IOException {
    super(new Path(jobFile), jobID);
  }

  public BSPJob(HamaConfiguration conf, Class<?> exampleClass) 
  		throws IOException {
    this(conf);
    setJarByClass(exampleClass);
  }

  public BSPJob(HamaConfiguration conf, int numPeer) {
    super(conf, null);
    this.setNumBspTask(numPeer);
  }

  private void ensureState(JobState state) 
  		throws IllegalStateException {
    if (state != this.state) {
      throw new IllegalStateException("Job in state " + this.state
          + " instead of " + state);
    }
  }

  // /////////////////////////////////////
  // Setter for Job Submission
  // /////////////////////////////////////
  public void setWorkingDirectory(Path dir) throws IOException {
    ensureState(JobState.DEFINE);
    dir = new Path(getWorkingDirectory(), dir);
    conf.set(WORKING_DIR, dir.toString());
  }
 
  
  /**
   * Set the {@link BC-BSP InputFormat} for the job.
   * 
   * @param cls
   *            the <code>InputFormat</code> to use
   * @throws IllegalStateException
   */
  @SuppressWarnings("unchecked")
  public void setInputFormatClass(Class<? extends InputFormat> cls)
          throws IllegalStateException {
      conf.setClass(Constants.USER_JOB_INPUT_FORMAT_CLASS, cls,
              InputFormat.class);
  }

  /**
   * Get the {@link BC-BSP InputFormat} class for the job.
   * 
   * @return the {@link BC-BSP InputFormat} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends InputFormat<?, ?>> getInputFormatClass()
          throws ClassNotFoundException {
      return ( Class<? extends InputFormat<?, ?>> ) conf
              .getClass(Constants.USER_JOB_INPUT_FORMAT_CLASS,
                      InputFormat.class);
  }
  
  /**
   * Set the {@link BC-BSP OutputFormat} for the job.
   * 
   * @param cls
   *            the <code>OutputFormat</code> to use
   * @throws IllegalStateException
   */
  @SuppressWarnings("unchecked")
  public void setOutputFormatClass(Class<? extends OutputFormat> cls)
          throws IllegalStateException {
      conf.setClass(Constants.USER_JOB_OUTPUT_FORMAT_CLASS, cls,
              OutputFormat.class);
  }
  
  @SuppressWarnings("unchecked")
  public Class<? extends OutputFormat<?,?>> getOutputFormatClass() 
     throws ClassNotFoundException {
    return (Class<? extends OutputFormat<?,?>>) 
      conf.getClass(Constants.USER_JOB_OUTPUT_FORMAT_CLASS, OutputFormat.class);
  }
  
  @SuppressWarnings("unchecked")
  public void setUserToolClass(Class<? extends UserTool<?, ?, ?, ?>> cls) 
  		throws IllegalStateException {
      conf.setClass(Constants.USER_JOB_TOOL_CLASS, cls, UserTool.class);
  }
  
  @SuppressWarnings("unchecked")
  public Class<? extends UserTool<?, ?, ?, ?>> getRecordFormatClass() 
		throws ClassNotFoundException {
	  return (Class<? extends UserTool<?, ?,  ?, ?>>) 
	  	conf.getClass(Constants.USER_JOB_TOOL_CLASS, UserTool.class);
  }
  
  /**
   * Set the BSP algorithm class for the job.
   * 
   * @param cls
   * @throws IllegalStateException
   */
  @SuppressWarnings("unchecked")
public void setBspClass(Class<? extends BSP> cls)
      throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(WORK_CLASS_ATTR, cls, BSP.class);
  }

  @SuppressWarnings("unchecked")
  public Class<? extends BSP> getBspClass() {
    return (Class<? extends BSP>) conf.getClass(WORK_CLASS_ATTR, BSP.class);
  }

  public void setJar(String jar) {
    conf.set("bsp.jar", jar);
  }

  public void setJarByClass(Class<?> cls) {
    String jar = findContainingJar(cls);
    if (jar != null) {
      conf.set("bsp.jar", jar);
    }
  }

  private static String findContainingJar(Class<?> my_class) {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    try {
      for (Enumeration<URL> itr = loader.getResources(class_file); itr
          .hasMoreElements();) {

        URL url = itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  public void setJobName(String name) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.set("bsp.job.name", name);
  }

  /*public void setInputPath(HamaConfiguration conf, Path iNPUTPATH) {

  }*/

  public void setUser(String user) {
    conf.set("user.name", user);
  }

  // /////////////////////////////////////
  // Methods for Job Control
  // /////////////////////////////////////
  public int getSuperStepCounter() throws IOException {
    ensureState(JobState.RUNNING);
    return info.getSuperstepCounter();
  }

  public boolean isComplete() throws IOException {
    ensureState(JobState.RUNNING);
    return info.isComplete();
  }

  public boolean isSuccessful() throws IOException {
    ensureState(JobState.RUNNING);
    return info.isSuccessful();
  }

  public void killJob() throws IOException {
    ensureState(JobState.RUNNING);
    info.killJob();
  }

  public void killTask(TaskAttemptID taskId) throws IOException {
    ensureState(JobState.RUNNING);
    info.killTask(taskId, false);
  }

  public void failTask(TaskAttemptID taskId) throws IOException {
    ensureState(JobState.RUNNING);
    info.killTask(taskId, true);
  }

  public void submit() throws IOException, 
  		ClassNotFoundException,InterruptedException, Exception {
    ensureState(JobState.DEFINE);
    info = jobClient.submitJobInternal(this);
    state = JobState.RUNNING;
  }

  public boolean waitForCompletion(boolean verbose) throws IOException,
      InterruptedException, ClassNotFoundException, Exception {
    if (state == JobState.DEFINE) {
      submit();
    }
    if (verbose) {
      jobClient.monitorAndPrintJob(this, info, getNumSuperStep());
    } else {
      info.waitForCompletion();
    }
    return isSuccessful();
  }

  public void set(String name, String value) {
    conf.set(name, value);
  }

  public void setPriority(String PRIORITY) {
	  conf.set("bsp.job.priority", PRIORITY);
  }

  public String getPriority() {
	  return conf.get("bsp.job.priority", Constants.PRIORITY.NORMAL);
  }
  
  public void setNumBspTask(int tasks) {
    conf.setInt("bsp.peers.num", tasks);
  }
  
  public int getNumBspTask() {
	return conf.getInt("bsp.peers.num", 1);
  }
  
  public void setNumTotalVertices(int verNum) {
	    conf.setInt("total.vertex.num", verNum);
  }
	  
  public int getNumTotalVertices() {
	  return conf.getInt("total.vertex.num", 0);
  }

  public void setNumSuperStep(int superSteps) {
	conf.setInt("bsp.supersteps.num", superSteps);
  }

  public int getNumSuperStep() {
	return conf.getInt("bsp.supersteps.num", 1);
  }
  
  /**
   * If ture, HybridGraph stores graphInfo 
   * and initializes graphInfo when executing {@link BSPInterface}.update() 
   * to update vertex values. 
   * Only for Pull.
   * User-defined paramater, false as default.
   * @param flag
   */
  public void useGraphInfoInUpdate(boolean flag) {
	  conf.setBoolean("bsp.update.use.graphinfo", flag);
  }
  
  /**
   * Is graphInfo required when updating vertex values. 
   * Only for Pull.
   * Return false as default.
   * @return
   */
  public boolean isUseGraphInfoInUpdate() {
	  return conf.getBoolean("bsp.update.use.graphinfo", false);
  }
  
  /**
   * If ture, HybridGraph stores edges in the adjacency list, 
   * and initializes edges when executing {@link BSPInterface}.update() 
   * to update vertex values.
   * User-defined paramater, false as default.
   * @param flag
   */
  public void useAdjEdgeInUpdate(boolean flag) {
	  storeAdjEdge(true);
	  conf.setBoolean("bsp.update.use.edge.adj", flag);
  }
  
  /**
   * Are edges in the adjacency list required when updating 
   * vertex values.
   * Return false as default.
   * @return
   */
  public boolean isUseAdjEdgeInUpdate() {
	  return conf.getBoolean("bsp.update.use.edge.adj", false);
  }
  
  /**
   * If ture, HybridGraph stores edges in the adjacency list.
   * Auto-defined paramater:
   *   true, if "style" is Push or Hybrid in setBspStyle(int style), 
   *         or "flag" is true in useAdjEdgeInUpdate(boolean flag).
   * False as default.
   * @param flag
   */
  private void storeAdjEdge(boolean flag) {
	  conf.setBoolean("bsp.store.edge.adj", flag);
  }
  
  /**
   * Does HybridGraph store edges in the adjacency list?
   * Auto-defined paramater:
   *   true, if "style" is Push or Hybrid in setBspStyle(int style), 
   *         or "flag" is true in useAdjEdgeInUpdate(boolean flag).
   * Return false as default.
   * @return
   */
  public boolean isStoreAdjEdge() {
	  return conf.getBoolean("bsp.store.edge.adj", false);
  }
  
  /** Push, Pull, or Hybrid */
  public void setBspStyle(Constants.STYLE style) {
	  conf.set("bsp.impl.style", style.toString());
	  switch(style) {
	  case PULL:
		  setStartIteStyle(style);
		  //LOG.info("set default StartIteStyle=pull");
		  break;
	  case PUSH:
		  setStartIteStyle(style);
		  storeAdjEdge(true);
		  //LOG.info("set default StartIteStyle=push, storeAdjEdge=true");
		  break;
	  case Hybrid:
		  setStartIteStyle(Constants.STYLE.PULL);
		  storeAdjEdge(true);
		  //LOG.info("set default StartIteStyle=pull, storeAdjEdge=true");
		  break;
	  }
  }
  
  /** return STYLE.Pull as default */
  public Constants.STYLE getBspStyle() {
	  String style = 
		  conf.get("bsp.impl.style", Constants.STYLE.PULL.toString());
	  return Constants.STYLE.valueOf(style);
  }
  
  /** Push or Pull */
  public void setStartIteStyle(Constants.STYLE style) {
	  conf.set("bsp.ite.impl.style.start", style.toString());
  }
  
  /**
   * Simulate PUSH under Hybrid implementaion. 
   * Now only work for mini-superstep.
   * @param flag
   */
  public void setSimulatePUSH(int flag) {
	  if (flag == 1) {
		  conf.setBoolean("bsp.ite.impl.style.simulatePUSH", true);
		  setStartIteStyle(Constants.STYLE.PUSH);
	  } else {
		  conf.setBoolean("bsp.ite.impl.style.simulatePUSH", false);
	  }
  }
  
  /**
   * Simulate PUSH under Hybrid implementaion. 
   * Now only work for mini-superstep.
   */
  public boolean isSimulatePUSH() {
	  return conf.getBoolean("bsp.ite.impl.style.simulatePUSH", false);
  }
  
  /** Push or Pull, return Pull as default */
  public Constants.STYLE getStartIteStyle() {
	  String style = 
		  conf.get("bsp.ite.impl.style.start", Constants.STYLE.PULL.toString());
	  return Constants.STYLE.valueOf(style);
  }
  
  /** 
   * True, using {@link GraphDataServerDisk}, 
   * otherwise, using {@link GraphDataServerMem}.
   * 
   * @return boolean, true as default.
   */
  public boolean isGraphDataOnDisk() {
	  return conf.getBoolean("bsp.storage.graphdata.disk", true);
  }
  
  /**
   * True, using {@link GraphDataServerDisk}, 
   * otherwise, using {@link GraphDataServerMem}.
   * Now, {@link GraphDataServerMem} is only used for Pull.
   * @param _flag
   */
  public void setGraphDataOnDisk(boolean _flag) {
	  conf.setBoolean("bsp.storage.graphdata.disk", _flag);
	  if (!_flag) {
		  setBspStyle(Constants.STYLE.PULL); 
	  }
  }
  
  /**
   * Set the size of message sending buffer, per dstTask.
   * @param size
   */
  public void setMsgSendBufSize(int size) {
	  conf.setInt("bsp.message.send.buffer.size", size);
  }
  
  /** 
   * Return the size of message sending buffer (#), per dstTask.
   * 10000 as default.
   * @return
   */
  public int getMsgSendBufSize() {
	  return conf.getInt("bsp.message.send.buffer.size", 10000);
  }
  
  /**
   * Set the size of message receiving buffer (#), per dstTask (i.e., the receiver-side).
   * The buffer will be divided into (#tasks*#local_buckets) sub-buffers evenly.
   * @param size
   */
  public void setMsgRecBufSize(int size) {
	  conf.setInt("bsp.message.receive.buffer.size", size);
  }
  
  /**
   * Set the size of message receiving buffer (#), per dstTask (i.e., the receiver-side).
   * The buffer will be divided into (#tasks*#local_buckets) sub-buffers evenly.
   * The buffer is 10000 as default.
   * @return
   */
  public int getMsgRecBufSize() {
	  return conf.getInt("bsp.message.receive.buffer.size", 10000);
  }
  
  /**
   * Set the {@link MsgPack} size for style.Pull.
   * @param size
   */
  public void setMsgPackSize(int size) {
	  conf.setInt("bsp.message.pack.size", size);
  }
  
  /** Return 10000 as default */
  public int getMsgPackSize() {
	  return conf.getInt("bsp.message.pack.size", 10000);
  }
  
  public void setBoolean(String name, boolean value) {
	  conf.setBoolean(name, value);
  }
  
  public boolean getBoolean(String name, boolean defaultValue) {
	  return conf.getBoolean(name, defaultValue);
  }
  
  /**
   * Set the number of buckets per task.
   * @param num
   */
  public void setNumBucketsPerTask(int num) {
	  conf.setInt(Constants.Hash_Bucket_Num, num);
  }
  
  public int getNumBucketsPerTask() {
	  return conf.getInt(Constants.Hash_Bucket_Num, 
			  Constants.DEFAULT.Hash_Bucket_Num);
  }
  
  /**
   * Set the checkpoint policy.
   * @param p
   */
  public void setCheckPointPolicy(Constants.CheckPoint.Policy p) {
	  conf.set("bsp.checkpoint.policy", p.toString());
  }
  
  /**
   * Get the checkpoint policy. 
   * Constants.CheckPoint.Policy.None is returned if the policy is
   * not specified by users.
   * @return
   */
  public Constants.CheckPoint.Policy getCheckPointPolicy() {
	  String policy = 
		  conf.get("bsp.checkpoint.policy", 
				  Constants.CheckPoint.Policy.None.toString());
	  return Constants.CheckPoint.Policy.valueOf(policy);
  }
  
  /**
   * Set the checkpoint interval, 
   * Checkpointing is disabled if _interval is less than 0.
   * @param _interval
   */
  public void setCheckPointInterval(int _interval) {
	  conf.setInt("fault.tolerance.ckp.interval", _interval);
  }
  
  /**
   * Get the checkpoint interval. 
   * Return 0 by default, indicating that checkpointing is disabled.
   * @return
   */
  public int getCheckPointInterval() {
	  return conf.getInt("fault.tolerance.ckp.interval", 0);
  }
  
  /**
   * Assume that some tasks fail at the given superstep to simulate 
   * the failure in real scenrios. No failure happens if the parameter 
   * "failed" is less than zero.
   * @param _interval
   */
  public void setFailedIteration(int failed) {
	  conf.setInt("fault.tolerance.failed.location", failed);
  }
  
  /**
   * Get the failure location.
   * Return 0 by default, indicating that no failure happens.
   * @return
   */
  public int getFailedIteration() {
	  return conf.getInt("fault.tolerance.failed.location", 0);
  }
  
  /**
   * Set the number of failed tasks. The parameter is invalid 
   * if no failure happens. 
   * @param _interval
   */
  public void setNumOfFailedTasks(int failednum) {
	  if ((getFailedIteration()<=0) 
			  && (failednum>0)){
		  LOG.info("!!! # of failed tasks is automatically set to 0" 
				   + ", instead of given " + failednum);
		  failednum = 0;
	  } else if (getCheckPointPolicy() == 
		  Constants.CheckPoint.Policy.CompleteRecoveryDynCkp) {
		  if (failednum != getNumBspTask()) {
			  LOG.info("!!! # of failed tasks is automatically set to " 
					  + getNumBspTask() + ", instead of given " + failednum);
			  failednum = getNumBspTask();
		  }
	  }
	  
	  conf.setInt("fault.tolerance.failed.percentage", failednum);
  }
  
  /**
   * Get the number of failed tasks.
   * Return 0 by default, indicating that no failure happens.
   * @return
   */
  public int getNumOfFailedTasks() {
	  if (getFailedIteration() <= 0){
		  return 0;
	  } else {
		  return conf.getInt("fault.tolerance.failed.percentage", 0);
	  }
  }
  
  /**
   * Set the flag to indicate that whether or not the mini-superstep 
   * implementation works. In the mini-superstep implementation, a 
   * traditional superstep is further divided into two mini-supersteps: 
   * 1) PUSH
   *    mini-1: pull messages from local disk, update vertices;
   *    mini-2: load newly updated vertices and then push messages.
   * 2) PULL
   *    mini-1: pull messages from source vertices, update target vertices;
   *    mini-2: do nothing.
   *    
   * A mini-barrier is initiated to seperate the two consecutive mini-supersteps. 
   * We recommend users to enable the mini-based impl for algorithms with 
   * the message data volume suddenly changed, like Maximal Independent Sets and 
   * Maximal Matching. This is because we can make a switching decision at the 
   * mini-barrier to guide how to generate messages (i.e., immediately push them 
   * at the current superstep, or pull them at the next superstep).
   * 
   * @param flag
   */
  public void setMiniSuperStep(boolean flag) {
	  conf.setBoolean("bsp.superstep.mini", flag);
  }
  
  /**
   * Is the mini-superstep implementation enabled?
   * 
   * @return false as default
   */
  public boolean isMiniSuperStep() {
	  return conf.getBoolean("bsp.superstep.mini", false);
  }
}
