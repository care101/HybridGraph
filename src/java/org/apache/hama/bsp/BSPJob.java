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
import org.apache.hama.Constants.Counters;


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
  
  public void setPriority(String PRIORITY) {
	conf.set("bsp.job.priority", PRIORITY);
  }

  public String getPriority() {
	return conf.get("bsp.job.priority", Constants.PRIORITY.NORMAL);
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
  public void setBspStyle(int style) {
	  conf.setInt("bsp.impl.style", style);
	  switch(style) {
	  case Constants.STYLE.Pull:
		  setStartIteStyle(style);
		  //LOG.info("set default StartIteStyle=pull");
		  break;
	  case Constants.STYLE.Push:
		  setStartIteStyle(style);
		  storeAdjEdge(true);
		  //LOG.info("set default StartIteStyle=push, storeAdjEdge=true");
		  break;
	  case Constants.STYLE.Hybrid:
		  setStartIteStyle(Constants.STYLE.Pull);
		  storeAdjEdge(true);
		  //LOG.info("set default StartIteStyle=pull, storeAdjEdge=true");
		  break;
	  }
  }
  
  /** return STYLE.Pull as default */
  public int getBspStyle() {
	  return conf.getInt("bsp.impl.style", Constants.STYLE.Pull);
  }
  
  /** Push or Pull */
  public void setStartIteStyle(int style) {
	  conf.setInt("bsp.ite.impl.style.start", style);
  }
  
  /** Push or Pull, return Pull as default */
  public int getStartIteStyle() {
	  return conf.getInt("bsp.ite.impl.style.start", Constants.STYLE.Pull);
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
		  setBspStyle(Constants.STYLE.Pull); 
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
  
  // Some counter
  public void setLocMinVerId(int vertexId) {
	  conf.setInt(Counters.LOCAL_MIN_VERTEX_ID, vertexId);
  }
  
  public int getLocMinVerId() {
	  return conf.getInt(Counters.LOCAL_MIN_VERTEX_ID, 0);
  }
  
  public void setLocHashBucNum(int number) {
	  conf.setInt(Counters.LOCAL_BUCKET_NUMBER, number);
  }
  
  public int getLocHashBucNum() {
	  return conf.getInt(Counters.LOCAL_BUCKET_NUMBER, 0);
  }
  
  public void setLocHashBucLen(int value) {
	  conf.setInt(Counters.LOCAL_BUCKET_LENGTH, value);
  }
  
  public int getLocHashBucLen() {
	  return conf.getInt(Counters.LOCAL_BUCKET_LENGTH, 0);
  }
}
