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
package org.apache.hama.ipc;

import java.io.IOException;

import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.GroomServerStatus;
import org.apache.hama.bsp.Directive;
import org.apache.hama.monitor.TaskInformation;
import org.apache.hama.myhama.comm.SuperStepReport;
import org.apache.hama.myhama.util.MiniCounters;

/**
 * A new protocol for GroomServers communicate with BSPMaster. This
 * protocol paired with WorkerProtocl, let GroomServers enrol with 
 * BSPMaster, so that BSPMaster can dispatch tasks to GroomServers.
 */
public interface MasterProtocol extends HamaRPCProtocolVersion {

  /**
   * A GroomServer register with its status to BSPMaster, which will update
   * GroomServers cache.
   *
   * @param status to be updated in cache.
   * @return true if successfully register with BSPMaster; false if fail.
   */
  boolean register(GroomServerStatus status) throws IOException;

  /**
   * A GroomServer (periodically) reports task statuses back to the BSPMaster.
   * @param directive 
   */
  boolean report(Directive directive) throws IOException;

  public String getSystemDir();
  
  /**
   * Ensure that all tasks have been launched successfully. Each task 
   * reports the local minimum vertex id so that {@link JobInProgress} 
   * can compute the distribution of vertex ids among tasks, and then 
   * get the global route table. This works only for the current Range 
   * graph partitioning method. You can use this function together with 
   * {@link JobInProgress}.setRouteTable().
   * @param jobId
   * @param local
   * @return
   */
  public void buildRouteTable(BSPJobID jobId, TaskInformation local);
  
  /**
   * Ensure that all tasks have loaded specific input data successfully. 
   * Some information about input data will be reported via the parameter
   * {@link TaskInformation}. You can use this function together with 
   * {@link CommunicationServerProtocol}.setRegisterInfo().
   * @param jobId
   * @param taskInfo
   */
  public void registerTask(BSPJobID jobId, TaskInformation taskInfo);
  
  /**
   * Ensure that all tasks have completed the preparation work before beginning 
   * a new superstep. You can use this function together with 
   * {@link CommunicationServerProtocol}.setRegisterInfo().
   * @param jobId
   * @param parId
   */
  public void beginSuperStep(BSPJobID jobId, int parId);
  
  /**
   * Ensure that all tasks have finished computation workloads 
   * at one superstep. Local information maintained in {@link SuperStepReport} is 
   * reported. You can use this function together with 
   * {@link CommunicationServerProtocol}.setNextSuperStepCommand().
   * @author 
   * @param jobId
   * @param parId
   * @param SuperStepReport ssr
   */
  public void finishSuperStep(BSPJobID jobId, int parId, SuperStepReport ssr);
  
  /**
   * Ensure that all tasks have dumped local computation results 
   * onto a distributed file system, like HDFS.
   * @param jobId
   * @param parId
   * @param dumpNum
   */
  public void dumpResult(BSPJobID jobId, int parId, int dumpNum);
  
  /**
   * Ensure that all tasks have completed some thing, like archiving/loading a 
   * checkpoint. You can use this function together with 
   * {@link CommunicationServerProtocol}.quit().
   * @param jobId
   * @param parId
   */
  public void sync(BSPJobID jobId, int parId);
  
  /**
   * Initiate a mini-barrier to divide a traditional superstep into two 
   * mini-supersteps. Runtime statistics are collected at the first mini-superstep. 
   * Accordingly, at the mini-barrier, the system smartly computes that which model 
   * (PUSH messages now or PULL messages at the next superstep) is efficient.
   *  
   * @param jobId
   * @param parId
   * @param minicounters
   */
  public void miniSync(BSPJobID jobId, int parId, MiniCounters minicounters);
}
