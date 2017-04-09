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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hama.myhama.util.TaskReportContainer;

/**
 * TaskTracker.
 */
public interface BSPTaskTrackerInterface 
	extends BSPRPCProtocolVersion, Closeable {
	/**
	 * Report progress.
	 * @param jobId
	 * @param taskId
	 * @param taskReportContainer
	 */
	public void ping(BSPJobID jobId, TaskAttemptID taskId, 
		  TaskReportContainer taskReportContainer);
	
	/**
	 * Report exceptions to the tasktracker {@link GroomServer}, and 
	 * then {@link BSPMaster}. If exception is null, that means this 
	 * task is done successfully.
	 * @param jobId
	 * @param taskId
	 */
	public void runtimeError(BSPJobID jobId, TaskAttemptID taskId);
  
	/**
	 * Get the number of tasks that run on the same machine and 
	 * belong to the same job.
	 * @return
	 */
	public int getLocalTaskNumber(BSPJobID jobId);
	
	/**
	 * Get the directory of local files managed by this job.
	 * @param jobId
	 * @return
	 */
	public String getLocalJobDir(BSPJobID jobId) throws IOException;
	
	/**
	 * Get the directory of local files managed by this task.
	 * @param jobId
	 * @param taskId
	 * @return
	 */
	public String getLocalTaskDir(BSPJobID jobId, TaskAttemptID taskId) 
		throws IOException;
	
	public int getPort();
}
