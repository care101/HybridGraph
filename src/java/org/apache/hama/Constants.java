/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hama;

/**
 * Some constants used in the Hama.
 */
public interface Constants {
 
  public  static final String GROOM_RPC_HOST = "bsp.groom.rpc.hostname";

  public static final String DEFAULT_GROOM_RPC_HOST = "0.0.0.0";

  public static final String GROOM_RPC_PORT = "bsp.groom.rpc.port";

  /** Default port region rpc server listens on. */
  public static final int DEFAULT_GROOM_RPC_PORT = 50000;
  

  ///////////////////////////////////////
  // Constants for BSP Package
  ///////////////////////////////////////
  /** default host address */
  public  static final String PEER_HOST = "bsp.peer.hostname";
  /** default host address */
  public static final String DEFAULT_PEER_HOST = "0.0.0.0";

  public static final String PEER_PORT = "bsp.peer.port";
  /** Default port region server listens on. */
  public static final int DEFAULT_PEER_PORT = 61000;

  public static final String PEER_ID = "bsp.peer.id";
  
  /** Parameter name for what groom server implementation to use. */
  public static final String GROOM_SERVER_IMPL= "hama.groomserver.impl";
  
  /** When we encode strings, we always specify UTF8 encoding */
  static final String UTF8_ENCODING = "UTF-8";
  
  /** Cluster is in distributed mode or not */
  static final String CLUSTER_DISTRIBUTED = "hama.cluster.distributed";
  /** Cluster is fully-distributed */
  static final String CLUSTER_IS_DISTRIBUTED = "true";
  
  //change in version-0.2.4 new bspPeer and priotiry level,partition_mode used to in scheduler job
  public static final String MAX_TASKS="bsp.task.max";
  
  
  public static final String USER_JOB_INPUT_FORMAT_CLASS = "job.input.format.class";
  public static final String USER_JOB_OUTPUT_FORMAT_CLASS = "job.output.format.class";
  public static final String USER_JOB_TOOL_CLASS = "job.user.tool.class";
  public static final String USER_JOB_INPUT_DIR = "job.input.dir";
  public static final String USER_JOB_OUTPUT_DIR = "job.output.dir";
  
  public static final String Graph_Dir = "graph";
  public static final String Graph_Ver_Dir = "vertex";
  public static final String Graph_Edge_Dir = "edge";
  
  public static final String Msg_Dir = "message";
  public static final String Msg_Arch_Dir = "archived";
  public static final String Msg_Rec_Dir = "received";
  
  public static final String Hash_Bucket_Num = "hash.bucket.num";
  
  public static class DEFAULT {
	  public static final int Load_Edge_Thread_Num = 20;
	  public static final int Hash_Bucket_Num = 5;
	  public static final int Pull_Msg_Thread_Num = 20;
  }
  
  public static class PRIORITY {
	  public static final String LOWER="5";
	  public static final String LOW="4";
	  public static final String NORMAL="3";
	  public static final String HIGH="2";
	  public static final String HIGHER="1";
  }
  
  public static class HardwareInfo {
	  public static final String RD_Read_ThroughPut = "random.read.throughput";
	  public static final String RD_Write_ThroughPut = "random.write.throughput"; 
	  public static final String Seq_Read_ThroughPut = "sequential.read.throughput";
	  public static final String Seq_Write_ThroughPut = "sequential.write.throughput";
	  public static final String Network_ThroughPut = "network.throughput";
	  
	  /** Default value (local cluster), KB/s */
	  public static final float Def_RD_Read_ThroughPut = 1205;
	  /** Default value (local cluster), KB/s */
	  public static final float Def_RD_Write_ThroughPut = 1210;
	  public static final float Def_Seq_Read_ThroughPut = 2415;
	  public static final float Def_Seq_Write_ThroughPut = 2414;
	  /** Default value (local cluster), MB/s */
	  public static final float Def_Network_ThroughPut = 112;
  }
  
  /**
   * Define update rules for vertices in one VBlock.
   * @author root
   *
   */
  public static enum VBlockUpdateRule {
	  /**
	   * Read all vertices but only update those with 
	   * newly received messages or true active-flag.
	   */
	  UPDATE,
	  /**
	   * Completely skip all vertices. No vertex is read.
	   */
	  SKIP,
	  /**
	   * All vertices will be read if any of them receives messages. 
	   * Further, only vertices with messages or true active-flag
	   * will be updated (e.g., shortest path).
	   */
	  MSG_DEPEND, 
	  /**
	   * All vertices will be read if any of them receives messages 
	   * or is active. Further, only vertices with messages or true 
	   * active-flag will be updated (e.g., maximal independent sets).
	   */
	  MSG_ACTIVE_DEPENDED
  }
  
  public static enum BufferStatus {
	  NORMAL, OVERFLOW
  }
  
  public static enum CommandType {
	  /** start a normal iteration */
	  START, 
	  /** archive a checkpoint */
	  ARCHIVE, 
	  /** recover failures */
	  RECOVER, 
	  /** re-run the iteration where failures happened */
	  REDO, 
	  /** terminate computations */
	  STOP
  }
  
  /**
   * Candidate computation model(style).
   */
  public static enum STYLE {
	  /** source vertices produce messages voluntarily */
	  PUSH,
	  /** messages are produced on demand of destination vertices (block-centric) */
	  PULL,
	  /** switch between PUSH and PULL during iterations if necessary */
	  Hybrid
  }
  
  // Other constants
  /**
   * Variables related to fault-tolerance.
   */
  public static class CheckPoint {
	  public static final String JobDir = "bsp.checkpoint.job.dir";
	  public static final String TaskFile = "bsp.checkpoint.task.file";
	  public static enum Policy {
		  /** 
		   * Nothing is archived. Upon any failures, a job immediately fails 
		   * and needs to be manually re-submitted. This is the default policy.
		   * */
		  None,
		  /** 
		   * Vertex values and metadata are periodically archived onto HDFS as 
		   * a checkpoint. Hence, failures can be recovered by rolling back 
		   * computations on all tasks to the most recent available checkpoint. 
		   * Currently, this is simulated by setting all tasks failed (ignoring 
		   * re-scheduling overheads). Note that the checkpointing interval is 
		   * dynamically computed based on user-specified parameter value and 
		   * the specific algorithm runtime feature, in order to balance the 
		   * archiving overhead and recovery efficiency. For more details, 
		   * please refer to our previous work: 
		   * https://link.springer.com/article/10.1007%2Fs10619-017-7192-2.
		   * */
		  CompleteRecoveryDynCkp,
		  /**
		   * Recovery is confined to failed tasks only since outgoing messages 
		   * for these tasks have been logged onto local disks on surviving tasks. 
		   * Dynamic checkpoint is still required to avoid recomputing from 
		   * scratch for failed tasks. 
		   */
		  ConfinedRecoveryLogMsg,
		  /**
		   * Confined recovery is performed but no message is logged. Instead, 
		   * vertex values are logged to reduce the I/O costs in failure-free 
		   * execution. Outgoing messages can be re-generated based on logged 
		   * vertices when necessary.
		   */
		  ConfinedRecoveryLogVert
	  }
  }

  /**
   * An empty instance.
   */
  static final byte [] EMPTY_BYTE_ARRAY = new byte [0];
  public static final String KV_SPLIT_FLAG = "\t";
}
