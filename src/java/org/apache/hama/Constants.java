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
  public static final String Graph_Msg_Dir = "message";
  
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
  
  public static class STYLE {
	  public static final int Push = 1;
	  public static final int Pull = 2;
	  public static final int Hybrid = 3;
  }
  
  public static class Counters {
	  public static final String TOTAL_GRAPH_RECORD = "total.graph.record.number";
	  public static final String LOCAL_GRAPH_RECORD = "local.graph.record.number";
	  
	  public static final String TOTAL_MIN_VERTEX_ID = "total.min.vertex.id";
	  public static final String LOCAL_MIN_VERTEX_ID = "local.min.vertex.id";
	  
	  public static final String LOCAL_BUCKET_NUMBER = "local.hash.bucket.number";
	  public static final String LOCAL_BUCKET_LENGTH = "local.hash.bucket.length";
  }
  
  public static class HardwareInfo {
	  public static final String RD_Read_Speed = "rand.read.speed";
	  public static final String RD_Write_Speed = "rand.write.speed"; 
	  public static final String Network_Speed = "network.speed";
	  
	  /** Default value (local cluster), KB/s */
	  public static final float Def_RD_Read_Speed = 1077;
	  /** Default value (local cluster), KB/s */
	  public static final float Def_RD_Write_Speed = 1071;
	  /** Default value (local cluster), MB/s */
	  public static final float Def_Network_Speed = 112;
  }
  
  public static enum Opinion {
	  YES, NO, MSG_DEPEND
  }
  
  public static enum BufferStatus {
	  NORMAL, OVERFLOW
  }
  
  public static enum CommandType {
	  START, CHECKPOINT, RECOVERY, STOP
  }

  // Other constants

  /**
   * An empty instance.
   */
  static final byte [] EMPTY_BYTE_ARRAY = new byte [0];
  public static final String KV_SPLIT_FLAG = "\t";
}
