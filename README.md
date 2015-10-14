#HybridGraph
HybridGraph is a Pregel-like system which hybriding Pulling/Pushing for I/O-Efficient distributed and iterative graph computing.

##1. Introduction
Billion-scale graphs are rapidly growing in size in many applications. That has been driving much of the research on enhancing the performance of distributed graph systems, in terms of graph partitioning, network communication, and the convergence. `HybridGraph` focuses on performing graph analysis on a cluster I/O-efficiently, since the memory resource of a given computer cluster can be easily exhausted. 
Specially, HybridGraph employs a `hybrid` solution to support switching between push and pull adaptively to obtain optimal performance in different scenarios. 

Features of HybridGraph:
* ___Block-centric pull mechanism (b-pull):___ First, the I/O accesses are shifted from the receiver-sides where messages are written/read by push to the sender-sides where graph data are read by pull. Second, the block-centric technique greatly optimizes the I/O-efficiency of reading vertices.
* ___Hybrid engine:___ A seamless switching mechanism and a prominent performance prediction method to guarantee the efficiency when switching between push and b-pull.

The HybridGraph project started in 2011 on top of Apache Hama 0.2.0-incubating. HybridGraph is a Java framework, which runs in the cloud.
