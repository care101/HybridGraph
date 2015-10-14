package cn.edu.neu.termite.examples.cluster.pull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.HashSet;

import cn.edu.neu.termite.examples.cluster.pull.SCUserTool.SCGraphRecord;

/**
 * This class represents a semi-cluster.
 */
public class SemiCluster implements Comparable<SemiCluster> {
  private int maxCapacity; //will not be computed in byte size.
	
  /** List of vertices  */
  private HashSet<Integer> vertices;
  /** Score of current semi cluster. */
  private double score;
  /** Inner Score. */
  private double innerScore;
  /** Boundary Score. */
  private double boundaryScore;

  /** Constructor: Create a new empty Cluster. */
  public SemiCluster() {
    vertices = new HashSet<Integer>();
    score = 1d;
    innerScore = 0d;
    boundaryScore = 0d;
  }
  
  /** Constructor: Create a new empty Cluster. */
  public SemiCluster(int _maxCapacity) {
	maxCapacity = _maxCapacity;
    vertices = new HashSet<Integer>();
    score = 1d;
    innerScore = 0d;
    boundaryScore = 0d;
  }

  /**
   * Constructor: Initialize a new Cluster.
   *
   * @param cluster cluster object to initialize the new object
   */
  public SemiCluster(final SemiCluster cluster) {
    vertices = new HashSet<Integer>();
    vertices.addAll(cluster.vertices);
    score = cluster.score;
    innerScore = cluster.innerScore;
    boundaryScore = cluster.boundaryScore;
  }
  
  public void setMaxCapacity(int _maxCapacity) {
	maxCapacity = _maxCapacity;
  }
  
  public HashSet<Integer> getVertices() {
	  return vertices;
  }

  /**
   * Adds a vertex to the cluster.
   * 
   * Every time a vertex is added we also update the inner and boundary score.
   * Because vertices are only added to a semi-cluster, we can save the inner
   * and boundary scores and update them incrementally.
   * 
   * Otherwise, in order to re-compute it from scratch we would need every
   * vertex to send a friends-of-friends list, which is very expensive.
   * 
   * @param vertex The new vertex to be added into the cluster
   * @param scoreFactor Boundary Edge Score Factor
   */
  public final void addVertex(final SCGraphRecord vertex, final double scoreFactor) {
    int vertexId = vertex.getVerId();

    if (vertices.add(new Integer(vertexId))) {
      if (size() == 1) {
        for (double w: vertex.getGraphInfo().getEdgeWs()) {
          boundaryScore += w;
        }
        score = 0.0;
      } else {
    	Integer[] eidset = vertex.getGraphInfo().getEdgeIds();
    	Double[] wset = vertex.getGraphInfo().getEdgeWs();
        for (int i = 0; i < vertex.getEdgeNum(); i++) {
          if (vertices.contains(eidset[i])) {
            innerScore += wset[i].doubleValue();
            boundaryScore -= wset[i].doubleValue();
          } else {
            boundaryScore += wset[i].doubleValue();
          }
        }
        score =  (innerScore-scoreFactor*boundaryScore)/(size()*(size()-1)/2);
      }
    }
  }

  /**
   * Returns score of this semi-cluster.
   * 
   * @return
   */
  public final double score() {
	  return this.score;
  }
  
  /**
   * Returns size of semi cluster list.
   *
   * @return Number of vertices in the list of this semi-cluster
   */
  public final int size() {
    return vertices.size();
  }

  /** 
   * Two semi clusters are the same when:
   * (i) they have the same number of vertices,
   * (ii) all their vertices are the same
   *
   * @param other Cluster to be compared with current cluster
   *
   * @return 0 if two clusters are the same
   */
  @Override
  public final int compareTo(final SemiCluster other) {
    if (other == null) {
      return 1;
    }
    if (this.size() < other.size()) {
      return -1;
    }
    if (this.size() > other.size()) {
      return 1;
    }
    if (other.vertices.containsAll(vertices)) {
      return 0;
    }
    return -1;
  }

  /**
   * hashCode method.
   *
   * @return result HashCode result
   */
  @Override
  public final int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result;
    if (vertices != null) {
      result += vertices.hashCode();
    }
    return result;
  }

  /**
   * Equals method.
   *
   * @param obj Object to compare if it is equal with.
   * @return boolean result.
   */
  @Override
  public final boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    SemiCluster other = (SemiCluster) obj;
    if (vertices == null) {
      if (other.vertices != null) {
        return false;
      }
    } else if (!vertices.equals(other.vertices)) {
      return false;
    }
    return true;
  }

  /**
   * Convert object to string object.
   *
   * @return string object
   */
  @Override
  public final String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("[ ");
    for (Integer vid: this.vertices) {
      builder.append(vid.toString());
      builder.append(" ");
    }
    builder.append(" | " + score + ", " + innerScore + ", "
        + boundaryScore + " ]");
    return builder.toString();
  }
  
  /**
   * Read fields, used to read data from local disk.
   * 
   * @param input input Input to be read.
   * @throws EOFException
   * @throws IOException
   */
  public void readFields(MappedByteBuffer input) throws EOFException, IOException {
	vertices = new HashSet<Integer>();
	int size = input.getInt();
    for (int i = 0; i < size; i++) {
      vertices.add(new Integer(input.getInt()));
    }
    score = input.getDouble();
    innerScore = input.getDouble();
    boundaryScore = input.getDouble();
  }
  
  /**
   * Write fields, used to spill data onto local disk.
   * 
   * @param output Output to be written.
   * @throws EOFException
   * @throws IOException
   */
  public void write(MappedByteBuffer output) throws EOFException, IOException {
	output.putInt(size());
	for (int vid: vertices) {
	  output.putInt(vid);
	}
	output.putDouble(score);
	output.putDouble(innerScore);
	output.putDouble(boundaryScore);
  }
  
  /**
   * Read data, used to read data from communication byte stream (RPC).
   * 
   * @param in
   * @throws IOException
   */
  public void readRPC(DataInputStream in) throws IOException {
	vertices = new HashSet<Integer>();
	int size = in.readInt();
	for (int i = 0; i < size; i++) {
	  vertices.add(new Integer(in.readInt()));
	}
	score = in.readDouble();
	innerScore = in.readDouble();
	boundaryScore = in.readDouble();
  }
  
  /**
   * Write data, used to write data into communication byte stream (RPC)
   * 
   * @param out
   * @throws IOException
   */
  public void writeRPC(DataOutputStream out) throws IOException {
	out.writeInt(size());
	for (int vid: vertices) {
	  out.writeInt(vid);
	}
	out.writeDouble(score);
	out.writeDouble(innerScore);
	out.writeDouble(boundaryScore);
  }
  
  /**
   * Return used byte size of this semi-cluster.
   * Including size(), ids of vertices, and three scores.
   * 
   * @return
   */
  public int getUsedByte() {
	return (4 + 4*vertices.size() + (3*8));
  }
  
  /**
   * Return maximum byte size of this semi-cluster.
   * Including size(), ids of vertices, and three scores.
   * 
   * @return
   */
  public int getMaxByte() {
	return (4 + 4*maxCapacity + (3*8));
  }
}