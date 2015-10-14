package cn.edu.neu.termite.examples.cluster.pull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A set of semi-clusters that is writable. We use a TreeSet since we want
 * the ability to sort semi-clusters.
 */

public class SemiClusterSet extends TreeSet<SemiCluster>
  implements Comparable<SemiClusterSet> {
	public static final Log LOG = LogFactory.getLog(SemiClusterSet.class);
  
  /**
   * The three variables will not be spilled onto disk, 
   * i.e., they are not computed in byte size.
   */
  private static final long serialVersionUID = 1L;
  private int maxClusters;
  private int maxCapacity;

/**
   * Default Constructor. We ensure that this list always sorts semi-clusters
   * according to the comparator.
   */
  public SemiClusterSet(int _maxClusters, int _maxCapacity) {
    super(new ClusterScoreComparator());
    maxClusters = _maxClusters;
    maxCapacity = _maxCapacity;
  }
  
  public SemiClusterSet() {
	  super(new ClusterScoreComparator());
  }
  
  public void setMetadata(int _maxClusters, int _maxCapacity) {
	maxClusters = _maxClusters;
	maxCapacity = _maxCapacity;
	for(SemiCluster sc: this) {
      sc.setMaxCapacity(maxCapacity);
	}
  }
  
  /**
   * Return the used byte size of this set.
   * 
   * @return
   */
  public int getUsedByte() {
	int counter = 4; //local size(), i.e., the real number of semi-clusters;
	for(SemiCluster sc: this) {
		counter += sc.getUsedByte();
	}
	return counter;
  }
  
  /**
   * Return the maximum byte size of this set.
   * 
   * @return
   */
  public int getMaxByte() {
	SemiCluster sc = new SemiCluster(maxCapacity);
	return (4+maxClusters*sc.getMaxByte());
  }

  /**
   * CompareTo method.
   * Two lists of semi clusters are the same when:
   * (i) they have the same number of clusters,
   * (ii) all their clusters are the same, i.e. contain the same vertices
   * For each cluster, check if it exists in the other list of semi clusters
   *
   * @param other A list with clusters to be compared with the current list
   *
   * @return return 0 if two lists are the same
   */
  public final int compareTo(final SemiClusterSet other) {
    if (this.size() < other.size()) {
      return -1;
    }
    if (this.size() > other.size()) {
      return 1;
    }
    Iterator<SemiCluster> iterator1 = this.iterator();
    Iterator<SemiCluster> iterator2 = other.iterator();
    while (iterator1.hasNext()) {
      if (iterator1.next().compareTo(iterator2.next()) != 0) {
        return -1;
      }
    }
    return 0;
  }

  /**
   * Implements the readFields method.
   *
   * @param input Input to be read.
   * @throws IOException for IO.
   */
  public void readFields(MappedByteBuffer input, int id) throws EOFException, IOException {
	//if (id < 10)
	//LOG.info("read_1:" + input.position());
    int size = input.getInt();
    for (int i = 0; i < size; i++) {
      SemiCluster c = new SemiCluster(maxCapacity);
      c.readFields(input);
      add(c);
    }
    
    //if (id < 10)
    //LOG.info("read_2:" + input.position() + " [max=" + getMaxByte() + ", used=" + getUsedByte() + "]");
    int newPosition = getMaxByte() - getUsedByte() + input.position();
    input.position(newPosition); //skip un-used space.
    //if (id < 10)
    //LOG.info("read_3:" + input.position());
  }

  /**
   * Implements the write method.
   *
   * @param output Output to be written.
   * @throws IOException for IO.
   */
  public void write(MappedByteBuffer output, int id) throws EOFException, IOException {
	//if (id < 10)
	//LOG.info("write_1:" + output.position());
    output.putInt(size());
    for (SemiCluster c : this) {
      c.write(output);
    }
    
    //if (id < 10)
	//LOG.info("write_2:" + output.position() + " [max=" + getMaxByte() + ", used=" + getUsedByte() + "]");
    int newPosition = getMaxByte() - getUsedByte() + output.position();
    output.position(newPosition); //skip un-used space.
    //if (id < 10)
    //LOG.info("write_3:" + output.position());
  }
  
  /**
   * Read data, used to read data from communication byte stream (RPC).
   * 
   * @param in
   * @throws IOException
   */
  public void readRPC(DataInputStream in) throws IOException {
	int size = in.readInt();
	for (int i = 0; i < size; i++) {
	  SemiCluster c = new SemiCluster();
	  c.readRPC(in);
	  add(c);
	}
  }
  
  /**
   * Write data, used to write data into communication byte stream (RPC)
   * 
   * @param out
   * @throws IOException
   */
  public void writeRPC(DataOutputStream out) throws IOException {
	out.writeInt(size());
	for (SemiCluster c : this) {
	  c.writeRPC(out);
	}
  }

  /**
   * Returns a string representation of the list.
   *
   * @return String object.
   */
  @Override
  public final String toString() {
    StringBuilder builder = new StringBuilder();
    for (SemiCluster v: this) {
      builder.append(v.toString() + " ");
    }
    return builder.toString();
  }
}