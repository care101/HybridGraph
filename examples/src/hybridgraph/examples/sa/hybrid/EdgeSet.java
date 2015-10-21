package hybridgraph.examples.sa.hybrid;

import java.io.EOFException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.Arrays;

import org.apache.hama.myhama.comm.CommRouteTable;

/**
 * Edge set of one vertex.
 * Used in Push or Hybrid.
 * 
 * @author root
 *
 */
public class EdgeSet {
	int num;
	private Integer[] edgeIds; //edge ids;
	
	public EdgeSet() {
		num = 0;
	}
	
	public EdgeSet(Integer[] _edgeIds) {
		setEdges(_edgeIds);
	}
	
	public void setEdges(Integer[] _edgeIds) {
		num = _edgeIds.length;
		edgeIds = _edgeIds;
	}
	
	public Integer[] getEdgeIds() {
		return edgeIds;
	}
	
	public int getEdgeNum() {
		return num;
	}
	
	public int getByteSize() {
		return (4 + 4*num);
	}
	
	public int getNumOfFragments(CommRouteTable commRT, boolean[][] hitFlag) {
		int dstTid = 0, dstBid = 0, count = 0;
    	for (int i = 0; i < hitFlag.length; i++) {
    		Arrays.fill(hitFlag[i], false);
    	}
    	for (int index = 0; index < this.num; index++) {
			dstTid = commRT.getDstParId(this.edgeIds[index]);
			dstBid = commRT.getDstBucId(dstTid, this.edgeIds[index]);
			if (!hitFlag[dstTid][dstBid]) {
				hitFlag[dstTid][dstBid] = true;
				count++;
			} //if
		} //loop all edges
    	return count;
	}
	
	/**
	   * Read fields, used to read data from local disk.
	   * 
	   * @param input input Input to be read.
	   * @throws EOFException
	   * @throws IOException
	   */
	public void readFields(MappedByteBuffer input) throws EOFException, IOException {
		num = input.getInt();
		edgeIds = new Integer[num];
		for (int i = 0; i < num; i++) {
			edgeIds[i] = input.getInt();
		}
	}
	
	/**
	   * Write fields, used to spill data onto local disk.
	   * 
	   * @param output Output to be written.
	   * @throws EOFException
	   * @throws IOException
	   */
	public void write(MappedByteBuffer output) throws EOFException, IOException {
		output.putInt(num);
		for (int i = 0; i < num; i++) {
			output.putInt(edgeIds[i]);
		}
	}
}
