package cn.edu.neu.termite.examples.sssp.push;

import java.io.EOFException;
import java.io.IOException;
import java.nio.MappedByteBuffer;

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
