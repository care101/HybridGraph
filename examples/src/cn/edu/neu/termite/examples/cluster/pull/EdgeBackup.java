package cn.edu.neu.termite.examples.cluster.pull;

import java.io.EOFException;
import java.io.IOException;
import java.nio.MappedByteBuffer;

/**
 * Edge set of one vertex.
 * This is a backup, which is used to update the vertex value.
 * 
 * @author root
 *
 */
public class EdgeBackup {
	private int num = 0; //edge number.
	private Integer[] edgeIds; //edge ids;
	private Double[] edgews; //edge weights;
	
	public EdgeBackup() {
		
	}
	
	public void setEdges(int _num, Integer[] _edgeIds, Double[] _edgews) {
		num = _num;
		edgeIds = _edgeIds;
		edgews = _edgews;
	}
	
	public Integer[] getEdgeIds() {
		return edgeIds;
	}
	
	public Double[] getEdgeWs() {
		return edgews;
	}
	
	public int getEdgeNum() {
		return num;
	}
	
	public int getByteSize() {
		return (4+num*(4+8));
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
		edgews = new Double[num];
		for (int i = 0; i < num; i++) {
			edgeIds[i] = input.getInt();
			edgews[i] = input.getDouble();
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
			output.putDouble(edgews[i]);
		}
	}
}
