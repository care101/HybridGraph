package hybridgraph.examples.mm;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class BipartiteMsgBundle {
	private ArrayList<Integer> labels;
	
	public BipartiteMsgBundle() {
		this.labels = new ArrayList<Integer>();
	}
	
	/** Just for test **/
	public void set(ArrayList<Integer> newLabels) {
		this.labels = newLabels;
	}
	
	public void add(int newLabel) {
		this.labels.add(newLabel);
	}
	
	public void combine(ArrayList<Integer> newLabels) {
		this.labels.addAll(newLabels);
	}
	
	public ArrayList<Integer> get() {
		return this.labels;
	}
	
	public int getByteSize() {
		return (4 + 4*this.labels.size());
	}
	
	public void write(ByteBuffer out) throws IOException {
		out.putInt(this.labels.size());
		for (int aId: this.labels) {
    		out.putInt(aId);
    	}
	}
	
	public void read(ByteBuffer in) throws IOException {
		int size = in.getInt();
    	this.labels = new ArrayList<Integer>(size);
    	for (int i = 0; i < size; i++) {
    		this.labels.add(in.getInt());
    	}
	}
	
    public void write(DataOutputStream out) throws IOException {
    	out.writeInt(this.labels.size());
    	for (int label: this.labels) {
    		out.writeInt(label);
    	}
    }
    
    public void read(DataInputStream in) throws IOException {
    	int size = in.readInt();
    	this.labels = new ArrayList<Integer>(size);
    	for (int i = 0; i < size; i++) {
    		this.labels.add(in.readInt());
    	}
    }
}
