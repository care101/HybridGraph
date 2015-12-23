package hybridgraph.examples.lpa.pull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class MsgBundle {
	private ArrayList<Integer> labels;
	
	public MsgBundle() {
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
	
	public ArrayList<Integer> getAll() {
		return this.labels;
	}
	
	public int getByteSize() {
		return (4 + 4*this.labels.size());
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
