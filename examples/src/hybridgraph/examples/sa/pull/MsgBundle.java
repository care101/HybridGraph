package hybridgraph.examples.sa.pull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class MsgBundle {
	private ArrayList<Integer> aIds;
	
	public MsgBundle() {
		this.aIds = new ArrayList<Integer>();
	}
	
	public void add(int _aId) {
		this.aIds.add(_aId);
	}
	
	public void combine(ArrayList<Integer> _aIds) {
		this.aIds.addAll(_aIds);
	}
	
	public ArrayList<Integer> getAll() {
		return this.aIds;
	}
	
	public int getByteSize() {
		return (4 + 4*this.aIds.size());
	}
	
    public void write(DataOutputStream out) throws IOException {
    	out.writeInt(this.aIds.size());
    	for (int aId: this.aIds) {
    		out.writeInt(aId);
    	}
    }
    
    public void read(DataInputStream in) throws IOException {
    	int size = in.readInt();
    	this.aIds = new ArrayList<Integer>(size);
    	for (int i = 0; i < size; i++) {
    		this.aIds.add(in.readInt());
    	}
    }
    
    @Override
    public String toString() {
    	StringBuffer sb = new StringBuffer();
    	for (int id: this.aIds) {
    		sb.append(Integer.toString(id));
    		sb.append(" ");
    	}
    	return sb.toString();
    }
}
