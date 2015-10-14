package cn.edu.neu.termite.examples.cluster.pull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class MsgBundle {
	private ArrayList<SemiClusterSet> msgs;
	
	public MsgBundle() {
		this.msgs = new ArrayList<SemiClusterSet>();
	}
	
	public void add(SemiClusterSet _scList) {
		this.msgs.add(_scList);
	}
	
	public void combine(ArrayList<SemiClusterSet> _scLists) {
		this.msgs.addAll(_scLists);
	}
	
	public ArrayList<SemiClusterSet> getAll() {
		return this.msgs;
	}
	
    public void write(DataOutputStream out) throws IOException {
    	out.writeInt(this.msgs.size());
    	for (SemiClusterSet scList: this.msgs) {
    		scList.writeRPC(out);
    	}
    }
    
    public void read(DataInputStream in) throws IOException {
    	int size = in.readInt();
    	this.msgs = new ArrayList<SemiClusterSet>(size);
    	for (int i = 0; i < size; i++) {
    		SemiClusterSet scList = new SemiClusterSet();
    		scList.readRPC(in);
    		this.msgs.add(scList);
    	}
    }
}
