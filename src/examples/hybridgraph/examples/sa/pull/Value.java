package hybridgraph.examples.sa.pull;

import java.io.IOException;
import java.nio.MappedByteBuffer;

public class Value {
	private int aId; //advertisement id
	private int aNum; //the number of aId of its in-neighbors
	
	public Value() {
		
	}
	
	public Value(int _aId, int _aNum) {
		set(_aId, _aNum);
	}
	
	/**
	 * Set new value of aId & aNum.
	 * @param _aId advertisement id (int)
	 * @param _aNum #of this id (int)
	 */
	public void set(int _aId, int _aNum) {
		this.aId = _aId;
		this.aNum = _aNum;
	}
	
	public int getAdverId() {
		return this.aId;
	}
	
	public int getAdverIdNum() {
		return this.aNum;
	}
	
	public int getByteSize() {
		return 8;
	}
	
    public void write(MappedByteBuffer out) throws IOException {
    	out.putInt(this.aId);
    	out.putInt(this.aNum);
    }
    
    public void read(MappedByteBuffer in) throws IOException {
    	this.aId = in.getInt();
    	this.aNum = in.getInt();
    }
    
    @Override
    public String toString() {
    	StringBuffer sb = new StringBuffer();
    	sb.append(Integer.toString(this.aId));
    	sb.append(" ");
    	sb.append(Integer.toString(this.aNum));
    	return sb.toString();
    }
}