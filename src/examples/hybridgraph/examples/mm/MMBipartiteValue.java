package hybridgraph.examples.mm;

import java.io.IOException;
import java.nio.ByteBuffer;

public class MMBipartiteValue {
	/** matched(1) or not(0) */
	private byte matched;
	/**
	 * guide operations in BSP.getMessages():
	 * -1 => broadcast v.id along outgoing edges;
	 * !=(-1) => send v.id to the vertex u with u.id==valule
	 */
	private int value;
	
	public MMBipartiteValue() {
		
	}
	
	/**
	 * Construct MMBipartiteValue.
	 * @param _matched Matched(1) or not(0)
	 * @param _value -1=>broadcast v.id along outgoing edges; 
	 *               not(-1)=>send v.id to the vertex u with u.id==_valule
	 */
	public MMBipartiteValue(byte _matched, int _value) {
		matched = _matched;
		value = _value;
	}
	
	/**
	 * Update MMBipartiteValue.
	 * @param _matched Matched(1) or not(0)
	 * @param _value -1=>broadcast v.id along outgoing edges; 
	 *               not(-1)=>send v.id to the vertex u with u.id==_valule
	 */
	public void set(byte _matched, int _value) {
		matched = _matched;
		value = _value;
	}
	
	/**
	 * Update the matched-flag
	 * @param _matched Matched(1) or not(0)
	 */
	public void setMatchedFlag(byte _matched) {
		matched = _matched;
	}
	
	/**
	 * Update the value-flag
	 * @param _value -1=>broadcast v.id along outgoing edges; 
	 *               not(-1)=>send v.id to the vertex u with u.id==_valule
	 */
	public void setValueFlag(int _value) {
		value = _value;
	}
	
	public boolean isMatched() {
		return (matched==1);
	}
	
	/**
	 * @return -1=>broadcast v.id along outgoing edges; 
	 *         not(-1)=>send v.id to the vertex u with u.id==_valule
	 */
	public int value() {
		return value;
	}
	
	public int getByteSize() {
		return 5; //(1+4)
	}
	
    public void write(ByteBuffer out) throws IOException {
    	out.put(matched);
    	out.putInt(value);
    }
    
    public void read(ByteBuffer in) throws IOException {
    	matched = in.get();
    	value = in.getInt();
    }
    
    @Override
    public String toString() {
    	StringBuffer sb = new StringBuffer();
    	sb.append(Byte.toString(matched));
    	sb.append(" ");
    	sb.append(Integer.toString(value));
    	return sb.toString();
    }
    
    public void parseValue(String str) {
    	String[] strs = str.split(" ");
    	matched = Byte.parseByte(strs[0]);
    	value = Integer.parseInt(strs[1]);
    } 
}
