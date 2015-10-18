package test.ditergraph;

import org.apache.hama.myhama.graph.VerHashBucBeta;

public class TestVerHashBucMgr {
	
	public static void main(String[] args) {
		int _verMinId = 23934034;
		int _verMaxId = 23934082;
		int _bucNum = 30;
		int hashBucLen = (_verMaxId-_verMinId+1)/_bucNum;
		System.out.println("_bucLen=" + hashBucLen);
		
		int sum = _verMinId;
        for (int bid = 0; bid < _bucNum; bid++) {
        	int tmpLen = hashBucLen;
        	if (bid == (_bucNum-1)) {
        		tmpLen = _verMaxId - sum + 1;
        	}
        	
        	System.out.println("min=" + sum + " max=" + (sum+tmpLen-1) + " len=" + tmpLen);
        	sum += tmpLen;
        }
	}
}
