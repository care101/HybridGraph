package org.apache.hama.myhama.graph;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;

public class BitBytes {
	//private static final Log LOG = LogFactory.getLog(BitBytes.class);
	int[] bitMarks = {0, 1, 2, 4, 8, 16, 32, 64};
	double base = 7.0; //7 bits in one byte is used
	
	public BitBytes() {
		
	}
	
	public long serialize(boolean[] flags, final int _fromIdx, final int _toIdx, 
			File output, final int _taskMinId) throws Exception {;
		int fromIdx = _fromIdx, toIdx = _toIdx; 
		//int taskMinId = _taskMinId;
		long numOfBytes = (long)Math.ceil((toIdx-fromIdx)/base);
		RandomAccessFile raf = new RandomAccessFile(output, "rw");
		FileChannel fc = raf.getChannel();
		MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_WRITE, 0L, numOfBytes);
		
		int counter = 1;
		int b = 0;
		for (; fromIdx < toIdx; fromIdx++) {
			if (flags[fromIdx]) {
				b = b | bitMarks[counter];
			}
			/*if ((taskMinId+fromIdx)>=93 && 
					(taskMinId+fromIdx)<=113) {
				LOG.info("vid=" + (taskMinId+fromIdx) 
						+ ", resFlag=" + resFlags[fromIdx]
						+ ", byte=" + b);
			}*/
			counter++;
			if (counter == (base+1)) {
				counter = 1;
				mbb.put((byte)b);
				b = 0;
			}
		}
		if (counter > 1) {
			mbb.put((byte)b);
		}
		
		fc.close();
		raf.close();
		return numOfBytes;
	}
	
	public long deserialize(boolean[] flags, final int _fromIdx, final int _toIdx, 
			File input, final int _taskMinId) throws Exception {
		int fromIdx = _fromIdx, toIdx = _toIdx, taskMinId = _taskMinId;
		long numOfBytes = (long)Math.ceil((toIdx-fromIdx)/base);
		RandomAccessFile raf = new RandomAccessFile(input, "r");
		FileChannel fc = raf.getChannel();
		MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_ONLY, 0L, numOfBytes);
		
		int counter = 1;
		byte b = 0;
		for (int idxOfBytes=0; idxOfBytes<numOfBytes; idxOfBytes++) {
			b = mbb.get();
			for (counter=1; counter<=base; counter++) {
				if ((b&bitMarks[counter]) == bitMarks[counter]) {
					/*if (!resFlags[fromIdx]) {
						LOG.error("i think it is true, but actually false, vid=" 
								+ (fromIdx+taskMinId) + ", byte=" + b);
					}*/
					flags[fromIdx] = true;
				} else {
					/*if (resFlags[fromIdx]) {
						LOG.error("i think it is false, but actually true, vid=" 
								+ (fromIdx+taskMinId) + ", byte=" + b);
					}*/
					flags[fromIdx] = false;
				}
				fromIdx++;
				if (fromIdx == toIdx) {
					//LOG.info("terminate...");
					break;
				}
			}
		}
		
		fc.close();
		raf.close();
		return numOfBytes;
	}
}
