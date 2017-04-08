package org.apache.hama.myhama.util;

import java.io.File;

public class LocalFileOperation {
	
	/**
	 * Delete the given directory and all files in it.
	 * @param dir
	 */
	public void deleteDir(File dir) {
		if (dir==null || !dir.exists()) {
			return;
		}
		  
		for (File file : dir.listFiles()) {
			if (file.isFile()) {
				file.delete(); // delete the file
			} else if (file.isDirectory()) {
				deleteDir(file); // recursive delete the subdir
			}
		}
		
		dir.delete();// delete the root dir
	}
}
