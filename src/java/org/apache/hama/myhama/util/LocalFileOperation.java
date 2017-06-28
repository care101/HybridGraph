package org.apache.hama.myhama.util;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LocalFileOperation {
	private static final Log LOG = LogFactory.getLog(LocalFileOperation.class);
	
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
				//LOG.info("delete file=" + file);
			} else if (file.isDirectory()) {
				deleteDir(file); // recursive delete the subdir
			}
		}
		
		dir.delete();// delete the root dir
	}
}
