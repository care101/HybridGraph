package org.apache.hama.myhama.util;

/**
 * Termite version information.
 * 
 * A version information class.
 * 
 * @author Zhigang Wang
 * @version 0.2
 */
public class VersionInfo {

    public static String getVersionInfo() {
        return "beta-0.2";
    }
    
    public static String getSourceCodeInfo() {
        return "svn://192.168.0.72/HybridGraph";
    }
    
    public static String getCompilerInfo() {
        return "Zhigang Wang on " + getCompilerDateInfo();
    }
    
    public static String getWorkPlaceInfo() {
    	return "NeuSoftLab 401, Northeastern University";
    }
    
    private static String getCompilerDateInfo() {
        return "2015-06-26";
    }
}
