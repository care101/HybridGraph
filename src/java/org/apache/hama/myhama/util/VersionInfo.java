package org.apache.hama.myhama.util;

/**
 * Termite version information.
 * 
 * A version information class.
 * 
 * @author root
 * @version 0.2
 */
public class VersionInfo {

    public static String getVersionInfo() {
        return "beta-0.2";
    }
    
    public static String getSourceCodeInfo() {
        return "https://github.com/HybridGraph";
    }
    
    public static String getCompilerInfo() {
        return "root on " + getCompilerDateInfo();
    }
    
    public static String getWorkPlaceInfo() {
    	return "Unknown";
    }
    
    private static String getCompilerDateInfo() {
        return "10/08/2015";
    }
}
