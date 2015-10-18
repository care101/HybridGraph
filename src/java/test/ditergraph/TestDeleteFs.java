package test.ditergraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hama.HamaConfiguration;

public class TestDeleteFs {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new HamaConfiguration();
		LocalFileSystem localFS = FileSystem.getLocal(conf);
	}
}
