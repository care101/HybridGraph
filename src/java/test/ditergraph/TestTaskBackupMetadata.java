package test.ditergraph;

import org.apache.hama.monitor.TaskBackupMetadata;

public class TestTaskBackupMetadata {
	
	public static void main(String[] args) {
		TaskBackupMetadata tbm = new TaskBackupMetadata();
		tbm.initialize(1, 2);
		
		for (int i = 0; i < 20; i++) {
			tbm.updateMaxOuterGhostInfo(i % 2, i, i * 10);
		}
		
		System.out.println(tbm.toString());
	}
}
