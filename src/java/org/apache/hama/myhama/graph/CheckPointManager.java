package org.apache.hama.myhama.graph;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.myhama.io.KeyValueInputFormat;
import org.apache.hama.myhama.io.RecordReader;
import org.apache.hama.myhama.io.RecordWriter;
import org.apache.hama.myhama.io.TextBSPFileOutputFormat;

/**
 * A simple checkpointing-based fault-tolerance method. 
 * Data are archived periodically and then any failure 
 * can be recovered from the most recent available checkpoint.
 * 
 * @author zhigang
 * @version 0.2 (March 8, 2017)
 */
public class CheckPointManager {
	private static final Log LOG = LogFactory.getLog(CheckPointManager.class);
	private BSPJob jobConf;
	private String ckpTaskDir;
	private int lastCkpVersion; //the last checkpoint
	
	private RecordWriter<Text, Text> output;
	private RecordReader<Text,Text> input;
	
	/**
	 * Construct CheckPointManager class.
	 * @param _jobConf
	 * @param _taskId
	 * @param _ckpJobDir
	 */
	public CheckPointManager(BSPJob _jobConf, TaskAttemptID _taskId, 
			String _ckpJobDir) throws Exception {
		jobConf = _jobConf;
		output = null;
		input = null;
		lastCkpVersion = -1;
		
		Path path = new Path(_ckpJobDir, "task-"+_taskId.getIntegerId());
		FileSystem fs = path.getFileSystem(jobConf.getConf());
		if (fs.mkdirs(path)) {
			LOG.info("\ncreate checkpoint dir:" + path.toString());
		} else {
			LOG.error("\nfail to create checkpoint dir:" + path.toString());
		}
		
		ckpTaskDir = path.toString();
	}
	
	/**
	 * Prepare to archive a new checkpoint
	 * @param _newVersion
	 * @throws Exception
	 */
	public void befArchive(int _newVersion) throws Exception {
		if (_newVersion < 0) {
			return;
		}
		TextBSPFileOutputFormat outputFormat = new TextBSPFileOutputFormat();
		outputFormat.initialize(jobConf.getConf());
		output = outputFormat.getRecordWriter(jobConf, getPath(_newVersion));
	}
	
	/**
	 * Archive a vertex_id and vertex_value.
	 * @param key
	 * @param value
	 * @throws Exception
	 */
	public void archive(String key, String value) throws Exception {
		output.write(new Text(key), new Text(value));
	}
	
	/**
	 * Cleanup after archiving checkpoint.
	 * @param _newVersion
	 * @throws Exception
	 */
	public void aftArchive(int _newVersion) throws Exception {
		output.close(this.jobConf);
		
		/** delete old checkpoint to save storage space */
		if (lastCkpVersion > 0) {
			Path delPath = getPath(lastCkpVersion);
			FileSystem fs = delPath.getFileSystem(jobConf.getConf());
			fs.delete(delPath, true);
			fs.close();
		}
		
		output = null;
		lastCkpVersion = _newVersion;
	}
	
	/**
	 * Get {@link RecordReader} of an existing checkpoint file.
	 * @param int the most recent available checkpoint version
	 * @return NULL if no checkpoint is available
	 * @throws Exception
	 */
	public RecordReader<Text, Text> befLoad(int readyCkpVersion) throws Exception {
		if (readyCkpVersion < 0) {
			return null;
		}
		
		KeyValueInputFormat inputFormat = new KeyValueInputFormat();
		inputFormat.initialize(jobConf.getConf());
		inputFormat.addCheckpointInputPath(jobConf, getPath(readyCkpVersion));
		org.apache.hadoop.mapreduce.InputSplit split = inputFormat.getSplit(jobConf);
		if (split == null) {
			return null;
		}
		
		input = inputFormat.createRecordReader(split, jobConf);
		input.initialize(split, jobConf.getConf());
		return input;
	}
	
	/**
	 * Cleanup after loading checkpoint.
	 * @throws Exception
	 */
	public void aftLoad() throws Exception {
		if (input != null) {
			input.close();
			input = null;
		}
	}
	
	/**
	 * Get the outputpath for checkpoint file.
	 * @param version
	 * @return
	 */
	private Path getPath(int version) {
		return new Path(ckpTaskDir, "ckp-" + version);
	}
}