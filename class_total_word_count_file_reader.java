
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 * count a class's word total times 
 * input class's word count times file,
 * map input key is the class name, value is the appear times
 */
public class class_total_word_count_file_reader extends FileInputFormat<Text, IntWritable> {
	@Override
	public boolean isSplitable(JobContext context, Path p) {
		//指定输入文件不被分片
		return false;
	}

	@Override
	public RecordReader<Text, IntWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		return new WholeFileRecordReader();
	}

	public static class WholeFileRecordReader extends
			RecordReader<Text, IntWritable> {
		private Text key = new Text();
		private LineReader lr;
		private IntWritable value = new IntWritable();
		private boolean read = false;
		private FileSystem fs = null;
		private FileSplit fSplit = null;
		private long start;
		private long end;
		private long currentPos;
		private Text line = new Text();

		@Override
		public void close() throws IOException {
			lr.close();
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public IntWritable getCurrentValue() throws IOException,
				InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return read ? 1 : 0;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			read = false;
			fSplit = (FileSplit) split;
			Configuration conf = context.getConfiguration();
			Path path = fSplit.getPath();
			fs = FileSystem.get(conf);
			FSDataInputStream fis=fs.open(path);
			lr = new LineReader(fis,conf);
			start=fSplit.getStart();
			end=start+fSplit.getLength();
			fis.seek(start);
			if(start!=0) {
				start += lr.readLine(new Text(),0,
						(int)Math.min(Integer.MAX_VALUE, end-start));
				currentPos=start;
			}
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (!read) {
				if(currentPos > end) {
					read = true;
					return false;
				}
				currentPos += lr.readLine(line);
				if(line.getLength()==0) {
					return false;
				}
				String word[] = line.toString().split("\\s");
				
				if(line.toString()==null || word.length<2) {
					System.err.println("line:"+line.toString()+".");
					return false;
				}
				if(word.length>=2) {
					value.set(Integer.parseInt(word[2]));
					key.set(word[0]);
				}
				else {
					value.set(0);
					key.set(line.toString());
				}
				return true;
			} else {
				return false;
			}
		}
		
	}
}