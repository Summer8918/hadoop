import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class nbcontroller extends Configured implements Tool{
	   public void delete(Configuration conf, Path path) throws IOException {
	       FileSystem fs = path.getFileSystem(conf);
	       if (fs.exists(path)) {
	           fs.delete(path, true);
	       } 
	   }
	  
	  @Override
	   public int run(String [] args) throws Exception {
		    Configuration conf = new Configuration();
		    conf.set("mapred.job.jar", "bayes.jar");
		    FileSystem hdfs = FileSystem.get(conf);
		    String trainFilesSequenceDir="hdfs:/user/trainSeqFiles";
		    SequenceFileWriter.turnSmallFilesToSequenceFile(trainFilesDir,trainFilesSequenceDir);
		    Job job1 = Job.getInstance(conf, "class documents count");
		    job1.setJarByClass(map_reduce.class);
		    //change mapper input format
		    job1.setInputFormatClass(SequenceFileInputFormat.class);
		    job1.setMapperClass(map_reduce.cnt_class_number_map.class);
		    job1.setCombinerClass(map_reduce.cnt_class_number_reduce.class);
		    job1.setReducerClass(map_reduce.cnt_class_number_reduce.class);
		    job1.setOutputKeyClass(Text.class);
		    job1.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job1, new Path(trainFilesSequenceDir));
		    Path outputPath1 = new Path(args[0]);
		    //delete exist output directory
		    if(hdfs.exists(outputPath1)) {
				hdfs.delete(outputPath1, true);
		    }
		    FileOutputFormat.setOutputPath(job1, outputPath1);
		    ControlledJob control_job1=new ControlledJob(conf);
		    control_job1.setJob(job1);
		    
		    Job job2 = Job.getInstance(conf, "class word count");
		    job2.setJarByClass(map_reduce.class);
		    //change mapper input format
		    job2.setInputFormatClass(SequenceFileInputFormat.class);
		    job2.setMapperClass(map_reduce.class_word_count_map.class);
		    job2.setCombinerClass(map_reduce.class_word_count_reduce.class);
		    job2.setReducerClass(map_reduce.class_word_count_reduce.class);
		    job2.setOutputKeyClass(TextPair.class);
		    job2.setOutputValueClass(IntWritable.class);

		    FileInputFormat.addInputPath(job2, new Path(trainFilesSequenceDir));

		    Path outputPath2 = new Path(args[1]);
		    //delete exist output directory
		    if(hdfs.exists(outputPath2)) {
				hdfs.delete(outputPath2, true);
		    }
		    FileOutputFormat.setOutputPath(job2, outputPath2);
		    ControlledJob control_job2=new ControlledJob(conf);
		    control_job2.setJob(job2);
		    
		    Job job3 = Job.getInstance(conf, "word vocabulary");
		    job3.setJarByClass(map_reduce.class);
		    //change mapper input format
		    job3.setInputFormatClass(word_vocabulary_file_reader.class);
		    job3.setMapperClass(map_reduce.word_dirictory_map.class);
		    job3.setCombinerClass(map_reduce.word_dirictory_reduce.class);
		    job3.setReducerClass(map_reduce.word_dirictory_reduce.class);
		    job3.setOutputKeyClass(Text.class);
		    job3.setOutputValueClass(IntWritable.class);
		    Path inputPath3 =new Path("hdfs:"+args[1]+"/part-r-00000");
		    FileInputFormat.setInputPaths(job3, inputPath3);
		    Path outputPath3 = new Path(args[2]);
		    //delete exist output directory
		    if(hdfs.exists(outputPath3)) {
				hdfs.delete(outputPath3, true);
		    }
		    FileOutputFormat.setOutputPath(job3, outputPath3);
		    ControlledJob control_job3=new ControlledJob(conf);
		    control_job3.setJob(job3);
		    control_job3.addDependingJob(control_job2);
		    
		    Job job4 = Job.getInstance(conf, "class total word count");
		    job4.setJarByClass(map_reduce.class);
		    job4.setInputFormatClass(class_total_word_count_file_reader.class);
		    job4.setMapperClass(map_reduce.class_total_word_count_map.class);
		    job4.setCombinerClass(map_reduce.class_total_word_count_reduce.class);
		    job4.setReducerClass(map_reduce.class_total_word_count_reduce.class);
		    job4.setOutputKeyClass(Text.class);
		    job4.setOutputValueClass(IntWritable.class);
		    Path inputPath4 =new Path("hdfs:"+args[1]+"/part-r-00000");
		    FileInputFormat.setInputPaths(job4, inputPath4);
		    Path outputPath4 = new Path(args[3]);
		    //delete exist output directory
		    if(hdfs.exists(outputPath4)) {
				hdfs.delete(outputPath4, true);
		    }
		    FileOutputFormat.setOutputPath(job4, outputPath4);
		    ControlledJob control_job4=new ControlledJob(conf);
		    control_job4.setJob(job4);
		    control_job4.addDependingJob(control_job2);
		  
		    Job job5 = Job.getInstance(conf, "document predition");
		    job5.setJarByClass(map_reduce.class);
		    //change mapper input format
		    job5.setInputFormatClass(SequenceFileInputFormat.class);
		    job5.setMapperClass(map_reduce.predition_map.class);
		    job5.setCombinerClass(map_reduce.predition_combine.class);
		    job5.setReducerClass(map_reduce.predition_reduce.class);
		    job5.setOutputKeyClass(Text.class);
		    job5.setOutputValueClass(Text.class);
		    String testFilesDir="hdfs:/user/BayesFiles/BayesFiles/testClassFile";
		    String testFilesSequenceDir="hdfs:/user/testSeqFiles";
		    SequenceFileWriter.turnSmallFilesToSequenceFile(testFilesDir,testFilesSequenceDir);
		    FileInputFormat.addInputPath(job5, new Path(testFilesSequenceDir));
		    Path outputPath5 = new Path(args[4]);
		    //delete exist output directory
		    if(hdfs.exists(outputPath5)) {
				hdfs.delete(outputPath5, true);
		    }
		    FileOutputFormat.setOutputPath(job5, outputPath5);
		    ControlledJob control_job5=new ControlledJob(conf);
		    control_job5.setJob(job5);
		    control_job5.addDependingJob(control_job4);
		    control_job5.addDependingJob(control_job3);
		    
		    JobControl whole_job_control=new JobControl("naive bayes classifier"); 
		    whole_job_control.addJob(control_job1);
		    whole_job_control.addJob(control_job2);
		    whole_job_control.addJob(control_job3);
		    whole_job_control.addJob(control_job4);
		    whole_job_control.addJob(control_job5);
		    Thread controller=new Thread(whole_job_control);
		    controller.start();
		    while(true) {
		    	if(whole_job_control.allFinished()) {
		    		System.out.println(whole_job_control.getSuccessfulJobList());
		    		whole_job_control.stop();
		    		break;
		    	}
		    }
		    return 0;
	  }
  }