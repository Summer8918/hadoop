import java.io.IOException;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class map_reduce {
      /*
       *计算属于各类别的文档总数 *map的输入Key为className:docName，输入Value为文档内容；
       *输出Key为className，输出Value为1，表示className的一个文档  
       */
	  public static class cnt_class_number_map
		  extends Mapper<Object, Text, Text, IntWritable>{
              
		private static IntWritable one=new IntWritable(1);
		private static Text key2=new Text();
	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	String[] word=key.toString().split(":");
	    	if(word.length>=0&&word[0]!=null) {
	    		key2.set(word[0]);
	    		context.write(key2, one);
	    	}
	    }
	  }

      /*
       * reduce将className的文档数相加，得到类名为className的文档总数
       * 输出Key为className，输出Value为className的文档总数 
       */
	  public static class cnt_class_number_reduce
	       extends Reducer<Text,IntWritable,Text,IntWritable> {
	    private IntWritable result = new IntWritable();

	    public void reduce(Text key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	    }
	  }
	  
      /*   
       * 计算属于某一类别的单词出现次数
       * map的输入Key为class_name:file_name，Value是文件的内容
       * map的输出为<class_name,term>,times>
       * <class_name,term>是自定义的数据类型text_pair
       * map的输出Value是1，表示属于类名为class_name有一个term  
       */
	  public static class class_word_count_map
		  extends Mapper<Object, Text, TextPair, IntWritable>{
		private static IntWritable one=new IntWritable(1);
		private static TextPair keys=new TextPair();
		private static Text first=new Text();
		private static Text second=new Text();
	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	String[] word=key.toString().split(":");
	    	if(word.length>=0&& word[0]!=null) {
	    		first.set(word[0]);   //class name 
	    		String[] values=value.toString().split(" ");
		    	for(int i=0;i<values.length;i++) {
		    		second.set(values[i]);
		    		keys.set(first, second);
		    		context.write(keys, one);
		    	}
	    	}
	    }
	  }
      /*
       * Reduce的输出Key为自定义类型TextPair，<class_name,term>
       * Reduce的输出Value是所有类名为class_name的文档中单词term出现的次数
       */
	  public static class class_word_count_reduce
	       extends Reducer<TextPair,IntWritable,TextPair,IntWritable> {
	    private IntWritable result = new IntWritable();

	    public void reduce(TextPair key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	    }
	  }
	  
      /*
       * 获得训练集的单词字典
       * 输入文件为output2，使用自定义的RecordReader处理
       * Map的输入为<term,times>,Map的输出为<term,1>
       * 
       */
	  public static class word_dirictory_map
	     //  extends Mapper<Object, BytesWritable, Text, IntWritable>{
		  extends Mapper<Object, IntWritable, Text, IntWritable>{
	    public void map(Object key, IntWritable value, Context context
	                    ) throws IOException, InterruptedException {
	    	context.write((Text) key, value);
	    }
	  }

      /*
       * reduce将相同的term合并，生成结果<term,1>
       */
	  public static class word_dirictory_reduce
	       extends Reducer<Text,IntWritable,Text,IntWritable> {
		private final static IntWritable one = new IntWritable(1);

	    public void reduce(Text key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      context.write(key, one);
	    }
	  }
	  
       /*   
        * 计算每个类的单词总数    
        * Map的输入Key 为类名
        * Map的输入Value 为某个单词在属于类名class_name的文档中出现的次数  
        */
	  public static class class_total_word_count_map
	  extends Mapper<Object, IntWritable, Text, IntWritable>{
    public void map(Object key, IntWritable value, Context context
                    ) throws IOException, InterruptedException {
    	context.write((Text) key, value);
    }
  }
    
    /*    
     * Reduce将属于class_name文档的单词次数相加，得到class_name的单词总数
     * 输出Key-Value对为<class_name,times>
     */
      public static class class_total_word_count_reduce
           extends Reducer<Text,IntWritable,Text,IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,Context 
            context) throws IOException, InterruptedException {
          int sum = 0;
          for (IntWritable val : values) {
            sum += val.get();
          }
          result.set(sum);
          context.write(key, result);
        }
      }
 
	  
  public static class Predition {
		private static HashMap<String,Double> prior_Probability = new HashMap<String,Double>();
		private static HashMap<String,Integer> class_word_total_nums = new HashMap<String,Integer>();
		private static long vocabulary_size=0;
		private static HashMap<String,Double> class_word_probability = new HashMap<String,Double>();
        /*
         * 计算先验概率
         */
		public static HashMap<String,Double> getPriorProbability(String args)
				throws IOException, InterruptedException{
			int totalDocNum=0;
			Path path=new Path(args+"/part-r-00000");
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			try {
				String line;
				line = br.readLine();
				while(line!=null) {
					String [] a = line.split("\\s");	
					int num = Integer.parseInt(a[1]);	//word appear times
					totalDocNum += num;
					line = br.readLine();
				}
			}
			finally {
				br.close();
			}
			br = new BufferedReader(new InputStreamReader(fs.open(path)));
			try {
				String line;
				line = br.readLine();
				while(line!=null) {
					String [] a = line.split("\\s");	
					int num = Integer.parseInt(a[1]);	//word appear times
					double probability=1.0;
					probability = probability * num / totalDocNum;
					prior_Probability.put(a[0],probability);
					System.out.println(a[0]+" "+probability);
					line = br.readLine();
				}
			}
			finally {
				System.out.println("end prior prob function");
				br.close();
			}
			return prior_Probability;
		}
         /*
         * 计算训练集词典大小
         */
		public static void vocabulary_size(String args2) throws IOException, InterruptedException{
			Path path=new Path(args2+"/part-r-00000");
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			try {
				String line;
				line = br.readLine();
				while(line!=null) {
					vocabulary_size+=1;
					line = br.readLine();
				}
			}
			finally {
				br.close();
				System.out.println("vocabulary size="+vocabulary_size);
			}
		}
		public static HashMap<String,Integer> get_class_word_nums(String args)
				throws IOException, InterruptedException{
			Path path=new Path(args+"/part-r-00000");	//output4
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			try {
				String line;
				line = br.readLine();
				while(line!=null) {
					String[] word = line.split("\\s");
					class_word_total_nums.put(word[0],Integer.parseInt(word[1]));
					line = br.readLine();
				}
			}
			finally {
				br.close();
				System.out.println("end class word total nums function");
			}
			return class_word_total_nums;
		}
        /*
         * 计算条件概率
         */
		public static HashMap<String,Double> get_class_word_probability(String args)
			throws IOException, InterruptedException{
			Path path=new Path(args+"/part-r-00000");
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			try {
				String line;
				line = br.readLine();
				while(line!=null) {
					String[] word = line.split("\\s");
					String class_name=word[0];
					Integer class_term_times=Integer.parseInt(word[2]);
					Integer class_total_word_num=class_word_total_nums.get(class_name);
					Double prob=(class_term_times+1.0)/(vocabulary_size+class_total_word_num);
					class_word_probability.put(word[0]+":"+word[1],prob);
					line = br.readLine();
				}
			}
			finally {
				System.out.println("end class word prob function");
				br.close();
			}
			return class_word_probability;
		}
        
        /*
         * 计算文档属于某一类别的概率
         */
		public static Double conditionalProbabilityForClass(String content,String className) {
			Double prob=0.0;
			if(prior_Probability.get(className)!=null) {
				prob+=Math.log(prior_Probability.get(className));
			}
			String[] words=content.split(" ");
			for(String word : words) {
				String target_str=className+":"+word;
				if(target_str!=null) {
					if(class_word_probability.get(target_str)!=null) {
						prob +=  Math.log(class_word_probability.get(target_str));
					}
					else {
						prob += Math.log(1.0/(class_word_total_nums.get(className)+vocabulary_size));
					}
				}
			}
			return prob;
		}
	}
          /*
           * Map的输入Key为文档名，Map的输入Value为文档内容
           * 计算文档属于所有类的概率
           */
		  public static class predition_map
		       extends Mapper<Object, Text, Text, Text>{
			  
			public void setup(Context context) throws IOException, InterruptedException{
				System.out.println("set up begin");
				String args0= "/user/output1";
			    Predition.getPriorProbability(args0);
			    String args1= "/user/output3";
			    Predition.vocabulary_size(args1);
			    String args2= "/user/output4";
			    Predition.get_class_word_nums(args2);
			    String args3="/user/output2";
			    Predition.get_class_word_probability(args3);
			}
			
		    private Text docName = new Text();
		    private Text val=new Text();
		    private static int cnt=0; 
		    public void map(Object key, Text value, Context context
		                    ) throws IOException, InterruptedException {
		    	String[] word=key.toString().split(":");
		    	if(word.length>1) {
		    		docName.set(word[1]);
		    	}
		    	if(key==null||value==null) {
		    		val.set("empty");
		    		context.write((Text)key, val);
		    	}
		    	else {
		    		for(String str : Predition.class_word_total_nums.keySet()) {
		    	  		Double prob=Predition.conditionalProbabilityForClass(value.toString(), str);
		    	  		val.set(str+" "+prob.toString());	//	class_name+",,"+prob
		    	  		context.write(docName, val);
		    	  	}
		    	}
		    	cnt++;
		    }
		  }
          /*
           * 寻找docName文件概率最大的Prob和对应的ClassName
           */
		  public static class predition_combine extends Reducer<Text,Text,Text,Text> {
			    private Text res = new Text();
			    static int cnt2=0;
			    public void reduce(Text key, Iterable<Text> values,Context context) 
			    		throws IOException, InterruptedException {
			    	String tmp_class=null;
			    	double tmp_prob=-Double.MAX_VALUE;

			    	for(Text value:values) {
			    		String[] word=value.toString().split(" ");
			    		if(word.length>=2) {
			  				if(Double.parseDouble(word[1])>tmp_prob) {
			  					tmp_class=word[0];
			  					tmp_prob=Double.parseDouble(word[1]);
			  				}
			    		}
			  		}
			    	res.set(tmp_class+" "+Double.toString(tmp_prob));
			    	context.write(key, res);
			    	cnt2++;
			    }
		  }
          
          /*
           * 寻找每个文件名概率最大的Prob对应的ClassName
           */
		  public static class predition_reduce extends Reducer<Text,Text,Text,Text> {
		    private Text res = new Text();
		    static int cnt2=0;
		    public void reduce(Text key, Iterable<Text> values,Context context) 
		    		throws IOException, InterruptedException {
		    	String tmp_class=null;
		    	double tmp_prob=-Double.MAX_VALUE;
		    	for(Text value:values) {
		    		String[] word=value.toString().split(" ");
		    		if(word.length>=2) {
		  				if(Double.parseDouble(word[1])>tmp_prob) {
		  					tmp_class=word[0];
		  					tmp_prob=Double.parseDouble(word[1]);
		  				}
		    		}
		  		}
		    	res.set(tmp_class);
		    	context.write(key, res);
		    	cnt2++;
		    }
		  }
		  
	  public static void main(String[] args) throws Exception {
		ToolRunner.run(new nbcontroller(), args);
	    System.out.println(predition_map.cnt);
	    System.out.println(predition_reduce.cnt2);
	    System.exit(1);
	  }
	}