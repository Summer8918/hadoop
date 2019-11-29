

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
Hadoop的HDFS和MapReduce框架主要是针对大数据文件设计的，在小文件的处理上不但效率低下。
而且十分消耗内存资源(每一个小文件占用一个Block,每一个block的元数据都存储在namenode
的内存里)。解决办法通常是选择一个容器，将这些小文件组织起来统一存储。HDFS 提供了两种
类型的容器，分别是SequenceFile和MapFile。这里使用SequenceFile来存储小文件
 */

public class SequenceFileWriter {
     /**
      * 将目录下的所有文件序列化
      * 输入：args[0]：准备序列化的文件的路径
      * 输出：args[1]：序列化后准备输出的文件路径名字
      */
	public static String get_class_and_file_name(String filePath) {
		StringTokenizer itr = new StringTokenizer(filePath,"/");
        int cnt=itr.countTokens();
        String str = new String();  //class tag
        while(cnt>0){
            str=itr.nextToken();
            if(cnt-- == 2){
            	str=str+":"+itr.nextToken();
                break;
            }
        }
        return str;
	}
	public static void main(String[] args) throws IOException {	
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path input_path=new Path(args[0]);
		System.out.println("hello");
		//递归读取指定目录下的所有文件
		RemoteIterator<LocatedFileStatus> file_itr=fs.listFiles(input_path, true);
		int cnt=0;
		Text key = new Text();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		Path output_path=new Path(args[1]);
		try{
			writer = SequenceFile.createWriter(fs, conf, output_path, key.getClass(), value.getClass());
			while(file_itr.hasNext()) {
				LocatedFileStatus tmp=file_itr.next();
				Path inFile=tmp.getPath();
				//读取单个文件的内容，将其转换为String类型，每行以空格分隔
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inFile)));
				String val_str="";
				try {
					String line;
					line = br.readLine();
					while(line!=null) {
						line = br.readLine();
						val_str=val_str+line+" ";
					}
				}
				finally {
					br.close();
				}
				key.set(get_class_and_file_name(tmp.getPath().toString()));
				value.set(val_str);
				System.out.println(key.toString());
				writer.append(key, value);
				cnt++;
			}
		}finally{
			IOUtils.closeStream(writer);
		}		
	}
}

