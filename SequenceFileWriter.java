

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
 * SequenceFileWriter:
 * 序列化小文件，格式为<<dir:doc>,contents>
 * eg:I01002:484619newsML.txt	national oilseed processorsassociation weekly soybean crushings reporting members...
 */

public class SequenceFileWriter {
	/**
	 * 将一个文件夹下的所有文件序列化,两个参数:
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

