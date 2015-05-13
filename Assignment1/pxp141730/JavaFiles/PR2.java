import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PR2 {

	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private Text key;
	

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] userInfo = line.split("::");
			if(Integer.parseInt(userInfo[2])>0 && Integer.parseInt(userInfo[2]) < 18 ){
				this.key = new Text("7 "+ userInfo[1]);  
				output.collect(this.key, new IntWritable(1));
			} else if(Integer.parseInt(userInfo[2])>=18 && Integer.parseInt(userInfo[2]) <= 24 ){
				this.key = new Text("24 "+ userInfo[1]);  
				output.collect(this.key, new IntWritable(1));
			} else if(Integer.parseInt(userInfo[2])>=25 && Integer.parseInt(userInfo[2]) <= 34 ){
				this.key = new Text("31 "+ userInfo[1]);  
				output.collect(this.key, new IntWritable(1));
			} else if(Integer.parseInt(userInfo[2])>=35 && Integer.parseInt(userInfo[2]) <= 44 ){
				this.key = new Text("41 "+ userInfo[1]);  
				output.collect(this.key, new IntWritable(1));
			} else if(Integer.parseInt(userInfo[2])>=45 && Integer.parseInt(userInfo[2]) < 55 ){
				this.key = new Text("51 "+ userInfo[1]);  
				output.collect(this.key, new IntWritable(1));
			} else if(Integer.parseInt(userInfo[2])>55 && Integer.parseInt(userInfo[2]) < 61 ){
				this.key = new Text("56 "+ userInfo[1]);  
				output.collect(this.key, new IntWritable(1));
			} else if(Integer.parseInt(userInfo[2])>=62  ){
				this.key = new Text("62 "+ userInfo[1]);  
				output.collect(this.key, new IntWritable(1));
			} 
			
		}
	}
	
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		      int sum = 0;
		       while (values.hasNext()) {
			         sum += values.next().get();
		       }
		       output.collect(key, new IntWritable(sum));
		     }
	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(PR2.class);
		conf.setJobName("PR2");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(PR2.Map.class);
		conf.setCombinerClass(PR2.Reduce.class);
		conf.setReducerClass(PR2.Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}
