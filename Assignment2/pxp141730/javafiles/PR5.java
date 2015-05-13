import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PR5 {

	public static class MapperMapSideJoinDCacheTextFile extends Mapper<LongWritable, Text, Text, Text> {

		private static HashMap<String, String> userMap = new HashMap<String, String>();
		private Text txtMapOutputKey = new Text("");
		private Text txtMapOutputValue = new Text("");
		private String movieID;
		private BufferedReader brReader;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			
			 Configuration conf=context.getConfiguration();
			 movieID=conf.get("movieId").toString();
			Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
	        for (Path P : cacheFilesLocal) {
	            String line = null;
	 
	        try {
	            brReader = new BufferedReader(new FileReader(P.getName()));
	 
	            while ((line = brReader.readLine()) != null) {
	                String newline = line.toString();
	                String userArray[] = newline.split("::");
	                userMap.put(userArray[0].trim(), userArray[1].trim() + "\t" + userArray[2].trim());
	            }
	 
	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	 
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	            if (brReader != null) {
	                brReader.close();
	 
	            }
	 
	        }
	            
	        }
		}


		/*public void configure(JobConf job) {
			movieID = job.get("movieId");

		}*/

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			
				String arrRatingAttributes[] = value.toString().split("::");

				if (arrRatingAttributes[1].equals(movieID) && Double.parseDouble(arrRatingAttributes[2]) >= 4) {

					txtMapOutputKey.set(arrRatingAttributes[0]);
					txtMapOutputValue.set(userMap.get(arrRatingAttributes[0]));
					//txtMapOutputValue.set(arrRatingAttributes[1]);
					context.write(txtMapOutputKey, txtMapOutputValue);
				}

		

		}
	}

	public static void main(String[] args) throws Exception {

		
		Configuration conf = new Configuration();         
		conf.set("movieId", args[3]);
			Job job = new Job(conf, "PR5");
		    
		    
		    DistributedCache.addCacheFile(new URI(args[0]),job.getConfiguration());
		 
		    job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
		    job.setJarByClass(PR5.class);
		    job.setMapperClass(MapperMapSideJoinDCacheTextFile.class);
		     
		    //job.setReducerClass(Reduce.class);
		         
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		         
		    FileInputFormat.addInputPath(job, new Path(args[1]));
		    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		         
		    job.waitForCompletion(true);

	}

}
