import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PR4 {

	public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

		// take the input from users.dat file and get the data
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] userInfo = line.split("::");
			String userId = userInfo[0]; // take userID from input this is our
											// joining attribute
			String gender = userInfo[1]; // take gender from input
			if (gender.equalsIgnoreCase("F")) // check whether the user is male
												// or female
			{
				// write userid as key and other info as value
				context.write(new Text(userId), new Text("1" + userId + "::" + gender));
			}

		}
	}// end of mapper1

	public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
		private Text ratingInfoText = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// extract required information from ratings file.
			String line = value.toString();
			String[] ratingInfo = line.split("::");
			String userId = ratingInfo[0];
			String movieId = ratingInfo[1];
			String rating = ratingInfo[2];
			// tag '2' is added to differentiate information from mapper1.
			ratingInfoText.set("2" + userId + "::" + movieId + "::" + rating);
			// emit user id and ratings details
			context.write(new Text(userId), ratingInfoText);

		}
	}// end of RatingMapper class

	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
		private ArrayList<Text> userInfo = new ArrayList<Text>();
		private ArrayList<Text> ratingInfo = new ArrayList<Text>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// clear the array lists
			userInfo.clear();
			ratingInfo.clear();
			{
				String[] temp1, temp2;
				for (Text val : values) {
					// check for tag. tag of 1 indicates it has come from
					// users.dat file after being processes by mapper1
					if (val.charAt(0) == '1') {
						val = new Text(val.toString().substring(1));
						userInfo.add(val);
					}
					// if tag is 2, it indicates that data has come from ratings
					// file after being processes by mapper2
					else if (val.charAt(0) == '2') {
						val = new Text(val.toString().substring(1));
						ratingInfo.add(val);
					}
				}
				for (Text usr : userInfo) // for each value in user info array
											// list get the ratings information
				{
					temp1 = usr.toString().split("::");
					String userId = (temp1[0]);
					for (Text rating1 : ratingInfo) // extract rating info of
													// each user from rating
													// info list
					{
						temp2 = rating1.toString().split("::");
						if (userId.equals(temp2[0])) { //

							String movieId = (temp2[1]);
							String rating = (temp2[2]);
							String details = movieId + "::" + rating;
							// write movie id as key and other useful info as
							// value
							context.write(new Text(movieId), new Text(details));
						}
					}
				}
			}
		}
	}// end of first reducer

	public static class Mapper3 extends Mapper<LongWritable, Text, Text, Text> {
		private Text movieID = new Text();
		private Text moviesFileDetails = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tempDetails = line.split("\t");
			movieID.set(tempDetails[0]);
			moviesFileDetails.set("3" + tempDetails[1].toString());
			context.write(movieID, moviesFileDetails);// movie Id is the join
														// key
		}
	}// end of mapper3

	public static class Mapper4 extends Mapper<Object, Text, Text, Text> {
		// private Text = new Text();
		private Text movieDetailsText = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] movieDetails = value.toString().split("::");
			// get the required information
			String movieId = movieDetails[0];
			String movieTitle = movieDetails[1];

			movieDetailsText.set("4" + movieId + "::" + movieTitle);
			context.write(new Text(movieId), movieDetailsText);

		}
	}// end of mapper4

	public static class ValueComparator implements Comparator<String> {
		private Map<String, Double> valueMap;

		public ValueComparator(Map<String, Double> valueMap) {
			this.valueMap = valueMap;
		}

		public int compare(String key1, String key2) {
			double val1 = (Double) valueMap.get(key1);
			double val2 = (Double) valueMap.get(key2);
			System.out.println(val1 +" :" + val2);
			if (val1 > val2) {
				
				return -1;
			} else {
				return 1;
			}
		}

	}
	
	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		private  ArrayList<Text> movie = new ArrayList<Text>();
		private  ArrayList<Text> rating = new ArrayList<Text>();
		private  static HashMap<String,Double> movieMap = new HashMap<String, Double>();
		private  static ValueComparator comparator = new ValueComparator(movieMap);
		private  static TreeMap<String, Double> topKMap =new TreeMap<String, Double>(comparator); 
		private static int index=0;
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// clear the array lists
			movie.clear();
			rating.clear();
			for (Text tmp : values) // for each value
			{ // seperate data from mapper3 and mapper 4 and put them in
				// different array lists
				if (tmp.charAt(0) == '3')
					rating.add(new Text(tmp.toString().substring(1)));
				else if (tmp.charAt(0) == '4')
					movie.add(new Text(tmp.toString().substring(1)));
			}

			for (Text mov1 : movie) // for each value in movie list, get the
									// user information
			{
				double totalRating = 0.0;
				int count = 0;

				String[] temp1 = mov1.toString().split("::");
				String movieId = temp1[0];
				for (Text val : rating) // for each value in rating list, get
										// the rating info, calculate the
										// average of rating for each user
				{
					String[] rating = val.toString().split("::");
					if (movieId.equals(rating[0])) {
						totalRating += Double.parseDouble(rating[1]);
						count++;
					}
				}
				/*
				 * if (count != 0 && movieCount<=5) { double avg = totalRating /
				 * count; // average of rating // top5.put(temp1[1], avg );
				 * }else if(count!=0 && movieCount>5){ double avg = totalRating
				 * / count; // average of rating
				 * if(avg>top5.firstEntry().getValue()){
				 * top5.remove(top5.firstKey()); top5.put(temp1[1],avg ); } }
				 */
				if (count != 0) {
					double avg = totalRating / count;
					//context.write(new Text(temp1[1]), new Text(String.valueOf(avg)));
					movieMap.put(temp1[1], avg);
				}
			}
			
			
			
			 
			
		}
		
		
		@Override
	    protected void cleanup(Context context) throws IOException,
	            InterruptedException {
			topKMap.putAll(movieMap);
			for (Entry<String, Double> entry : topKMap.entrySet()) {
				if (index < 5) {
					context.write(new Text(entry.getKey()), new Text(String.valueOf(entry.getValue())));
					index++;
				}
			}
	    }
	} // end of reducer2
	

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		// conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",
		// "\t");

		Job job = new Job(conf, "PR4"); // define the configuration of
										// assignment2 job
		job.setJarByClass(PR4.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class); // define the
																// input format
																// class

		job.setOutputKeyClass(Text.class);// output key format of Mapper
		job.setOutputValueClass(Text.class); // output value format of Mapper

		job.setReducerClass(Reducer1.class); // declare reducer1 class

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// input to mapper1 and mapper2
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Mapper1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Mapper2.class);

		// output file for reducer1's output
		FileOutputFormat.setOutputPath(job, new Path(args[3] + "temp/"));

		job.waitForCompletion(true); // wait till job 1 completes

		Job job2 = new Job(conf, "PR4"); // define 2nd job
		job2.setInputFormatClass(KeyValueTextInputFormat.class); //

		// job configurations of Job2 consisting of mapper3 , mapper4 and
		// reducer 2
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setJarByClass(PR4.class);

		job2.setReducerClass(Reducer2.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		// inputs to mapper3 and mapper4
		MultipleInputs
				.addInputPath(job2, new Path(args[3] + "temp/part-r-00000"), TextInputFormat.class, Mapper3.class);
		MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, Mapper4.class);

		// output file of reducer2
		FileOutputFormat.setOutputPath(job2, new Path(args[3] + "output/"));
		job2.waitForCompletion(true);


	} // end of main
}
