import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	// Your mapper function is a class that extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	public static class MyMapper
		extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Take the value, get its contents, convert to lowercase, and remove every character except for
			// spaces and a-z values
			String document = value.toString().toLowerCase().replaceAll("[^a-z\\s]", "");
			// Split the line up in an array of words
			String[] words = document.split(" ");
			for (String word : words) {
				// "context" is used to emit output values from your mapper or reducer
				// Note that we cannot emit standard Java types such as int, String, etc.
				// Instead, we need to use a org.apache.hadoop.io.* class
				// Such as Text (for string values) and IntWritable (for integers)
				Text textWord = new Text(word);
				context.write(textWord, one);
			}
		}
	}

	// Your reducer function is a class that extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	public static class MyReducer 
		extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// Give your "job" a sensible short name
		Job job = Job.getInstance(conf, "wordcount");
		// Tell Hadoop which JAR it needs to distribute to the workers
		// We can easily set this using setJarByClass
		// Don't forget to change this in your other programs
		job.setJarByClass(WordCount.class);
		
		// What is your mapper and reducer class?
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		// What does the output look like? Change if necessary
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Our program expects two arguments, the first one is the input file
		// Tell Hadoop our input is in the form of TextInputFormat (every line in the file will become value to be mapped)
		TextInputFormat.addInputPath(job, new Path(args[0]));
		// The second one is the output directory
		Path outputDir = new Path(args[1]);
		// Tell Hadoop what our desired output structure is: a file in a directory
		FileOutputFormat.setOutputPath(job, outputDir);
		
		// Delete the output directory if it exists to start fresh
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outputDir, true);
		
		// Stop after our job has completed
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}