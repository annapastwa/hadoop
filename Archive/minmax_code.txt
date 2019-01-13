import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MinMaxTempPerMonth {

	public static class MyMapper
		extends Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Our input value is a line of measurements, tab separated:
			String[] measurement = value.toString().split("\\t");
			// Convert to second [1] and fourth [3] values to integers
			int month = Integer.parseInt(measurement[1]);
			int temp = Integer.parseInt(measurement[3]);
			
			// Make a textual key
			Text mappedKey = new Text(month+"");
			// Make textual value: just two times temp, separated with a space
			Text mappedValue = new Text(temp+" "+temp);
			context.write(mappedKey, mappedValue);
		}
	}

	public static class MyReducer 
		extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int newMin = 0;
			int newMax = 0;
			boolean firstValueDone = false;
			
			for (Text val : values) {
				// Convert our two temperatures separated with a space back to integers
				String[] valueMinMax = val.toString().split(" ");
				int min = Integer.parseInt(valueMinMax[0]);
				int max = Integer.parseInt(valueMinMax[1]);
				// Check if this is the first value seen or whether the min is lower than current min
				if (min < newMin || !firstValueDone)
					newMin = min;
				// Same for max
				if (max > newMax || !firstValueDone)
					newMax = max;
				firstValueDone = true;
			}
			// Make textual value: the min and max, separated with a space
			Text reducedValue = new Text(newMin+" "+newMax);
			// Our incoming key is the month (as Text), but we can just reuse that as is
			context.write(key, reducedValue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "minmaxtemppermonth");
		job.setJarByClass(MinMaxTempPerMonth.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		// Here, both the output key and values are Text!
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outputDir, true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}