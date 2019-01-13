import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CommonFriends {

	public static class MyMapper
	extends Mapper<Object, Text, Text, Text> {

		//first helper method is to build a string of Friends
		//takes as input a String array of tokens, which come from the text file
		// if there are just two friends, they do not have any other common friends, return empty string
		static String getFriends(String[] tokens) {
			if (tokens.length == 2) {
				return "";
			}
			StringBuilder builder = new StringBuilder();
			for (int i = 1; i < tokens.length; i++) {
				builder.append(tokens[i]);
				if (i < (tokens.length - 1)) {
					builder.append(",");
				}
			}
			return builder.toString();
		}

		//second helper method is to build a sorted key pair of person and friend
		static String buildSortedKey(String person, String friend) {

			int compare = person.compareToIgnoreCase(friend);
			if (compare < 0 ) {
				return person + "," + friend;
			} else {
				return friend + "," + person;
			}
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Our input value is a line of measurements, tab separated:
			String[] personfriends = value.toString().split("\\t");

			//Convert first element to person string, this will be the key
			String person = personfriends[0];

			//convert the following elements to a list of strings
			//String[] friends = Arrays.copyOfRange(personfriends, 1,personfriends.length);
			String friends  = getFriends(personfriends);

			// Make value -- should be a list of friends space separated
			Text mappedValue = new Text(friends);

			// Make a textual key which is sorted person friend tuple
			Text mappedKey = new Text();	

			for (int i = 1; i < personfriends.length; i++) {
				String friend = personfriends[i];
				String reducerKeyAsString = buildSortedKey(person, friend);
				mappedKey.set(reducerKeyAsString);

				context.write(mappedKey, mappedValue);
			}
		}
	}

	public static class MyReducer 
	extends Reducer<Text, Text, Text, Text> {

		static void addFriends(Map<String, Integer> map, String friendsList) {
			String[] friends = friendsList.split(",");
			for (String friend : friends) {
				Integer count = map.get(friend);
				if (count == null) {
					map.put(friend, 1);
				} else {
					map.put(friend, ++count);
				}
			}
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Map<String, Integer> map = new HashMap<String, Integer>();
			Iterator<Text> iterator = values.iterator();
			int numOfValues = 0;
			while (iterator.hasNext()) {
				String friends = iterator.next().toString();
				if (friends.equals("")) {
					context.write(key, new Text("[]"));
					return;
				}
				addFriends(map, friends);
				numOfValues++;
			}

			// now iterate the map to see how many have numOfValues
			List<String> commonFriends = new ArrayList<String>();
			for (Map.Entry<String, Integer> entry : map.entrySet()) {
				//System.out.println(entry.getKey() + "/" + entry.getValue());
				if (entry.getValue() == numOfValues) {
					commonFriends.add(entry.getKey());
				}
			}

			// send it to output
			context.write(key, new Text(commonFriends.toString()));
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "commonfriends"); //new name
		job.setJarByClass(CommonFriends.class);

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
