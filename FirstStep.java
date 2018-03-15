import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FirstStep {

	/*
	input:
		facebook	videos
		google	videos
		google	search
		facebook	photos
		youtube	photos
		youtube	videos

	output:
		videos	[facebook, google, youtube]
		search	[google]
		photos	[facebook, youtube]
	*/
	public static class FirstStepMapper extends
	Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split("\t");
			context.write(new Text(parts[1]), new Text(parts[0]));
		}
	}

	/*
	input:
		videos	[facebook, google, youtube]
		search	[google]
		photos	[facebook, youtube]

	output:
		facebook:google	1
		google:facebook	1
		facebook:youtube	1
		youtube:facebook	1
		youtube:google	1
		google:youtube	1
		facebook:youtube	1
		youtube:facebook	1
	*/
	public static class FirstStepReducer extends
	Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			List<String> list = new ArrayList<String>();

			for (Text t : values) {
				for (String previous : list) {
					context.write(new Text(previous + ":" + t.toString()), new Text("1"));
					context.write(new Text(t.toString() + ":" + previous), new Text("1"));
				}
				list.add(t.toString());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "FirstStep");
		job.setJarByClass(FirstStep.class);
		job.setReducerClass(FirstStepReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, FirstStepMapper.class);
		Path outputPath = new Path(args[1]);

		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
