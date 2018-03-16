import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondStep {

	/*
	input:
		facebook google youtube
		google
		facebook youtube

	output:
		facebook:google	[1]
		facebook:youtube	[1,1]
		google:facebook	[1]
		google:youtube	[1]
		youtube:facebook	[1,1]
		youtube:google	[1]
	*/
	public static class SecondStepMapper extends
	Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			List<String> sites = new ArrayList<String>(Arrays.asList(value.toString().split(" ")));
			List<String> passedSites = new ArrayList<String>();

			for (String t : sites) {
				for (String previous : passedSites) {
					context.write(new Text(previous + ":" + t), one);
					context.write(new Text(t + ":" + previous), one);
				}
				passedSites.add(t);
			}
		}
	}

	/*
	input:
		facebook:google	[1]
		facebook:youtube	[1,1]
		google:facebook	[1]
		google:youtube	[1]
		youtube:facebook	[1,1]
		youtube:google	[1]

	output:
		facebook:google:1	null
		facebook:youtube:2 null
		google:facebook:1	null
		google:youtube:1	null
		youtube:facebook:2	null
		youtube:google:1	null
	*/
	public static class SecondStepReducer extends
	Reducer<Text, IntWritable, Text, NullWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
      context.write(new Text(key + ":" + sum), NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "SecondStep");
		job.setJarByClass(SecondStep.class);
		job.setReducerClass(SecondStepReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, SecondStepMapper.class);
		Path outputPath = new Path(args[1]);

		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
