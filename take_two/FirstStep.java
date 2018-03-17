import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

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
		facebook google youtube
		google
		facebook youtube
	*/
	public static class FirstStepReducer extends
	Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			String value = "";

			for (Text t : values) {
				value += t.toString();
				value += " ";
			}

			context.write(new Text(value.substring(0, value.length() - 1)), NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "FirstStep");
		job.setJarByClass(FirstStep.class);
		job.setReducerClass(FirstStepReducer.class);
		job.setMapperClass(FirstStepMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
