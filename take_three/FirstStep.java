import java.io.IOException;
import java.lang.StringBuilder;

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
		videos	facebook
		videos	google
		videos	youtube
		search	google
		photos	facebook
		photos	youtube
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
		photos	[youtube, facebook]
		search	[google]
		videos	[facebook, google, youtube]

	output:
		youtube facebook
		google
		facebook google youtube
	*/
	public static class FirstStepReducer extends
	Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			StringBuilder value = new StringBuilder();

      String space = "";
			for (Text t : values) {
				value.append(space);
				space = " ";
				value.append(t.toString());
			}

			context.write(new Text(value.toString()), NullWritable.get());
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
