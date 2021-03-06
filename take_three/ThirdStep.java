import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.lang.Integer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class ThirdStep {

	/*
	input:
		youtube:facebook	1
		facebook:youtube	1
		facebook:google	1
		google:facebook	1
		facebook:youtube	1
		youtube:facebook	1
		google:youtube	1
		youtube:google	1

	output:
		youtube:facebook	1
		facebook:youtube	1
		facebook:google	1
		google:facebook	1
		facebook:youtube	1
		youtube:facebook	1
		google:youtube	1
		youtube:google	1
	*/
	public static class ThirdStepMapper extends
	Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			String[] parts = value.toString().split("\t");

			context.write(new Text(parts[0]), new IntWritable(Integer.parseInt(parts[1])));
		}
	}

	/*
	input:
		facebook:google	[1]
		facebook:youtube	[1, 1]
		google:facebook	[1]
		google:youtube	[1]
		youtube:facebook	[1, 1]
		youtube:google	[1]

	output:
		facebook:google:1	null
		facebook:youtube:2 null
		google:facebook:1	null
		google:youtube:1	null
		youtube:facebook:2	null
		youtube:google:1	null
	*/
	public static class ThirdStepReducer extends
	Reducer<Text, IntWritable, Text, NullWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
      context.write(new Text(key.toString() + ":" + sum), NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ThirdStep");
		job.setJarByClass(ThirdStep.class);
		job.setReducerClass(ThirdStepReducer.class);
		job.setMapperClass(ThirdStepMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
