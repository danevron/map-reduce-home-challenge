import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.lang.Integer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class ThirdStep {

	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {

			Text t1 = (Text) w1;
			Text t2 = (Text) w2;
			String[] t1Items = t1.toString().split(":");
			String[] t2Items = t2.toString().split(":");
			String t1Base = t1Items[0];
			String t2Base = t2Items[0];
			int comp = t1Base.compareTo(t2Base);

			if (comp == 0) {
				comp = -1 * Integer.compare(Integer.parseInt(t1Items[2]), Integer.parseInt(t2Items[2]));
			}
			return comp;
		}
	}

	public static class GroupComparator extends WritableComparator {

		protected GroupComparator() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {

			Text t1 = (Text) w1;
			Text t2 = (Text) w2;
			String t1Base = t1.toString().split(":")[0];
			String t2Base = t2.toString().split(":")[0];
			int comp = t1Base.compareTo(t2Base);

			return comp;

		}
	}

	public static class CustomPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String naturalKey = key.toString().split(":")[0];
			int part  = naturalKey.hashCode() % numPartitions;
			return part;
		}
	}

	/*
	input:
	facebook:google:1
	facebook:youtube:2
	google:facebook:1
	google:youtube:1
	youtube:facebook:2
	youtube:google:1

	output:
	facebook:google:1	google 1
	facebook:youtube:2	youtube 2
	google:facebook:1	facebook 1
	google:youtube:1	youtube 1
	youtube:facebook:2	facebook 2
	youtube:google:1 google 1
	*/
	public static class ThirdStepMapper extends
	Mapper<LongWritable, Text, Text, Text> {


		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

			String[] parts = value.toString().split(":");
			context.write(value, new Text(parts[1] + " " + parts[2]));
		}
	}

	/*
	input:
	facebook:google:1	google 1
	facebook:youtube:2	youtube 2
	google:facebook:1	facebook 1
	google:youtube:1	youtube 1
	youtube:facebook:2	facebook 2
	youtube:google:1 google 1

	output:
	facebook youtube 2
	facebook google 1
	google facebook 1
	google youtube 1
	youtube facebook 2
	youtube google 1
	*/
	public static class ThirdStepReducer extends
	Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {

			String naturalKey = key.toString().split(":")[0];

			int count = 0;
			for (Text val : values) {
				if (count < 10) {
					context.write(new Text(naturalKey), val);
					count++;
				} else {
					break;
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ThirdStep");
		job.setJarByClass(ThirdStep.class);
		job.setReducerClass(ThirdStepReducer.class);
		job.setMapperClass(ThirdStepMapper.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
