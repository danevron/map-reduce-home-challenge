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

public class FourthStep {

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
			String naturalKey1 = t1Items[0];
			String naturalKey2 = t2Items[0];
			int comp = naturalKey1.compareTo(naturalKey2);

			if (comp == 0) {
				int valueKey1 = Integer.parseInt(t1Items[1]);
				int valueKey2 = Integer.parseInt(t2Items[1]);

				comp = -1 * Integer.compare(valueKey1, valueKey2);
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
			String naturalKey1 = t1.toString().split(":")[0];
			String naturalKey2 = t2.toString().split(":")[0];
			int comp = naturalKey1.compareTo(naturalKey2);

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
		facebook:google:1	null
		facebook:youtube:2 null
		google:facebook:1	null
		google:youtube:1	null
		youtube:facebook:2	null
		youtube:google:1	null

	output:
		facebook:1	google 1
		facebook:2	youtube 2
		google:1	facebook 1
		google:1	youtube 1
		youtube:2	facebook 2
		youtube:1 google 1
	*/
	public static class FourthStepMapper extends
	Mapper<LongWritable, Text, Text, Text> {


		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

			String[] parts = value.toString().split(":");
			context.write(new Text(parts[0] + ":" + parts[2]), new Text(parts[1] + " " + parts[2]));
		}
	}

	/*
	input:
		facebook:2	["youtube 2", "google 1"]
		google:1	["facebook 1", "youtube 1"]
		youtube:2	["facebook 2", "google 1"]

	output:
		facebook youtube 2
		facebook google 1
		google facebook 1
		google youtube 1
		youtube facebook 2
		youtube google 1
	*/
	public static class FourthStepReducer extends
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
		Job job = Job.getInstance(conf, "FourthStep");
		job.setJarByClass(FourthStep.class);
		job.setReducerClass(FourthStepReducer.class);
		job.setMapperClass(FourthStepMapper.class);
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
