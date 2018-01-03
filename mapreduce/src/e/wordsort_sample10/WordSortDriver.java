package e.wordsort_sample10;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordSortDriver extends Configured implements Tool {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public int run(String[] args) throws Exception {
		String input, output;
		if (args.length == 2) {
			input = args[0];
			output = args[1];
		} else {
			System.err.println("Incorrect number of arguments.  Expected: input output");
			return -1;
		}
		
		String jobName = "WordSort";
		String mapJobName = jobName + "-Map";
		String reduceJobName = jobName + "-Reduce";

		Path mapInputPath = new Path(input);
		Path mapOutputPath = new Path(output + "-inter");
		Path reduceOutputPath = new Path(output);

		Path partitionPath = new Path(output + "-part.lst");

		@SuppressWarnings("deprecation")
		Job mapJob = new Job(getConf());
		mapJob.setJobName(mapJobName);
		mapJob.setJarByClass(WordSortDriver.class);
		mapJob.setMapperClass(WordSortMapper.class);
		mapJob.setNumReduceTasks(0);
		mapJob.setOutputKeyClass(Text.class);
		mapJob.setOutputValueClass(NullWritable.class);
		TextInputFormat.setInputPaths(mapJob, mapInputPath);

		mapJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(mapJob, mapOutputPath);

		int exitCode = mapJob.waitForCompletion(true) ? 0 : 1;
		if (exitCode != 0) {
			return exitCode;
		}

		@SuppressWarnings("deprecation")
		Job reduceJob = new Job(getConf());
		reduceJob.setJobName(reduceJobName);
		reduceJob.setJarByClass(WordSortDriver.class);

		reduceJob.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(reduceJob, mapOutputPath);

		TextOutputFormat.setOutputPath(reduceJob, reduceOutputPath);

		reduceJob.setReducerClass(WordSortReducer.class);
		reduceJob.setMapOutputKeyClass(Text.class);
		reduceJob.setMapOutputValueClass(NullWritable.class);
		reduceJob.setOutputKeyClass(Text.class);
		reduceJob.setOutputValueClass(NullWritable.class);
		reduceJob.setNumReduceTasks(10);

		reduceJob.setPartitionerClass(TotalOrderPartitioner.class);

		TotalOrderPartitioner.setPartitionFile(reduceJob.getConfiguration(),
				partitionPath);
		InputSampler.writePartitionFile(reduceJob,new InputSampler.SplitSampler(10));

		return reduceJob.waitForCompletion(true) ? 0 : 2;

	}

	public static void main(String[] args) throws Exception {
		WordSortDriver driver = new WordSortDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}