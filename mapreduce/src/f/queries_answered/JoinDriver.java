package f.queries_answered;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import e.wordsort_sample1000.WordSortDriver;

public class JoinDriver extends Configured implements Tool {


	@Override
	public int run(String[] args) throws Exception {
		String titles, output, queries;
		if (args.length == 3) {
			titles = args[0];
			queries = args[1];
			output = args[2];
		} else {
			System.err.println("Incorrect number of arguments.  Expected: input output");
			return -1;
		}

		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(JoinDriver.class);
		job.setJobName(this.getClass().getName());

		FileInputFormat.addInputPaths(job, titles + ","+ queries);
		job.setMapperClass(JoinMapper.class);
		String interOutput = new String("./tmp4");
		FileOutputFormat.setOutputPath(job, new Path(interOutput));

		job.setReducerClass(JoinReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);

		boolean success = job.waitForCompletion(true);
		
		
		@SuppressWarnings("deprecation")
		Job reduceJob = new Job();
		reduceJob.setJobName("JoinReducerExtra");
		reduceJob.setJarByClass(WordSortDriver.class);

		FileInputFormat.setInputPaths(reduceJob, interOutput);

		TextOutputFormat.setOutputPath(reduceJob, new Path(output));

		reduceJob.setMapperClass(JoinDupMapper.class);
		reduceJob.setReducerClass(JoinDupReducer.class);
		reduceJob.setMapOutputKeyClass(LongWritable.class);
		reduceJob.setMapOutputValueClass(NullWritable.class);
		reduceJob.setOutputKeyClass(LongWritable.class);
		reduceJob.setOutputValueClass(NullWritable.class);		
		
		success = reduceJob.waitForCompletion(true);

		return 1;
	}

	public static void main(String[] args) throws Exception {
		JoinDriver driver = new JoinDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}