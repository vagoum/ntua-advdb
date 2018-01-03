package d.commonwords;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExtractWordsDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String input, output, stoplist;
		if (args.length == 3) {
			input = args[0];
			stoplist = args[1];
			output = args[2];
		} else {
			System.err.println("Incorrect number of arguments.  Expected: input output");
			return -1;
		}

		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(ExtractWordsDriver.class);
		job.setJobName(this.getClass().getName());
		MultipleInputs.addInputPath(job,new Path(input),TextInputFormat.class,ExtractWordsMapper.class);
		MultipleInputs.addInputPath(job,new Path(stoplist),TextInputFormat.class,StopListMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setReducerClass(ExtractWordsReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ExtractWordsDriver driver = new ExtractWordsDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}