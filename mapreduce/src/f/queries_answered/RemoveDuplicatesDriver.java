package f.queries_answered;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RemoveDuplicatesDriver  extends Configured implements Tool {

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

		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(RemoveDuplicatesDriver.class);
		job.setJobName(this.getClass().getName());
		FileInputFormat.setInputPaths(job, new Path(input));

		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setReducerClass(RemoveDuplicatesReducer.class);
		job.setMapperClass(RemoveDuplicatesMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		boolean success = job.waitForCompletion(true);
		
		Counter someCount = job.getCounters().findCounter(CustomCounter.Total);
		
		QueriesAnswered.queryCounter = someCount;
		
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		RemoveDuplicatesDriver driver = new RemoveDuplicatesDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}