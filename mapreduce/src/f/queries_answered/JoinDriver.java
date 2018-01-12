package f.queries_answered;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class JoinDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String input, output, stoplist, titles;
		if (args.length == 4) {
			input = args[0];
			stoplist = args[1];
			titles = args[2];
			output = args[3];
		} else {
			System.err.println("Incorrect number of arguments.  Expected: input output");
			return -1;
		}


		Configuration conf = new Configuration();

	    Job job = Job.getInstance(conf, "joindriver");
	    
		job.setJarByClass(JoinDriver.class);
		job.setJobName(this.getClass().getName());
	    
		MultipleInputs.addInputPath(job,new Path(input),TextInputFormat.class,QueriesMapper.class);
		MultipleInputs.addInputPath(job,new Path(stoplist),TextInputFormat.class,StopListMapper.class);
		MultipleInputs.addInputPath(job,new Path(titles),TextInputFormat.class,TitlesMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setReducerClass(JoinReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		boolean success = job.waitForCompletion(true);
		
		Counter someCount = job.getCounters().findCounter(CustomCounter.Total);
		
		QueriesAnswered.queryCounter = someCount;
		
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		JoinDriver driver = new JoinDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}