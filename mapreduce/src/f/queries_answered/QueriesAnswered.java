package f.queries_answered;

import org.apache.hadoop.mapreduce.Counter;

public class QueriesAnswered {

	public static Counter queryCounter;
	
	public static void main(String[] args) throws Exception {
		String queries = null, titles = null, output = null, stop = null;
		if (args.length == 4) {
			queries = args[0];
			titles = args[1];
			stop = args[2];
			output = args[3];
		} else {
			System.err.println("Incorrect number of arguments.  Expected: input output");
			System.exit(-1);
		}
		
		String joinPath = new String("./joinPath");
		String arr[] = new String[4];
		arr[0] = queries;
		arr[1] = stop;
		arr[2] = titles;
		arr[3] = joinPath;
				
		JoinDriver joinDriver = new JoinDriver();
		joinDriver.run(arr);
		
		String qidsFile = new String("./qidsFile");
		String arr1[] = new String[2];
		arr1[0] = joinPath;
		arr1[1] = qidsFile;
		
		RemoveDuplicatesDriver rduplicatesDriver = new RemoveDuplicatesDriver();
		rduplicatesDriver.run(arr1);
		
	}

}
