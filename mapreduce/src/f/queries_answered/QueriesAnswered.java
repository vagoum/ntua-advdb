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
		
		String intermediatePathQueries = new String("./tmp1");
		String arr[] = new String[3];
		arr[0] = queries;
		arr[1] = stop;
		arr[2] = intermediatePathQueries;
		
		QueriesDriver queriesDriver = new QueriesDriver();
		queriesDriver.run(arr);
		
		String intermediatePathIds = new String("./tmp3");
		String arr2[] = new String[3];
		arr2[0] = titles;
		arr2[1] = intermediatePathQueries;
		arr2[2] = intermediatePathIds;
		
		JoinDriver joinDriver = new JoinDriver();
		joinDriver.run(arr2);
		
		String arr3[] = new String[2];
		arr3[0] = intermediatePathIds;
		arr3[1] = output;
		
		CountDriver countDriver = new CountDriver();
		countDriver.run(arr3);
	}

}
