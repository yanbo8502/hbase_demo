package hbase_mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyConditionCount {
	
	public static void main(String[] args){	
		for(String arg:args)
		{
			System.out.println(arg);
		}
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum","10.183.93.130,10.183.93.127,10.183.93.128");  
        config.set("hbase.zookeeper.property.clientPort", "21818"); 
        config.set("hbase.client.scanner.timeout.period", "600000"); 
        
   	    String[] otherArgs;
   	    String input_table = "";
   	    String output_filepath = "";
   		try {
   			otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
   			System.out.println("input table is " + otherArgs[0] );
   			System.out.println("output filepath is " + otherArgs[1] );
   			input_table =  otherArgs[0];
   			output_filepath = otherArgs[1];
   		} catch (IOException e1) {
   			// TODO Auto-generated catch block
   			e1.printStackTrace();
   		}
   		
		Job job;
		try {
			job = Job.getInstance(config, "MyConditionCount");
			job.setJarByClass(MyConditionCount.class);     // class that contains mapper
			
			Scan scan = new Scan();
			scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  // don't set to true for MR jobs
			// set other scan attrs...
			scan.addColumn(Bytes.toBytes("b"), Bytes.toBytes("tv_expire_time"));
			TableMapReduceUtil.initTableMapperJob(
			input_table,        // input HBase table name
			  scan,             // Scan instance to control CF and attribute selection
			  MyMapper.class,   // mapper
			  Text.class,             // mapper output key 
			  IntWritable.class,             // mapper output value
			  job);

			job.setReducerClass(MyReducer.class);    // reducer class
			job.setNumReduceTasks(1);    // at least one, adjust as required
			FileOutputFormat.setOutputPath(job, new Path(output_filepath));  // adjust directories as required	    
			boolean b = job.waitForCompletion(true);
			if (!b) {
			  throw new IOException("error with job!");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static class MyMapper extends TableMapper<Text, IntWritable>  {

		private final IntWritable ONE = new IntWritable(1);
	   	private Text text = new Text();

	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
	        	String val = new String(value.getValue(Bytes.toBytes("b"), Bytes.toBytes("tv_expire_time")));
	        	if(val.compareTo("2017-02-29") == 1)
	          	{ 
	        		text.set("default");     // we can only emit Writables...
	        		context.write(text, ONE);
	          	}
	   	}
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int i = 0;
			for (IntWritable val : values) {
				i += val.get();
			}
			context.write(key, new IntWritable(i));
		}
	}
	  

}
