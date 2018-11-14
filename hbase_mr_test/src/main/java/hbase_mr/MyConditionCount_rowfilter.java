package hbase_mr;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
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

public class MyConditionCount_rowfilter {
	
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
			job = Job.getInstance(config, "MyConditionCount_rowfilter");
			job.setJarByClass(MyConditionCount_rowfilter.class);     // class that contains mapper
			
			Scan scan = new Scan();
			scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  // don't set to true for MR jobs
			
			// set other scan attrs...
			
			scan.addColumn(Bytes.toBytes("base"), Bytes.toBytes("max_mobile_pay_time"));
			scan.addColumn(Bytes.toBytes("behaviors"), Bytes.toBytes("15_pid"));
			scan.addColumn(Bytes.toBytes("behaviors"), Bytes.toBytes("30_pid"));
			scan.addColumn(Bytes.toBytes("behaviors"), Bytes.toBytes("60_pid"));
			scan.addColumn(Bytes.toBytes("behaviors"), Bytes.toBytes("7_pid"));
			//Filter rf = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("row1") ) ); 
			Filter rpf = new PrefixFilter(Bytes.toBytes("a02"));			
 
	       scan.setFilter(rpf);
	        
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
		   		
		   		text.set("default");     // we can only emit Writables...
	    		context.write(text, ONE);
	   			/**/
	        	String val1 = getValue(value, "behaviors", "7_pid", context);
	        	String val2 = getValue(value, "behaviors", "15_pid", context);
	        	String val3 = getValue(value, "behaviors", "30_pid", context);
	        	String val4 = getValue(value, "behaviors", "60_pid", context);
	        	if(val1.contains("10001"))	        	
	        	{
	        		context.write(new Text("in7_pid"), ONE);
	        	}
	        	if(val1.contains("10001"))	        	
	        	{
	        		context.write(new Text("in15_pid"), ONE);
	        	}
	        	if(val1.contains("10001"))	        	
	        	{
	        		context.write(new Text("in30_pid"), ONE);
	        	}
	        	if(val1.contains("10001"))	        	
	        	{
	        		context.write(new Text("in60_pid"), ONE);
	        	}
	        		        	        		
	   	}
	   	
	   	public String getValue(Result value, String family, String qualifier, Context context) throws IOException, InterruptedException
	   	{
	   		String val = "";
	   		if(value == null)
	   		{
	   			text.set("result_value_null");     // we can only emit Writables...
        		context.write(text, ONE);
        		return val;
	   		}
	   		try
	   		{
	   			byte[] cell_value = value.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
	   			if(null == cell_value)
	   			{
		   			text.set("cell_value_null");     // we can only emit Writables...
	        		context.write(text, ONE);
	        		return val;
	   			}
				val = new String(cell_value);
				text.set(qualifier);     // we can only emit Writables...
        		context.write(text, ONE);
        		
        		context.write(new Text(val), ONE);
	   		}
	   		catch(Exception e)
	   		{
	   			String exception_name = "exception";
	   			text.set(exception_name);     // we can only emit Writables...
        		context.write(text, ONE);
	   		}
	   		return val;
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
