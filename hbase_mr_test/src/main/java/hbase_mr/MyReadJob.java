package hbase_mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import  org.apache.hadoop.hbase.*;
import org.apache.hadoop.io.Text;

public class MyReadJob{
	
	public static void main(String[] args){	
		for(String arg:args)
		{
			System.out.println(arg);
		}
		
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum","10.183.93.130,10.183.93.127,10.183.93.128");  
        config.set("hbase.zookeeper.property.clientPort", "21818"); 
        //config.set("hbase.client.pause", "50"); 
        //config.set("hbase.client.retries.number", "3"); 
        //config.set("hbase.rpc.timeout", "600000"); 
        //config.set("hbase.client.operation.timeout", "600000"); 
        config.set("hbase.client.scanner.timeout.period", "600000"); 
        
	    String[] otherArgs;
	    String input_table = "";
		try {
			otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
			System.out.println("input table is " + otherArgs[0] );
			input_table =  otherArgs[0];
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	   
	    
		Job job;
		try {
			job = Job.getInstance(config, "MyReadJob");
			job.setJarByClass(MyReadJob.class);     // class that contains mapper
			
			Scan scan = new Scan();
			scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  // don't set to true for MR jobs
			// set other scan attrs...
			TableMapReduceUtil.initTableMapperJob(
					input_table,        // input HBase table name
					  scan,             // Scan instance to control CF and attribute selection
					  MyMapper.class,   // mapper
					  null,             // mapper output key
					  null,             // mapper output value
					  job);
			job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper

			job.setNumReduceTasks(0);	    
			boolean b = job.waitForCompletion(true);
			if (!b) {
			  throw new IOException("error with job!");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static class MyMapper extends TableMapper<Text, Text> {
			
		  public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		    // process data for the row from the Result instance.
		   }
		}
}
