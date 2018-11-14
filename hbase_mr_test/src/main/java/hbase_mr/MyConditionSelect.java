package hbase_mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import com.yanbo.hbase.util.HBaseTableOperation;

public class MyConditionSelect {
	
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
   	    String output_table = "";
   		try {
   			otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
   			System.out.println("input table is " + otherArgs[0] );
   			System.out.println("output table is " + otherArgs[1] );
   			input_table =  otherArgs[0];
   			output_table = otherArgs[1];
   		} catch (IOException e1) {
   			// TODO Auto-generated catch block
   			e1.printStackTrace();
   		}
   		
		Job job;
		try {
			job = Job.getInstance(config, "MyConditionSelect");
			job.setJarByClass(MyConditionSelect.class);     // class that contains mapper
			
			Scan scan = new Scan();
			scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  // don't set to true for MR jobs
			// set other scan attrs...

			HBaseTableOperation.createTableFrom(output_table, input_table);
			
			TableMapReduceUtil.initTableMapperJob(
			  input_table,        // input HBase table name
			  scan,             // Scan instance to control CF and attribute selection
			  MyMapper.class,   // mapper
			  ImmutableBytesWritable.class,             // mapper output key 
			  Put.class,             // mapper output value
			  job);
			
			
			TableMapReduceUtil.initTableReducerJob(
					output_table,      // output table
					  null,             // reducer class
					  job);
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
	
	public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put>  {

		  public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			     String val = new String(value.getValue(Bytes.toBytes("b"), Bytes.toBytes("tv_expire_time")));
	        	if(val.compareTo("2017-02-29") == 1)
	        	{
	        		context.write(row, resultToPut(row,value));
	        	}
		      
		    }

		    private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
		      Put put = new Put(key.get());
		      for (KeyValue kv : result.raw()) {
		        put.add(kv);
		      }
		      return put;
		    }
		}
	  

}
