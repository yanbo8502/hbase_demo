package hbase_mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import com.yanbo.hbase.util.HBaseTableOperation;


public class MyConditionReadWriteJob {
	
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
			job = Job.getInstance(config, "MyConditionReadWriteJob");
			job.setJarByClass(MyConditionReadWriteJob.class);     // class that contains mapper
			
			Scan scan = new Scan();
			scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  // don't set to true for MR jobs
			// set other scan attrs...
			
			SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("audiences"), Bytes.toBytes("05f8b534e090480899355a4b0b04ca58"), CompareOp.EQUAL, Bytes.toBytes("20170321"));
			SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("audiences"), Bytes.toBytes("2f39fca9cacb4da084d72e23954e8cb9"), CompareOp.EQUAL, Bytes.toBytes("20170321"));
			SingleColumnValueFilter filter3 = new SingleColumnValueFilter(Bytes.toBytes("audiences"), Bytes.toBytes("66109775faad42428ca9b73102533328"), CompareOp.EQUAL, Bytes.toBytes("20170321"));
			SingleColumnValueFilter filter4 = new SingleColumnValueFilter(Bytes.toBytes("audiences"), Bytes.toBytes("db87c6b7fd054e6e959c520279d5804f"), CompareOp.EQUAL, Bytes.toBytes("20170321"));
			ArrayList<Filter> andListForFilters = new ArrayList<Filter>();
			ArrayList<Filter> orListForFilters = new ArrayList<Filter>();
			ArrayList<Filter> allListForFilters = new ArrayList<Filter>();
			andListForFilters.add(filter1);
			andListForFilters.add(filter2);
			orListForFilters.add(filter3);
			orListForFilters.add(filter4);
			// 通过将operator参数设置为Operator.MUST_PASS_ONE,达到list中各filter为"或"的关系
	        	// 默认operator参数的值为Operator.MUST_PASS_ALL,即list中各filter为"并"的关系
	        Filter orFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, orListForFilters);
	        Filter andFilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, andListForFilters);
	        allListForFilters.add(orFilterList);
	        allListForFilters.add(andFilterList);
	        Filter allFilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, allListForFilters);
			scan.setFilter(allFilterList);
			
			// set other scan attrs...
			scan.addColumn(Bytes.toBytes("audiences"), Bytes.toBytes("05f8b534e090480899355a4b0b04ca58"));
			scan.addColumn(Bytes.toBytes("audiences"), Bytes.toBytes("2f39fca9cacb4da084d72e23954e8cb9"));
			scan.addColumn(Bytes.toBytes("audiences"), Bytes.toBytes("66109775faad42428ca9b73102533328"));
			scan.addColumn(Bytes.toBytes("audiences"), Bytes.toBytes("db87c6b7fd054e6e959c520279d5804f"));
			List<String> families = new ArrayList<String>();
			families.add("audiences");
			HBaseTableOperation.createTable(output_table, families);
			
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
		    // this example is just copying the data from the source table...
			  
			  Put put = new Put(row.get());
			  put.addColumn(Bytes.toBytes("audiences"),Bytes.toBytes("00001"), Bytes.toBytes("20170321"));
		      context.write(row, put);
		    }
		}

}
