package hbase;

import com.yanbo.hbase.util.HBaseConnectionProviderImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class Main {
	private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static Configuration config=null;  
    static{  
        config= HBaseConfiguration.create();
		String zkhosts = "bxzj-test-swift0.bxzj.baixinlocal.com,bxzj-test-swift1.bxzj.baixinlocal.com,bxzj-test-swift2.bxzj.baixinlocal.com";
		String zkport = "2181";
		//conf.addResource("hbase-site.xml");
		config.set("hbase.zookeeper.quorum",zkhosts);
		config.set("hbase.zookeeper.property.clientPort",zkport);
		config.set("zookeeper.znode.parent","/hbase-unsecure");
		config.set("hbase.user.name","testG");
        
    }  
    
	public static void main(String[] args) throws Exception {
		logger.debug("test log");
		//HBaseTableOperation.test(args);
		Date start = new Date();
		//test_scan();
		//test_second_index();
		//testBatchSecondaryIndex();
		testBatchSecondaryIndexMT();
		System.out.println("done!");
		Date stop = new Date();
		System.out.println((stop.getTime() - start.getTime())/60000 + "  minutes");
				 
    }

	private static void test_scan() throws IOException {
		System.out.println("connect Hbase ...");
		 ExecutorService pool = Executors.newFixedThreadPool(10);//建立一个数量为10的线程池 
		 Connection connection = ConnectionFactory.createConnection(config,pool);
		 System.out.println("connected!");
		    try {
				String table_name = "testG:urm_audience";
				Table table = connection.getTable(TableName.valueOf(table_name));
		      System.out.println("get table:" + table.getName());
		      try {
		        		       
		        // Sometimes, you won't know the row you're looking for. In this case, you
		        // use a Scanner. This will give you cursor-like interface to the contents
		        // of the table.  To set up a Scanner, do like you did above making a Put
		        // and a Get, create a Scan.  Adorn it with column names, etc.
		        Scan s = new Scan();
		        s.setCaching(1000); 
		        s.setCacheBlocks(false); 
		        s.addColumn(Bytes.toBytes("audiences"), null);
		        ResultScanner scanner = table.getScanner(s);
		        try {
		           // Scanners return Result instances.
		           // Now, for the actual iteration. One way is to use a while loop like so:
		           for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
						System.out.println(rr.toString());
		           }

		         } catch (IOException e) {
		 			// TODO Auto-generated catch block
		 			e.printStackTrace();
		 		}  finally {
		           // Make sure you close your scanners when you are done!
		           // Thats why we have it inside a try/finally clause
		           scanner.close();
		         }

		         // Close your table and cluster connection.
		       } catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}  finally {
		         if (table != null) table.close();
		       }
		     }catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
		       connection.close();
		       pool.shutdown();
		     }
	}

	private static void test_second_index() throws IOException {
		System.out.println("test_second_index ...");
		Connection connection = null;
		/*ExecutorService pool = Executors.newFixedThreadPool(10);//建立一个数量为10的线程池
	 	Connection connection = ConnectionFactory.createConnection(config,pool);
	 */

		try {
			HBaseConnectionProviderImpl impl = new HBaseConnectionProviderImpl(config);
			connection = impl.provideConnection();
			System.out.println("connected!");
			String data_table_name = "testG:data_table";
			String index_table_name = "testG:index_table";
			Table data_table = connection.getTable(TableName.valueOf(data_table_name));
			Table index_table = connection.getTable(TableName.valueOf(index_table_name));
			System.out.println("get table:" + data_table.getName());
			try {
					Put put = new Put("123_c123_13810750000".getBytes());
					put.addColumn("f1".getBytes(), "id".getBytes(), "123".getBytes());
					put.addColumn("f1".getBytes(), "cid".getBytes(), "c123".getBytes());
					put.addColumn("f1".getBytes(), "phone".getBytes(), "13810750000".getBytes());
					put.addColumn("f2".getBytes(), "data".getBytes(), "qweasdzxc".getBytes());
					data_table.put(put);
					System.out.println(" put done!");

				//testSecondaryIndexCoprocessorLogoic(data_table, index_table);
				testSecondaryIndexCoprocessorResult(data_table, index_table);

				// Close your table and cluster connection.
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  finally {
				if (data_table != null) data_table.close();
				if(impl!=null) impl.release();
			}
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(connection!=null)
			{
				connection.close();
			}
			//pool.shutdown();
		}
	}


	/**
	 * 本函数用于测试二级索引coprocessr的实现逻辑，试验成功后单独编写coprocessor实现
	 * @param data_table
	 * @param index_table
	 * @throws IOException
	 */
	private static void testSecondaryIndexCoprocessorLogoic(Table data_table, Table index_table) throws IOException {
		Get get = new Get("123".getBytes());
		Result result1 = index_table.get(get);
		System.out.println(" get index done!");

		byte[] realRowkey = result1.getValue("f1".getBytes(), "rowKey".getBytes());

		System.out.println("get real rowkey:" + Bytes.toString(realRowkey));

		Get realget = new Get(realRowkey);
		Result result = data_table.get(realget);
		System.out.println(" get data done!");
		System.out.println(Bytes.toString(result.getValue("f2".getBytes(), "data".getBytes())));
		List<Cell> cells = result.listCells();

		for (Cell cell : cells) {
            System.out.println(Bytes.toString(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength()));

    }

		Cell rowkeycell = result1.listCells().get(0);

		realRowkey = Bytes.copy(rowkeycell.getValueArray(), rowkeycell.getValueOffset(), rowkeycell.getValueLength());
		System.out.println("get real rowkey:" + Bytes.toString(realRowkey));

		realget = new Get(realRowkey);
		result = data_table.get(realget);
		System.out.println(" get data done!");
		cells = new ArrayList<Cell>();
		cells.addAll(result.listCells());
		cells.addAll(result1.listCells());

		for (Cell cell : cells) {
            System.out.println(Bytes.toString(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength()));
        }
	}

	/**
	 * 本函数用于测试二级索引coprocessr的效果，看看已经添加二级索引应用的索引表是否实现了索引查询的功能
	 * @param data_table
	 * @param index_table
	 * @throws IOException
	 */
	private static void testSecondaryIndexCoprocessorResult(Table data_table, Table index_table) throws IOException {
		Get get = new Get("123".getBytes());
		Result result1 = index_table.get(get);
		System.out.println(" get data done!");

		List<Cell> cells = result1.listCells();

		for (Cell cell : cells) {
			System.out.println(Bytes.toString(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength()));

		}

	}

	private static void testBatchSecondaryIndex() throws IOException {
		System.out.println("batch test_second_index ...");
		Connection connection = null;

		try {
			HBaseConnectionProviderImpl impl = new HBaseConnectionProviderImpl(config);
			connection = impl.provideConnection();
			System.out.println("connected!");
			String data_table_name = "testG:data_table";

			Table data_table = connection.getTable(TableName.valueOf(data_table_name));

			System.out.println("get table:" + data_table.getName());
			Date date = new Date();
			long start = date.getTime();
			int max_size =1000000;
			for(Integer i=5001;i<max_size;i++)
			{

				try {
					String phone = String.format("13%09d", i);
					String cid = String.format("c%d", i);
					String id = i.toString();
					String rowKey = String.format("%s_%s_%s", id,cid,phone);
					//System.out.println(rowKey);
					Put put = new Put(rowKey.getBytes());
					put.addColumn("f1".getBytes(), "id".getBytes(), id.getBytes());
					put.addColumn("f1".getBytes(), "cid".getBytes(), cid.getBytes());
					put.addColumn("f1".getBytes(), "phone".getBytes(), phone.getBytes());
					put.addColumn("f2".getBytes(), "data".getBytes(), i.toString().getBytes());
					data_table.put(put);

					// Close your table and cluster connection.
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}  finally {
				}

			}
			long end = date.getTime();
			long duration = end - start;

			System.out.println("All puts done!");
			System.out.println(String.format("cost totlaly %d s", duration));
			if (data_table != null) data_table.close();
			if(impl!=null) impl.release();
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(connection!=null)
			{
				connection.close();
			}
		}
	}

	private static void testBatchSecondaryIndexMT() throws IOException {
		System.out.println("batch test_second_index ...");
		Connection connection = null;
		ExecutorService pool = Executors.newFixedThreadPool(10);//建立一个数量为10的线程池

		try {
			HBaseConnectionProviderImpl impl = new HBaseConnectionProviderImpl(config);
			connection = impl.provideConnection();
			System.out.println("connected!");
			String data_table_name = "testG:data_table";

			Table data_table = connection.getTable(TableName.valueOf(data_table_name));

			System.out.println("get table:" + data_table.getName());
			Date date = new Date();
			long start = date.getTime();
			long total_size =1000;
			int parallize_num = 10;
			Long part_size = total_size/ parallize_num;
			long start_point = 1000001;
			long total_end = total_size + start_point;
			List<Future> tresults = new ArrayList<>();
			for(int i=0;i<parallize_num;i++)
			{
				long part_start = part_size*i+start_point;
				long part_end = part_size*(i+1)+start_point;
				part_end = part_end>total_end?total_size:part_end;
				Future f = pool.submit(new DataIO(part_start, part_end, data_table));
				tresults.add(f);


			}

			for(Future f : tresults)
			{
				f.get();
			}
			date = new Date();
			long end = date.getTime();
			double duration = (end - start)/1000;

			System.out.println("All puts done!");
			System.out.println(String.format("cost totlaly %d s", duration));
			if (data_table != null) data_table.close();
			if(impl!=null) impl.release();
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(connection!=null)
			{
				connection.close();
			}
		}
	}

}

