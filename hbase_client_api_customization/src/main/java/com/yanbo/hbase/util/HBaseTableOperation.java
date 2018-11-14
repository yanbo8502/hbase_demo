package com.yanbo.hbase.util;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class HBaseTableOperation implements Closeable  {
	
	private static final Logger logger = LoggerFactory.getLogger(HBaseTableOperation.class);
	private Table table;
	private String _table_name;
	public HBaseTableOperation(String table_name)
	{
		_table_name = table_name;
		com.yanbo.hbase.util.HBaseClient client = HBaseClient.getInstance();
		try {
			table = client.getHTable(_table_name);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("hbase 获取table失败: "+ e.getMessage());
		} 
		finally{						
		}
	}
	
	public String getTableName()
	{
		if(table!=null)
		{
			return table.getName().toString();
		}
		return _table_name;
	}
	
	public void close() throws IOException {
		HBaseClient.relaseHTable(table);		
	}

	  /** 
     * get方式，通过rowKey、列名column （包含列族（column family）和单个列（column qualifier）） 确定某行某个cell的值 
     * @param tablename 
     * @param rowKey 
     * @param column
     * @throws IOException 
     */  
    public  String getRowCellValue(String rowKey, HBaseColumn column)
    {    
    	String value = "";
		try {
				String family = column.getFamily();
				String qualifier = column.getQualifier();
		        Get g = new Get(Bytes.toBytes(rowKey));  
		        byte[] family_bytes =  null != family ? Bytes.toBytes(family) : null ;	        
				byte[] qualifier_bytes = null != qualifier ? Bytes.toBytes(qualifier) : null;
		        Result r = table.get(g);
		        byte[] value_bytes= r.getValue(family_bytes, qualifier_bytes);		        
		        value = Bytes.toString(value_bytes);		        					        
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("hbase 获取table cell value失败: "+ e.getMessage());		
		}
		
		return value;
    } 
	
	  /** 
     * get方式，通过rowKey、列族（column family）和单个列（column qualifier）确定一行的若干cells，遍历 
     * @param tablename 
     * @param rowKey 
     * @param column
     * @throws IOException 
     */  
    public  HashMap<String, String> fetchRowCellValues(String rowKey, List<HBaseColumn> columns)
    {    
    	HashMap<String, String> values = new  HashMap<String, String>();
		try {
			for(HBaseColumn item:columns)
			{
				String family = item.getFamily();
				String qualifier = item.getQualifier();
		        Get g = new Get(Bytes.toBytes(rowKey));  
		        byte[] family_bytes =  null != family ? Bytes.toBytes(family) : null ;	        
				byte[] qualifier_bytes = null != qualifier ? Bytes.toBytes(qualifier) : null;
				g.addColumn(family_bytes, qualifier_bytes);  
		        Result r = table.get(g);  
		        for(Cell cell:r.rawCells()){  
		            String column = family;
		            if(qualifier!=null&& qualifier!="")
		            {
		            	column +=":" + qualifier;
		            }
		            values.put(column, Bytes.toString(CellUtil.cloneValue(cell)));		            
		        }  		        				
			}
	        
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("hbase 获取table cell values失败: "+ e.getMessage());		
		}
		
		return values;

    } 
    
    /** 
 	* 通过rowKey查询某一行的指定若干cell
	* @param rowKey  要查询行的行健
	* @param columns 需要返回的列定义的集合（包含列族column family和单个列column qualifier）
	* return value:
	* List<Cell> 返回输入列对应的cell集合
	*/ 
    public  List<Cell> fetchRowCells(String rowKey,  List<HBaseColumn> columns)
    {    
    	List<Cell> cells = new ArrayList<Cell>();
		try {
			for(HBaseColumn item:columns)
			{
				String family = item.getFamily();
				String qualifier = item.getQualifier();
		        Get g = new Get(Bytes.toBytes(rowKey));  
		        byte[] family_bytes =  null != family ? Bytes.toBytes(family) : null ;	        
				byte[] qualifier_bytes = null != qualifier ? Bytes.toBytes(qualifier) : null;
				g.addColumn(family_bytes, qualifier_bytes);  
		        Result r = table.get(g);  
		        for(Cell cell:r.rawCells()){ 		        	
		        	cells.add(cell);	            
		        }  		        				
			}
	        
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("hbase 获取table cells失败: "+ e.getMessage());		
		}
		
		return cells;

    } 
    
    /** 
     	* 通过rowKey查询某一行，返回行实例Result对象
	* @param rowKey  要查询行的行健
	* @param columns 需要返回的列定义的集合（包含列族column family和单个列column qualifier）
	* return value:
	* HashMap<String, String> 返回列值的集合HashMap，key是的格式是family:qualifier，value是列值
	*/ 
    public  Result fetchRow(String rowKey,  List<HBaseColumn> columns)
    {    
    	Result row = null;
		try {
			for(HBaseColumn item:columns)
			{
				String family = item.getFamily();
				String qualifier = item.getQualifier();  
		        byte[] family_bytes =  null != family ? Bytes.toBytes(family) : null ;	        
				byte[] qualifier_bytes = null != qualifier ? Bytes.toBytes(qualifier) : null;
				Get g = new Get(Bytes.toBytes(rowKey));  
				g.addColumn(family_bytes, qualifier_bytes);  
		        row = table.get(g);  		      			
			}
	        
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("hbase 获取table row失败: "+ e.getMessage());		
		}
		
		return row;

    } 
    
    /** 
           *通过rowKey查询某一行，返回需要的列对应的值
     * @param rowKey  要查询行的行健
     * @param columns 需要返回的列定义的集合（包含列族column family和单个列column qualifier）
     * return value:
     * HashMap<String, String> 返回列值的集合HashMap，key是的格式是family:qualifier，value是列值
     */  
    
    public  HashMap<String, String> fetchCellValues2(String rowKey, List<HBaseColumn> columns)
    {    
    	HashMap<String, String> values = new  HashMap<String, String>();
		try {
			for(HBaseColumn item:columns)
			{
				String family = item.getFamily();
				String qualifier = item.getQualifier();
		        Get g = new Get(Bytes.toBytes(rowKey));  
		        byte[] family_bytes =  null != family ? Bytes.toBytes(family) : null ;	        
				byte[] qualifier_bytes = null != qualifier ? Bytes.toBytes(qualifier) : null;
				g.addColumn(family_bytes, qualifier_bytes);  
		        Result r = table.get(g); 
		        CellScanner scan = r.cellScanner();
		        while(scan.advance()){ 	        	
		        	Cell cell = scan.current();   
		            String column = family;
		            if(qualifier!=null&& qualifier!="")
		            {
		            	column +=":" + qualifier;
		            }
		            values.put(column, Bytes.toString(CellUtil.cloneValue(cell)));		            
		        }  
		        				
			}
	        
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("hbase 获取table cells失败: "+ e.getMessage());		
		}
		
		return values;

    } 
    
    /** 
        * 输入rowKey的范围以及需要的列，返回行的集合
	* @param columns 需要提取的列 
	* @param start_row 起点rowKey
	* @param stop_row  终点rowKey
	* @throws IOException 
	* return value:
	* List<Result>  hbase行（result）集合的列表, 如果符合条件的行太多可能会影响性能
	*/ 
    public List<Result> fetchRows( List<HBaseColumn> columns, String start_row, String stop_row)
    {
    	List<Result> results = new ArrayList<Result>();
    	Scan s = new Scan();
		for(HBaseColumn item:columns)
		{
			String family = item.getFamily();
			String qualifier = item.getQualifier();
			byte[] family_bytes =  null != family ? Bytes.toBytes(family) : null ;	        
			byte[] qualifier_bytes = null != qualifier ? Bytes.toBytes(qualifier) : null;
			s.addColumn(family_bytes,qualifier_bytes);		        
		}
    	
    	byte[] stop_row_bytes =  null != stop_row ? Bytes.toBytes(stop_row) : null ;	        
		byte[] start_row_bytes = null != start_row ? Bytes.toBytes(start_row) : null;
		s.setStopRow(stop_row_bytes).setStartRow(start_row_bytes);
			  
	    ResultScanner scanner = null;
	    try {
	           scanner = table.getScanner(s);
	           // Scanners return Result instances.
	           // Now, for the actual iteration. One way is to use a while loop like so:
	           for (Result rr :scanner) {
	             // print out the row we found and the columns we were looking for
	        	   results.add(rr);             
	           }
	     }catch (Exception e) {
	    	 logger.error("hbase 获取table rows失败: "+ e.getMessage());	
	 	 }finally {
	           // Make sure you close your scanners when you are done!
	           // Thats why we have it inside a try/finally clause
	       scanner.close();
	     }
	    
	    return results;
	} 
    /** 
          * 输入rowKey的范围以及需要的列，返回行的集合查询游标ResultScanner
     * @param columns 需要提取的列 
     * @param start_row 起点rowKey
     * @param stop_row  终点rowKey
     * @throws IOException 
     * return value:
     * ResultScanner hbase行（result）集合的遍历游标, 可以从中获取查询到的所有行，具体查询hbase接口文档
     */  
    public ResultScanner findRows(List<HBaseColumn> columns,  String start_row, String stop_row)
    {    	
    	Scan s = new Scan();
		for(HBaseColumn item:columns)
		{
			String family = item.getFamily();
			String qualifier = item.getQualifier();
			byte[] family_bytes =  null != family ? Bytes.toBytes(family) : null ;	        
			byte[] qualifier_bytes = null != qualifier ? Bytes.toBytes(qualifier) : null;
			s.addColumn(family_bytes,qualifier_bytes);		        
		}
    	
    	byte[] stop_row_bytes =  null != stop_row ? Bytes.toBytes(stop_row) : null ;	        
		byte[] start_row_bytes = null != start_row ? Bytes.toBytes(start_row) : null;
		s.setStopRow(stop_row_bytes).setStartRow(start_row_bytes);
    	ResultScanner scanner = null;
	    
	    try {
	           scanner = table.getScanner(s);

	     }catch (Exception e) {
	    	 logger.error("hbase 获取table rows失败: "+ e.getMessage());	
	 	 }
	    
	    return scanner;
	} 
    
   
	  /** 
          * 创建HBase Table, 如果table不存在则创建
     * @param tablename table名称
     * @param families 需要创建的table的列族信息（hbase的table只需要设定列族family，不需要列qualifier）
     * @throws IOException 
     * return value:
     * false if table exists, true if successfully created, exception if failure
     */ 
	 public static boolean createTable(String tablename, List<String> families) throws IOException{
		 	boolean created = false;
	        Connection con = HBaseClient.getInstance().getConnection();
	        Admin admin = con.getAdmin();
	        TableName tn = TableName.valueOf(tablename);
	        if (!admin.tableExists(tn)){
	            HTableDescriptor htd = new HTableDescriptor(tn);
	            for(String family : families)
	            {
	            	HColumnDescriptor hcd = new HColumnDescriptor(family);
	            	htd.addFamily(hcd);
	            }
	            	            
	            admin.createTable(htd);
	            created = true;
	            System.out.println("表不存在，新创建表成功....");
	        }
	        return created;
	    }
	   
	  /** 
	   *用一个已有的HBase table的shema信息创建一个新Table, 如果table不存在则创建
	 * @param targettablename 需要新创建的 table名称
	 * @param sourcetablename 提供shema的source table名称
	 * @throws IOException 
	 * return value:
	 * false if table exists, true if successfully created, exception if failure
	 */
	   public static boolean createTableFrom(String targettablename, String sourcetablename) throws IOException{
		    boolean created = false;
		    Connection con = HBaseClient.getInstance().getConnection();
	        Admin admin = con.getAdmin();
	        TableName target_tn = TableName.valueOf(targettablename);
	        TableName source_tn = TableName.valueOf(sourcetablename);
	        if (!admin.tableExists(target_tn) && admin.tableExists(source_tn)){
	            HTableDescriptor htd = new HTableDescriptor(target_tn);
	            for(byte[] family : con.getTable(source_tn).getTableDescriptor().getFamiliesKeys())
	            {
	            	HColumnDescriptor hcd = new HColumnDescriptor(family);
	            	htd.addFamily(hcd);
	            }
	            admin.createTable(htd);
	            created = true;
	            System.out.println("表不存在，新创建表成功....");
	        }
	        return created;
	    }
	   /** 
		 *插入/更新某一行某一列的值，指定时间戳, 指定过期时间ttl
		 * @param rowKey 行键名称
		 * @param column 列名称
		 * @param value   列值
		 * @param time_stamp 时间戳（毫秒）
		 * @param ttl 过期时间（毫秒）
		 * return value:
		 * true if put succeeded, otherwise false
		 */
	   public boolean putTempValue(String rowKey, HBaseColumn column, String value, long time_stamp, long ttl)
	    {
	    	return putValueImpl(rowKey,  column,  value,  time_stamp, ttl);
	    }
	   
	   /** 
		 *插入/更新某一行某一列的值，指定值过期时间ttl
		 * @param rowKey 行键名称
		 * @param column 列名称
		 * @param value   列值
		 * @param ttl 过期时间（毫秒）
		 * return value:
		 * true if put succeeded, otherwise false
		 */
	   public boolean putTempValue(String rowKey, HBaseColumn column, String value,  long ttl)
	    {
	    	return putValueImpl(rowKey,  column,  value,  -1l, ttl);
	    }
	   
		/** 
		 *插入/更新某一行某一列的值，指定时间戳
		 * @param rowKey 行键名称
		 * @param column 列名称
		 * @param value   列值
		 * @param time_stamp 时间戳（毫秒）
		 * return value:
		 * true if put succeeded, otherwise false
		 */
	   public boolean putValue(String rowKey, HBaseColumn column, String value, long time_stamp)
	    {
	    	return putValueImpl(rowKey,  column,  value,  time_stamp, -1l);
	    }
	    
		/** 
		 *插入/更新某一行某一列的值（不指定时间戳，默认当前时间戳
		 * @param rowKey 行键名称
		 * @param column 列名称
		 * @param value   列值
		 * return value:
		 * true if put succeeded, otherwise false
		 */
	    public boolean putValue(String rowKey, HBaseColumn column, String value)
	    {
	    	return putValueImpl(rowKey,  column,  value,  -1l, -1l);
	    }
	    
	    
		   /** 
			 *插入/更新某一行某一列的值，指定时间戳, 指定过期时间ttl
			 * @param rowKey 行键名称
			 * @param column 列名称
			 * @param value   列值
			 * @param time_stamp 时间戳（毫秒）
			 * @param ttl 过期时间（毫秒）
			 * return value:
			 * true if put succeeded, otherwise false
			 */
	    private boolean putValueImpl(String rowKey, HBaseColumn column, String value, long time_stamp, long ttl)
	    {  
	    		boolean success = false;
		        Put p = null;
		        if(time_stamp>=0)
		        	p = new Put(Bytes.toBytes(rowKey),time_stamp);
		        else
		        	p= new Put(Bytes.toBytes(rowKey));

		        p.addColumn(Bytes.toBytes(column.getFamily()), Bytes.toBytes(column.getQualifier()),Bytes.toBytes(value));
		        
		        if(ttl>0)
		        {
		        	p.setTTL(ttl);
		        }
		        
		        try {
					table.put(p);
					success = true;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					logger.error("hbase 写入table cell失败: "+ e.getMessage());	
				}
		        
		        return success;
	    }
	    
	    
		 /** 
		 * 插入/更新某一行某几列的值，指定时间戳, 指定过期时间ttl
		 * @param rowKey 行键名称
		 * @param column_values 列和值组合
		 * @param time_stamp 时间戳（毫秒）
		 * @param ttl 过期时间（毫秒）
		 * return value:
		 * true if put succeeded, otherwise false
		 */
	    public boolean putValues(String rowKey, Map<HBaseColumn, String> column_values, long time_stamp)
	    {
	    	return putValuesImpl(rowKey, column_values,  time_stamp, -1l);
	    }
	    
		 /** 
		 *插入/更新某一行某几列的值，指定时间戳, 指定过期时间ttl
		 * @param rowKey 行键名称
		 * @param column_values 列和值组合
		 * @param time_stamp 时间戳（毫秒）
		 * @param ttl 过期时间（毫秒）
		 * return value:
		 * true if put succeeded, otherwise false
		 */
	    public boolean putValues(String rowKey, Map<HBaseColumn, String> column_values)
	    {
	    	return putValuesImpl(rowKey, column_values,  -1l, -1l);
	    }
	    
	    /** 
		 * 插入/更新某一行某几列的值，指定过期时间ttl
		 * @param rowKey 行键名称
		 * @param column_values 列和值组合
		 * @param ttl 过期时间（毫秒）
		 * return value:
		 * true if put succeeded, otherwise false
		 */
	    public boolean putTempValues(String rowKey, Map<HBaseColumn, String> column_values, long ttl)
	    {
	    	return putValuesImpl(rowKey, column_values,  -1l, ttl);
	    }
	    
	    /** 
		 * 插入/更新某一行某几列的值，指定时间戳，指定过期时间ttl
		 * @param rowKey 行键名称
		 * @param column_values 列和值组合
		 * @param time_stamp 时间戳（毫秒）
		 * @param ttl 过期时间（毫秒）
		 * return value:
		 * true if put succeeded, otherwise false
		 */
	    public boolean putTempValues(String rowKey, Map<HBaseColumn, String> column_values, long time_stamp, long ttl)
	    {
	    	return putValuesImpl(rowKey, column_values,  -1l, ttl);
	    }
	    
		 /** 
				 *插入/更新某一行某几列的值，指定时间戳, 指定过期时间ttl
				 * @param rowKey 行键名称
				 * @param column_values 列和值组合
				 * @param time_stamp 时间戳（毫秒）
				 * @param ttl 过期时间（毫秒）
				 * return value:
				 * true if put succeeded, otherwise false
		 */
	    private boolean putValuesImpl(String rowKey, Map<HBaseColumn, String> column_values, long time_stamp, long ttl)
	    {  
	    		boolean success = false;
	        	Put p = null;
		        if(time_stamp>=0)
		        	p = new Put(Bytes.toBytes(rowKey),time_stamp);
		        else
		        	p= new Put(Bytes.toBytes(rowKey));
		        
		        for(Entry column_value : column_values.entrySet())
		        {
		        	HBaseColumn column = (HBaseColumn) column_value.getKey();
		        	String value = column_value.getValue().toString();

		        	p.addColumn(Bytes.toBytes(column.getFamily()), Bytes.toBytes(column.getQualifier()),Bytes.toBytes(value));
		        }
		        
		        if(ttl>0)
		        {
		        	p.setTTL(ttl);
		        }
		       
		        try {
					table.put(p);
					success = true;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					logger.error("hbase 写入table cell失败: "+ e.getMessage());	
				}
		        
		        return success;
	    }
	    
	    public static void printRows( List<Result> rows)
	    {
	    	  for (Result rr :rows) {
		             // print out the row we found and the columns we were looking for
		             System.out.println("Found row: " + rr);
		             System.out.println(Bytes.toString(rr.getRow()));
		             for(Cell cell : rr.rawCells())
		             {
		                 System.out.println("family: " + new String(CellUtil.cloneFamily(cell))); 
		                 System.out.println("qualifier: " + new String(CellUtil.cloneQualifier(cell))); 
		                 System.out.println("value: " + new String(CellUtil.cloneValue(cell))); 	             }	             
		        }
	    }
	    
	    public static void printRowCells(HashMap<String, String> cells)
	    {
	    	for(String column : cells.keySet())
	    	{
	    		 System.out.println("column: " + column);
	    		 System.out.println("value: " + cells.get(column));
	    	}
	    }
	    
	    public static void printRowCells(List<Cell> cells)
	    {
	    	for( Cell  cell: cells)
	    	{
	            System.out.println("family: " + new String(CellUtil.cloneFamily(cell))); 
	            System.out.println("qualifier: " + new String(CellUtil.cloneQualifier(cell))); 
	            System.out.println("value: " + new String(CellUtil.cloneValue(cell))); 
	    	}
	    }
	    
	    public static void printRowCells(Result result)
	    {
	    	for( Cell  cell: result.rawCells())
	    	{
	            System.out.println("family: " + new String(CellUtil.cloneFamily(cell))); 
	            System.out.println("qualifier: " + new String(CellUtil.cloneQualifier(cell))); 
	            System.out.println("value: " + new String(CellUtil.cloneValue(cell))); 
	    	}
	    }	    
	    
		public static void main(String[] args) throws Exception {
			
			byte[] bytes = null;
			String table_name = args[0];
			System.out.println("test tabble " + table_name);
			HBaseTableOperation tableOp = new HBaseTableOperation(table_name);
			List<String> families = new ArrayList<String>();
			families.add("f1");
			families.add("f2");
			tableOp.createTable(table_name, families);

			System.out.println("test write hbase ... ");

			System.out.println("put value into  one column of table " + table_name + " ... ");
			tableOp.putValue("0001", new HBaseColumn("f1", ""), "12345");
			System.out.println("put values into columns  of table" + tableOp.getTableName());
			HashMap<HBaseColumn, String> column_values = new HashMap<HBaseColumn, String>();
			column_values.put(new HBaseColumn("f1", ""), "12345");
			column_values.put(new HBaseColumn("f1", "c1"), "45345");
			tableOp.putValues("0002", column_values);


			System.out.println("test query row by input row key and 2 columns of table" + tableOp.getTableName());
			List<HBaseColumn>  columns = new ArrayList<HBaseColumn>();
			columns.add(new HBaseColumn("f1", ""));
			columns.add(new HBaseColumn("f1", "c1"));
			
			HashMap<String, String> values = tableOp.fetchCellValues2("0002", columns);
			HBaseTableOperation.printRowCells(values);
			
			System.out.println("test query row by input row key and 1 column of table" + tableOp.getTableName());
			String result = tableOp.getRowCellValue("0001", new HBaseColumn("f1", ""));
			System.out.println(result);
			
			System.out.println("test query rows by input columns of table" + tableOp.getTableName());
			List<Result> rows = tableOp.fetchRows(columns, "0001", "0002");
			HBaseTableOperation.printRows(rows);
			
			System.out.println("test only input family of table" + tableOp.getTableName());
			columns.clear();
			columns.add(new HBaseColumn("f1", ""));
			values = tableOp.fetchCellValues2("0002", columns);
			HBaseTableOperation.printRowCells(values);


			
			System.out.println("test write temp value with ttl 10s... ");
			tableOp.putTempValue("0005", new HBaseColumn("f2", ""), "12345", 10000l);
			System.out.println("read value put just now: " + tableOp.getRowCellValue("0005", new HBaseColumn("f2", "")));
			System.out.println("wait 11s ... ");
			Thread.sleep(11000l);
			System.out.println("read value put just now: " + tableOp.getRowCellValue("0005", new HBaseColumn("f2", "")));

			tableOp.close();
	    }
		
}
