package com.yanbo.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HBaseClient {
	private static final Logger logger = LoggerFactory.getLogger(HBaseClient.class);
	/*通过线程池的方式来获取hbase的connection；这样做的好处是在创建的时候就有一定数量 
	 * 的connection，每次使用的時候直接去线程池中取就可以了，不需要每次都创建connection。
	 * connection是一个比较重的对象，可以节约很多系统的资源。
	 * */  
	private static HBaseClient instance = null;
    private static Configuration config = null; 
    private Connection connection = null;//要创建的connection  
    static{  
    	try{
             config= HBaseConfiguration.create();
             String zkhosts = "bxzj-test-swift0.bxzj.baixinlocal.com,bxzj-test-swift1.bxzj.baixinlocal.com,bxzj-test-swift2.bxzj.baixinlocal.com";
             String zkport = "2181";
            //conf.addResource("hbase-site.xml");
            config.set("hbase.zookeeper.quorum",zkhosts);
            config.set("hbase.zookeeper.property.clientPort",zkport);
            config.set("zookeeper.znode.parent","/hbase-unsecure");
    	}catch (Exception e) {  
    		logger.error("hbase配置创建失败:" +  e.getMessage());   
        }  
        
    }  
	
	private HBaseClient() {//私有化构造方法，让用户不能new这个类的对象  
	  initConnection();
	  logger.info("HBaseClient实例创建成功");
	}  
	    
     public static HBaseClient getInstance()
     {
            if(instance == null){
                synchronized (HBaseClient.class) {
                    if(instance == null){
                        instance = new HBaseClient();
                    }
                }
            }
            return instance;
      }
	  
	  public  Connection getConnection() {  
	        if (null == connection) {//空的时候创建，不为空就直接返回；典型的单例模式  
	        	 synchronized (HBaseClient.class) {
	        		 if (null == connection) {
			        	try {  
			                connection = ConnectionFactory.createConnection(config);//创建线程安全的connection  
			            } catch (Exception e) {  
			            	logger.error("hbase中表实例化失败:" +  e.getMessage());  
			            } 
	        		 }
	        	 }
	        }  
	        return connection;  
	  
	    } 
	  
	  public void initConnection() {  
	        if (null == connection) {//空的时候创建，不为空就直接返回；典型的单例模式  
	        	 synchronized (HBaseClient.class) {
	        		 if (null == connection) {
			        	try {  
			                connection = ConnectionFactory.createConnection(config);//创建线程安全的connection  
			            } catch (Exception e) {  
			            	logger.error("hbase中表实例化失败:" +  e.getMessage());  
			            } 
	        		 }
	        	 }
	        }  	  
	    } 
	  
	   /**
	      * 获取htable操作类
       * @param tableName
       * @return
       * @throws IOException
       */
      public Table getHTable(String tableName) throws Exception{
          return connection.getTable(TableName.valueOf(tableName));
      }

	  /**
       * 
       * @param hTableInterface
       */
      public static void relaseHTable(Table table){
          if(table == null){
              return;
          }
          try {
              table.close();
          } catch (Exception e) {
              logger.error("hbase中表关闭失败:" +  e.getMessage());
          }
      }

      /**
       * 关闭hbase连接
       */
      public void destory(){
          try {
              connection.close();
              instance = null;
          } catch (Exception e) {
              logger.error("hbase中连接关闭失败:" + e.getLocalizedMessage());
          }
      }


}
