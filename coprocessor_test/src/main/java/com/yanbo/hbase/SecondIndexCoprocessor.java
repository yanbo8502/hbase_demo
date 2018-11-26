package com.yanbo.hbase;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by yanbo on 18-11-19.
 */
public class SecondIndexCoprocessor extends BaseRegionObserver {
    protected static final Logger logger = LoggerFactory.getLogger(SecondIndexCoprocessor.class);
    protected String SOURCE_TABLE = "not set";
    protected Table table = null;
    protected ExecutorService pool = Executors.newFixedThreadPool(10);//建立一个数量为10的线程池

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        try {
            logger.info(String.format("%s Observer %s start ...", Constant.version_str, this.getClass().getName()));

            table = env.getTable(TableName.valueOf(SOURCE_TABLE), pool);
            if(table!=null)
            {
                logger.info(String.format("%s table connected ...",SOURCE_TABLE));
            }
            else
            {
                logger.error(String.format("%s table not got in  CoprocessorEnvironment...",SOURCE_TABLE));
            }

        } catch (Exception ex) {
            logger.error(ex.getMessage());

        }

    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        try {
            logger.info(String.format("%s Observer %s stop ...", Constant.version_str,  this.getClass().getName(),  this.getClass().getName()));
            if(table!=null) table.close();
            if(pool!=null) pool.shutdown();

        } catch (Exception ex) {
            logger.error(ex.getMessage());

        }

    }

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        try {
            logger.info(String.format("%s Observer %s preOpen ...", Constant.version_str));
            if(table!=null) table.close();
            table = e.getEnvironment().getTable(TableName.valueOf(SOURCE_TABLE), pool);
            logger.info(String.format("%s table connected ...",SOURCE_TABLE));

        } catch (Exception ex) {
            logger.error(ex.getMessage());

        }

    }



    protected void processOperationException(ObserverContext<RegionCoprocessorEnvironment> c, Exception e) throws IOException {

        try {
            if(e!=null && e.getMessage()!=null && (IOException.class.isInstance(e) ||e.getMessage().contains("IOException")))
            {
                if(table!=null) table.close();
                table = c.getEnvironment().getTable(TableName.valueOf(SOURCE_TABLE), pool);
                logger.info(String.format("Connection to table %s is closed in TestCoprocessorData coprocessor, reconnect table in processOperationException", SOURCE_TABLE));
            }

        } catch (Exception ex) {
            logger.error("processOperationException: " + ex.getMessage());

        }

    }
}
