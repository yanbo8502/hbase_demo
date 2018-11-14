package com.yanbo.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//for coprocessor writing

public class TestCoprocessorData extends BaseRegionObserver {
    private static final Logger logger = LoggerFactory.getLogger(TestCoprocessorData.class);
    static Table table = null;

    private final static String SOURCE_TABLE = "testG:index_table";

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        try {
            logger.info(String.format("%s Observer TestCoprocessorData started ...", Constant.version_str));

            ExecutorService pool = Executors.newFixedThreadPool(10);//建立一个数量为10的线程池
            table = env.getTable(TableName.valueOf(SOURCE_TABLE), pool);

        } catch (Exception ex) {
            logger.error(ex.getMessage());

        }

    }


    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        try {
            logger.info("Observer TestCoprocessorData stopped ...");

        } catch (Exception ex) {
            logger.error(ex.getMessage());

        }


    }
/*
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,
                       Put put, WALEdit edit, Durability durability) throws IOException {
        try {
            //super.prePut(e, put, edit, durability);
            put2Index(put);

        } catch (Exception ex) {

        }
    }
*/
    private void put2Index(Put put) throws IOException {
        byte[] rowKey = put.getRow();
        Cell cell = put.get("f1".getBytes(), "id".getBytes()).get(0);
        Put putIndex = new
                Put(cell.getValueArray(), cell.getValueOffset(),cell.getValueLength());
        putIndex.addColumn("f1".getBytes(), "rowKey".getBytes(), rowKey);
        table.put(putIndex);

        cell = put.get("f1".getBytes(), "phone".getBytes()).get(0);
        putIndex = new
                Put(cell.getValueArray(), cell.getValueOffset(),cell.getValueLength());
        putIndex.addColumn("f1".getBytes(), "rowKey".getBytes(), rowKey);
        table.put(putIndex);

        cell = put.get("f1".getBytes(), "cid".getBytes()).get(0);
        putIndex = new
                Put(cell.getValueArray(), cell.getValueOffset(),cell.getValueLength());
        putIndex.addColumn("f1".getBytes(), "rowKey".getBytes(), rowKey);
        table.put(putIndex);
        table.close();
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e,
                        Put put, WALEdit edit, Durability durability) throws IOException {
        try {
            logger.info(String.format("%s, %s postPut", Constant.version_str, SOURCE_TABLE));
            //super.prePut(e, put, edit, durability);
            put2Index(put);

        } catch (Exception ex) {
            logger.error(ex.getMessage());

        }

    }

}