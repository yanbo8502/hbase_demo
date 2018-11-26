package com.yanbo.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by yanbo on 18-11-6.
 */
public class TestCoprocessorIndex extends SecondIndexCoprocessor {

    protected final static String data_table = "testG:data_table";

    private static final Logger logger = LoggerFactory.getLogger(TestCoprocessorIndex.class);

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        SOURCE_TABLE = data_table;
        super.start(env);

    }
    @Override
    public void postGetOp(final ObserverContext<RegionCoprocessorEnvironment> c,
                          final Get get, final List<Cell> results) throws IOException {
        try{

            //logger.info(String.format("%s, %s postGetOp", Constant.version_str, SOURCE_TABLE));
            Cell rowKeyCell =results.get(0);
            byte[] realRowkey = Bytes.copy(rowKeyCell.getValueArray(), rowKeyCell.getValueOffset(), rowKeyCell.getValueLength());

            //logger.info("realRowkey " + Bytes.toString(realRowkey));
            Get realget = new Get(realRowkey);
            Result data_result = table.get(realget);

            List<Cell> data_cells = data_result.listCells();
            //results.clear();
            results.addAll(0,data_cells);

        }catch(Exception e){
            logger.error("postGetOp " + e.getMessage());
            processOperationException(c, e);

        }


    }



}
