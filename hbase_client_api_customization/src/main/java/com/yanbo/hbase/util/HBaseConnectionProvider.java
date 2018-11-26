package com.yanbo.hbase.util;

import org.apache.hadoop.hbase.client.Connection;

/**
 * Created by yanbo on 18-11-13.
 */
public interface HBaseConnectionProvider {
    public Connection provideConnection();
    public void release();
}
