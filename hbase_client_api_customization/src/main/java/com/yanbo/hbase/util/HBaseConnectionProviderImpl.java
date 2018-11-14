package com.yanbo.hbase.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HBaseConnectionProviderImpl implements HBaseConnectionProvider {
    private Connection conn;

    public HBaseConnectionProviderImpl(final Configuration conf) throws Exception {
        final ExecutorService pool = Executors.newFixedThreadPool(10);//建立一个数量为10的线程池
        String user = conf.get("hbase.user.name");
        System.out.println(user);
        conn = UserGroupInformation.createRemoteUser(user).doAs(new PrivilegedExceptionAction<Connection>() {
            @Override
            public Connection run() throws Exception {
                return ConnectionFactory.createConnection(conf, pool);
            }
        });
//        if(conn == null || conn.isClosed()){
//            conn = ConnectionFactory.createConnection(conf);
//        }
    }
    /**
     * 数据源提供方法
     * @return
     * @throws Exception
     */
    @Override
    public Connection provideConnection() {
        return conn;
    }
}
