package com.yanbo.hbase;

import java.util.Date;

/**
 * Created by yanbo on 18-11-12.
 */
public class Constant {

    public static final Long version = new Date().getTime() ;
    public static final String version_str = "comprocessor version:" + version;
}
