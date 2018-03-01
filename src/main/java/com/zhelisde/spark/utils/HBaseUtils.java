package com.zhelisde.spark.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class HBaseUtils {

    HBaseAdmin admin = null;
    Configuration configration = null;
    private HBaseUtils(){
        configration = new Configuration();
        configration.set("hbase.zookeeper.quorum","192.168.25.211:2181");
        configration.set("hbase.rootdir","hdfs://192.168.25.211:9000/hbase");
        try {
            admin = new HBaseAdmin(configration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //privatize constructor
    private static HBaseUtils instance = null;
    public static synchronized HBaseUtils getInstance(){
        if(null == instance){
            instance = new HBaseUtils();
        }
        return instance;
    }

    //get table
    public HTable getTable(String tableName){
        HTable table = null;
        try {
            table = new HTable(configration,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 添加一条记录到 Hbase 表 70 30 128 32 核 200T 8000
     * @param tableName Hbase 表名
     * @param rowkey Hbase 表的 rowkey
     * @param cf Hbase 表的 columnfamily
     * @param column Hbase 表的列
     * @param value 写入 Hbase 表的值
     */

    public void put(String tableName,String rowkey,String cf,String column,String value){
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //for test
    public static void main(String[] args) {
//HTable table = HBaseUtils.getInstance().getTable("category_clickcount");
//System.out.println(table.getName().getNameAsString());
        String tableName = "category_clickcount";
        String rowkey = "20271111_88";
        String cf="info";
        String column ="click_count";
        String value = "2";
        HBaseUtils.getInstance().put(tableName,rowkey,cf,column,value);
    }

}
