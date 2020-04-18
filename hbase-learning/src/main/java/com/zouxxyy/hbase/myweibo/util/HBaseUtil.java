package com.zouxxyy.hbase.myweibo.util;

import com.zouxxyy.hbase.myweibo.constants.Constants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * HBase工具类
 * 1.创建名称空间
 * 2.判断表是否存在
 * 3.创建表
 */
public class HBaseUtil {

    /**
     * 创建名称空间
     * @param nameSpace
     * @throws IOException
     */
    public static void createNameSpace(String nameSpace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();

        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        admin.createNamespace(namespaceDescriptor);
        admin.close();
        connection.close();
    }

    public static boolean isExistTable(String tableName) throws IOException{
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();
        boolean exist = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        connection.close();
        return exist;
    }

    //public static void createTable(String table);
}
