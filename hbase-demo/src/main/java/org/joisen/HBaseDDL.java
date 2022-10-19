package org.joisen;

import javafx.scene.control.Tab;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author : joisen
 * @date : 9:25 2022/10/18
 */
public class HBaseDDL {

    // 声明一个静态属性
    public static Connection connection = HBaseConnection.connection;

    /**
     * 创建命名空间
     * @param namespace 命名空间名称
     */
    public static void createNamespace(String namespace) throws IOException {
        // 1 获取admin
        // admin的连接是轻量级的，不是线程安全的，不推荐池化或者缓存这个连接
        Admin admin = connection.getAdmin();
        // 2 调用方法创建命名空间
        // 2.1 创建命名空间描述 建造者
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);
        // 2.2 给命名空间添加需求 <键值对>
        builder.addConfiguration("user","joisen");
        // 2.3 使用bulider构造出对应的添加完参数的对象，完成创建
        // 代码相对shell更加底层 shell能够实现的功能 代码也能是实现，所以需要填写完整的命名空间描述
        // 创建命名空间出现问题应该捕获

        try {
            admin.createNamespace(builder.build());
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("创建命名空间错误");
        }

        // 3 关闭admin
        admin.close();

    }

    /**
     * 判断表格是否存在
     * @param namespace 命名空间名称
     * @param tableName 表格名称
     * @return
     */
    public static boolean isTableExists(String namespace,String tableName) throws IOException {
        // 获取admin
        Admin admin = connection.getAdmin();
        // 调用方法判断表格是否存在
        boolean exists = false;
        try {
            exists = admin.tableExists(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //关闭admin
        admin.close();
        // 返回结果
        return exists;
    }

    /**
     * 创建表格
     * @param namespace 命名空间名称
     * @param tableName 表名
     * @param columnFamiles 列族（可变）
     */
    public static void createTable(String namespace,String tableName,String... columnFamiles) throws IOException {

        // 由于列族设置的可变参数，故存在创建时设置0个列族的情况
        if (columnFamiles.length == 0){
            System.out.println("至少需要一个列族...");
            return;
        }

        // 判断表格是否存在
        if (isTableExists(namespace,tableName)){
            System.out.println("表格已经存在");
            return;
        }

        // 1 获取admin
        Admin admin = connection.getAdmin();
        // 2 调用方法创建表格
        // 2.1 创建表格描述的建造者
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));
        // 2.2 添加参数
        for (String columnFamily : columnFamiles) {
            // 2.3 创建列族描述的建造者
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
            // 2.4 对应当前的列族添加参数
            columnFamilyDescriptorBuilder.setMaxVersions(5);
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }

        try {
            // 3 创建对应的表格描述
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 关闭admin
        admin.close();
    }

    /**
     * 修改表格中一个列族的版本
     * @param namespace
     * @param tableName
     * @param columnFamily 列族名称
     * @param version 版本
     */
    public static void modifyTable(String namespace,String tableName,String columnFamily,int version) throws IOException {
        // 1 获取admin
        Admin admin = connection.getAdmin();
        if (!isTableExists(namespace,tableName)){
            System.out.println("表格不存在。。。");
            return;
        }
        try {
            // 2 调用方法修改表格
            // 2.0 获取之前的表格描述
            TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(namespace, tableName));

            // 2.1 创建表格描述建造者
            // 如果使用填写tableName的方法，相当于创建了一个新的表格描述建造者，没有之前的信息
            // 如果想要修改之前的信息，必须调用方法填写一个旧的表格描述
//        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);

            // 2.2 对应建造者进行表格数据的修改
            ColumnFamilyDescriptor columnFamily1 = descriptor.getColumnFamily(Bytes.toBytes(columnFamily));
            // 创建列族描述建造者
            // 需要填写旧的列族描述
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily1);
            // 修改对应的版本
            columnFamilyDescriptorBuilder.setMaxVersions(version);

            tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());

            admin.modifyTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 关闭admin
        admin.close();
    }

    /**
     * 删除表格
     * @param namespace
     * @param tableName
     * @return
     */
    public static boolean deleteTable(String namespace,String tableName) throws IOException {
        // 1 判断表格是否存在
        if (!isTableExists(namespace,tableName)){
            System.out.println("表格不存在。。。");
            return false;
        }
        //  删除表格
        // 2 获取admin
        Admin admin = connection.getAdmin();
        // 3 调用相关方法删除表格
        try {
            TableName tableName1 = TableName.valueOf(namespace, tableName);
            admin.disableTable(tableName1);
            admin.deleteTable(tableName1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 4 关闭admin
        admin.close();
        return true;
    }

    public static void main(String[] args) throws IOException {
        // 测试创建命名空间
//        createNamespace("joisen");

        // 测试表是否存在
//        System.out.println(isTableExists("bigdata", "student"));

        // 测试创建表格
//        createTable("joisen","student","info","msg");

        // 测试修改表格列族版本
//        modifyTable("joisen","student","info",6);

        // 测试删除表格
        boolean flag = deleteTable("joisen","student");

        System.out.println("******");

        // 关闭HBase连接
        HBaseConnection.colseConnect();
    }


}
