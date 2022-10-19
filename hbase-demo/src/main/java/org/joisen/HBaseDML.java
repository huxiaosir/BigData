package org.joisen;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author : joisen
 * @date : 15:03 2022/10/18
 */
public class HBaseDML {
    // 静态属性
    public static Connection connection = HBaseConnection.connection;

    /**
     * 插入数据
     * @param namespace
     * @param tableName
     * @param rowKey 主键
     * @param columnFamily 列族
     * @param columnName 列明
     * @param value 插入值
     */
    public static void putCell(String namespace,String tableName,String rowKey,
                               String columnFamily,String columnName,String value) throws IOException {
        // 判断表格是否存在
        if(!HBaseDDL.isTableExists(namespace,tableName)){
            System.out.println("表不存在。。。");
            return;
        }
        // 1 获取Table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        // 2 调用相关方法插入数据
        // 2.1 创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        // 2.2 给put对象添加数据
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(value));
        // 2.3 将put对象写入对应的方法
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 3 关闭table
        System.out.println("添加成功...");
        table.close();
    }

    /**
     * 读取数据  读取对应的一行中的某一列
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     */
    public static void getCells(String namespace,String tableName,String rowKey,
                                String columnFamily, String columnName) throws IOException {
        // 1 获取table
        Table table = connection.getTable(TableName.valueOf(namespace,tableName));

        // 2 创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        // 如果直接调用get方法读取数据，此时读一整行

        // 如果想读取某一cell的数据  需要添加对应的参数(即添加列族和列名)
        get.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));

        // 设置读取数据的版本
        get.readAllVersions();

        try {
            // 读取数据得到Result对象
            Result result = table.get(get);
            // 处理数据
            Cell[] cells = result.rawCells();
            // 把数据打印到控制台
            for (Cell cell : cells) {
                // cell存储数据比较底层
                String value = new String(CellUtil.cloneValue(cell));
                System.out.println(value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 关闭table
        table.close();
    }

    /**
     * 扫描数据  读取多行数据
     * @param namespace
     * @param tableName
     * @param startRow 开始行 （左闭右开）
     * @param stopRow 结束行
     */
    public static void scanRows(String namespace,String tableName,String startRow,
                                String stopRow) throws IOException {
        // 1 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        // 2 创建scan对象  如果直接使用该scan对象，会扫描整张表
        Scan scan = new Scan();
        // 添加参数 来控制扫描的数据行数
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));

        try {
            // 读取多行数据  获得scanner
            ResultScanner scanner = table.getScanner(scan);
            // result用来记录一行数据   cell数组
            // ResultScanner用来记录多行数据   result数组
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.print(new String(CellUtil.cloneRow(cell))+"-"+new String(CellUtil.cloneFamily(cell))+"-"+new String(CellUtil.cloneQualifier(cell))+"-"+new String(CellUtil.cloneValue(cell))+"\t");
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 关闭table
        table.close();
    }

    /**
     * 过滤扫描
     * @param namespace
     * @param tableName
     * @param startRow
     * @param stopRow
     * @param columnFamily
     * @param columnName
     * @param value 过滤值
     * @throws IOException
     */
    public static void filterScan(String namespace,String tableName,String startRow,
                                String stopRow,String columnFamily,String columnName,String value) throws IOException {
        // 1 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        // 2 创建scan对象  如果直接使用该scan对象，会扫描整张表
        Scan scan = new Scan();
        // 添加参数 来控制扫描的数据行数
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));
        // 可以添加多个过滤
        FilterList filterList = new FilterList();
        // 创建过滤器
        // 1) 结果只保留当前列的数据
        ColumnValueFilter columnValueFilter = new ColumnValueFilter(
                // 列族名称
                Bytes.toBytes(columnFamily),
                // 列名
                Bytes.toBytes(columnName),
                // 比较关系
                CompareOperator.EQUAL,
                // 值
                Bytes.toBytes(value)
        );
        // 2) 结果保留整行数据
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                // 列族名称
                Bytes.toBytes(columnFamily),
                // 列名
                Bytes.toBytes(columnName),
                // 比较关系
                CompareOperator.EQUAL,
                // 值
                Bytes.toBytes(value)
        );
        // 本身可以添加多个过滤器
//        filterList.addFilter(columnValueFilter);
        filterList.addFilter(singleColumnValueFilter);
        // 添加过滤
        scan.setFilter(filterList);

        try {
            // 读取多行数据  获得scanner
            ResultScanner scanner = table.getScanner(scan);
            // result用来记录一行数据   cell数组
            // ResultScanner用来记录多行数据   result数组
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.print(new String(CellUtil.cloneRow(cell))+"-"+new String(CellUtil.cloneFamily(cell))+"-"+new String(CellUtil.cloneQualifier(cell))+"-"+new String(CellUtil.cloneValue(cell))+"\t");
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 关闭table
        table.close();
    }

    /**
     * 删除一行中的一列数据
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     */
    public static void deleteColumn(String namespace,String tableName,String rowKey,
                                    String columnFamily,String columnName) throws IOException {
        // 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        // 创建delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 添加列信息
        // addColumn删除一个版本
//        delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        // addColumns删除所有版本
        delete.addColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        // 调用方法删除数据
        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 关闭table
        table.close();
        System.out.println("删除成功...");
    }
    public static void main(String[] args) throws IOException {
        // 测试添加数据
//        putCell("bigdata","student","1003","info","name","joisen");
//        putCell("bigdata","student","1003","info","name","joisen2");
//        putCell("bigdata","student","1004","info","name","hannabi");
        // 测试读取数据
//        getCells("bigdata","student","1003","info","name");

        // 测试扫描多行数据
//        scanRows("bigdata","student","1002","1005");

//        filterScan("bigdata","student","1002","1005","info","name","hannabi");


        getCells("bigdata","student","1003","info","name");
        deleteColumn("bigdata","student","1003","info","name");
        getCells("bigdata","student","1003","info","name");



        System.out.println("***********");

        //关闭连接
        connection.close();

    }



}
