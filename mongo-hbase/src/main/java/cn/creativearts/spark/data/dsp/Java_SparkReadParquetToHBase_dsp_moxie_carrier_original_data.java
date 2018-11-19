package cn.creativearts.spark.data.dsp;

import cn.creativearts.spark.util.HBaseSparkUtils;
import cn.creativearts.spark.util.Java_Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;


/**
 * Created by xuchl on 2018/9/28.
 *
 */

public class Java_SparkReadParquetToHBase_dsp_moxie_carrier_original_data {
    private static Configuration hbaseConf;

    static {
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hadoop.security.authentication", "Kerberos");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 1024);
    }

    public static void main(String[] args) throws Exception {
        final SparkSession spark = SparkSession.builder()
                .config("spark.default.parallelism", "1000")
                .config("spark.sql.shuffle.partitions", "1000")
                //.config("spark.sql.shuffle.compress", true)
                .config("spark.shuffle.consolidateFiles", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .appName(Thread.currentThread().getStackTrace()[1].getClassName())
                //.master("local[4]")
                //.config("spark.sql.warehouse.dir", "/opt/spark-warehouse")
                //.enableHiveSupport()
                .getOrCreate();

        //读取指定hive表数据/Parquet文件
        //Dataset ds = spark.table(args[0]);
        Dataset ds = spark.read().parquet(args[0]);
        //ds.printSchema();

        //写入的HBase表名
        final String quorum = args[1];
        final String table_name = args[2];

        //Kerberos认证相关
        final String krb5confPath = args[3];
        final String principal = args[4];
        final String keytabPath = args[5];

        //设置集群相关
        hbaseConf.set("hbase.zookeeper.quorum", quorum);
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, table_name);
        //设置Hfile压缩
        hbaseConf.set("hfile.compression", "snappy");
        //设置HBase压缩（多余，以表设置为准？）
        hbaseConf.set("hbase.regionserver.codecs", "snappy");

        //认证
        Java_Utils.initKerberosENV(hbaseConf, krb5confPath, principal, keytabPath);



        JavaRDD<Row> javaRDD = ds.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                return Java_Utils.getValue(row, "create_time").length() >= 23;
            }
        }).map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {

                //解析出嵌套结构对象
                String id = Java_Utils.getValue(row,"_id");
                String create_time = Java_Utils.getValue(row, "create_time");
                String mobile = Java_Utils.getValue(row, "mobile");

                //原始数据表 rowkey ： 用户手机号反转+最大时间(2524579200L)-create_time
                String rowkey = new StringBuffer(mobile).reverse()
                        .append("_")
                        .append(2524579200L - Java_Utils.getLongTime(create_time))
                        .toString();

                String moxie_carrier_originalInfo = Java_Utils.getValue(row, "moxie_carrier_originalInfo");
                String successFlag = Java_Utils.getValue(row, "successFlag");
                String task_id = Java_Utils.getValue(row, "task_id");
                String update_time = Java_Utils.getValue(row, "update_time");

                return RowFactory.create(rowkey,id,create_time,mobile,moxie_carrier_originalInfo,successFlag,task_id,update_time);

            }});


        //执行写入
        HBaseSparkUtils.saveHDFSHbaseHFile(hbaseConf, javaRDD, table_name, 0,"rowkey,_id,create_time,mobile,moxie_carrier_originalInfo,successFlag,task_id,update_time");



        spark.stop();
    }
}

