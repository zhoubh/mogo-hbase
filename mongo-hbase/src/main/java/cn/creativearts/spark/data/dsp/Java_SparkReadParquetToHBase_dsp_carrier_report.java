package cn.creativearts.spark.data.dsp;

import cn.creativearts.spark.data.dsp.moxie.Calls;
import cn.creativearts.spark.data.dsp.moxie.Items;
import cn.creativearts.spark.data.dsp.moxie.MoxieCarrierOriginalInfoJson;
import cn.creativearts.spark.util.HBaseSparkUtils;
import cn.creativearts.spark.util.Java_Utils;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Created by xuchl on 2018/9/28.
 */

public class Java_SparkReadParquetToHBase_dsp_carrier_report {
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
        hbaseConf.set("hfile.compression","snappy");
        //设置HBase压缩（多余，以表设置为准？）
        hbaseConf.set("hbase.regionserver.codecs","snappy");

        //认证
        Java_Utils.initKerberosENV(hbaseConf,krb5confPath,principal,keytabPath);



        spark.stop();
    }
}

