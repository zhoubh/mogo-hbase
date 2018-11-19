package cn.creativearts.spark;

import cn.creativearts.spark.util.Java_Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.SparkSession;


/**
 * Created by xuchl on 2018/9/28.
 */

public class HFile_Bulkload {
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
                .config("spark.shuffle.consolidateFiles", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .appName(Thread.currentThread().getStackTrace()[1].getClassName())
                //.master("local[4]")
                //.config("spark.sql.warehouse.dir", "/opt/spark-warehouse")
                //.enableHiveSupport()
                .getOrCreate();

        //HFile文件路径
        final String hfile_path = args[0];

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
        hbaseConf.set("hbase.regionserver.codecs","snappy");
        //认证
        Java_Utils.initKerberosENV(hbaseConf, krb5confPath, principal, keytabPath);


        //导入Hfile
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConf);

        Job job = Job.getInstance();
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);

        Connection conn = ConnectionFactory.createConnection(hbaseConf);
        TableName tableName = TableName.valueOf(table_name);
        HRegionLocator regionLocator = new HRegionLocator(tableName, (ClusterConnection) conn);
        Table realTable = ((ClusterConnection) conn).getTable(tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, realTable, regionLocator);
        Admin admin = conn.getAdmin();

        try {
            loader.doBulkLoad(new Path(hfile_path), admin, realTable, regionLocator);
        }finally {
            conn.close();
        }

        spark.stop();
    }
}

