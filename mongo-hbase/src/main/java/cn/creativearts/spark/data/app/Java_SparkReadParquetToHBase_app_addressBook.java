package cn.creativearts.spark.data.app;

import cn.creativearts.spark.util.Java_Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag;

import java.util.Map;


/**
 * Created by xuchl on 2018/9/28.
 * 使用saveAsNewAPIHadoopDataset方法（API+MR），灵活方便、效率较低；
 */

public class Java_SparkReadParquetToHBase_app_addressBook {
    private static volatile Broadcast<Map<String,Boolean>> bcMap = null;

    public static void main(String[] args) throws Exception {
        final SparkSession spark = SparkSession.builder()
                //.config("spark.default.parallelism", "20")
                //.config("spark.sql.shuffle.partitions", "1000")
                //.config("spark.shuffle.consolidateFiles", "true")
                //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .appName(Thread.currentThread().getStackTrace()[1].getClassName())
                //.master("local[4]")
                //.config("spark.sql.warehouse.dir", "/opt/spark-warehouse")
                //.enableHiveSupport()
                .getOrCreate();

        //读取指定hive表数据/Parquet文件
        //Dataset ds = spark.table(args[0]);
        Dataset ds = spark.read().parquet(args[0]);

        //写入的HBase表相关
        final String quorum = args[1];
        final String tableName = args[2];

        //Kerberos认证相关
        final String krb5confPath = args[3];
        final String principal = args[4];
        final String keytabPath = args[5];

        //获取相应的Schema信息（StructField数组）
        //List structFields = new ArrayList();
        final StructField[] structFields = ds.schema().fields();

        //Java版Broadcast需要设置ClassTag
        ClassTag ct = ClassManifestFactory.classType(StructField[].class);
        final Broadcast<StructField[]> bc = spark.sparkContext().broadcast(structFields, ct);
        //structFields[1].name();

        //获取rdd数据
        JavaRDD<Row> rdd = ds.javaRDD();

        //====================================设置环境、任务相关配置=============================
        //设置JobConf（替换HBaseConfiguration）
        Configuration conf = spark.sparkContext().hadoopConfiguration();
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hbase.zookeeper.quorum",quorum);
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);

        //初始化Kerberos权限
        Java_Utils.initKerberosENV(conf,krb5confPath,principal,keytabPath);

        //设置job参数
        Job job = new Job(conf);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        //job.setOutputValueClass(Result.class);
        job.setOutputValueClass(KeyValue.class);
        //job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        job.setOutputFormatClass(TableOutputFormat.class);

        //========================================处理数据=======================================
        //遍历rdd，转为RDD<ImmutableBytesWritable, put>
        //要使用PairRDD
        JavaPairRDD<ImmutableBytesWritable,Put> pairRDD = rdd.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
            @Override
            public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {

                String tel = "noTel";
                if(row.getAs("tel") != null){
                    tel = Java_Utils.trims(row.getAs("tel").toString());
                }

                Put put = new Put(Bytes.toBytes(new StringBuffer(row.getAs("phoneNo").toString()).reverse().append("_").append(tel).toString()));

                for (int i = 0; i < bc.getValue().length; i++) {
                    if ("tel".equals(bc.getValue()[i].name())) {
                        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("tel"), Bytes.toBytes(Java_Utils.trims(Java_Utils.getValue(row, i))));
                    } else {
                        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(bc.getValue()[i].name()), Bytes.toBytes(Java_Utils.getValue(row, i)));
                    }
                }

                return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
            }
        });

        //存入HBase
        pairRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());

        spark.stop();

    }

}
