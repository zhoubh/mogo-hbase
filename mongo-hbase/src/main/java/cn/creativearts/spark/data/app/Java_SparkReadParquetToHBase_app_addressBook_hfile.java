package cn.creativearts.spark.data.app;

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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag;

import java.util.*;


/**
 * Created by xuchl on 2018/9/28.
 * 改写为Hfile+Bulkload方式
 */

public class Java_SparkReadParquetToHBase_app_addressBook_hfile {
    private static Configuration hbaseConf;

    static {
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hadoop.security.authentication", "Kerberos");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 1024);
    }

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
        //设置集群相关
        hbaseConf.set("hbase.zookeeper.quorum", quorum);
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        //设置Hfile压缩
        hbaseConf.set("hfile.compression", "snappy");
        //设置HBase压缩（多余，以表设置为准？）
        hbaseConf.set("hbase.regionserver.codecs", "snappy");

        //初始化Kerberos权限
        Java_Utils.initKerberosENV(hbaseConf, krb5confPath, principal, keytabPath);


        //=================================非嵌套数据直接存储====================================
        JavaPairRDD<ImmutableBytesWritable,KeyValue> javaPairRDD =  rdd.mapToPair(new PairFunction<Row, ImmutableBytesWritable, List<KeyValue>>() {
            @Override
            public Tuple2<ImmutableBytesWritable, List<KeyValue>> call(Row row) throws Exception {
                List<KeyValue> tps = new ArrayList<>();

                String tel = "noTel";
                if(row.getAs("tel") != null){
                    tel = Java_Utils.trims(row.getAs("tel").toString());
                }

                String rowkey = new StringBuffer(row.getAs("phoneNo").toString()).reverse().append("_").append(tel).toString();
                ImmutableBytesWritable writable = new ImmutableBytesWritable(Bytes.toBytes(rowkey));

                // Sort Columns这里需要对列进行排序，不然会报错
                ArrayList<Tuple2> tuple2s = new ArrayList<>();
                // org.apache.spark.sql.types.StructField cannot be cast to java.lang.Comparable
                String[] fields = new String[bc.getValue().length];
                for (int i = 0; i < bc.getValue().length; i++) {
                    fields[i] = bc.getValue()[i].name();
                }
                Arrays.sort(fields);
                //排序后的数据存入Tuple2
                for (int i = 0; i < fields.length; i++) {
                    tuple2s.add(new Tuple2(i, fields[i]));
                }

                // 遍历列名，组装Tuple2<ImmutableBytesWritable,KeyValue>
                for (Tuple2 t : tuple2s) {
                    // 不将作为rowkey的字段存在列里面
                    /*if (Integer.valueOf(t._1().toString()) == rowKeyIndex) {
                        //System.out.println(String.format("%s == %s continue", t._2(), fieldNames[rowKeyIndex]));
                        continue;
                    }*/

                    KeyValue kv = null;
                    if ("tel".equals(t._2().toString())) {
                        tel = Java_Utils.trims(Java_Utils.getValue(row, "tel"));
                        kv = new KeyValue(Bytes.toBytes(rowkey),
                                Bytes.toBytes("cf"),
                                Bytes.toBytes(t._2().toString()),
                                Bytes.toBytes(tel));
                    } else {
                        String value = Java_Utils.getValue(row, t._2().toString());
                        kv = new KeyValue(Bytes.toBytes(rowkey),
                                Bytes.toBytes("cf"),
                                Bytes.toBytes(t._2().toString()),
                                Bytes.toBytes(value));
                    }

                    tps.add(kv);
                }

                return new Tuple2<>(writable, tps);
            }
            // 这里一定要按照rowkey进行排序，这个效率很低，目前没有找到优化的替代方案
        }).sortByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, List<KeyValue>>, ImmutableBytesWritable, KeyValue>() {
            @Override
            public Iterator<Tuple2<ImmutableBytesWritable, KeyValue>> call(Tuple2<ImmutableBytesWritable, List<KeyValue>> tuple2s) throws Exception {
                ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>> result = new ArrayList<>();

                ImmutableBytesWritable imm = tuple2s._1();
                List<KeyValue> list = tuple2s._2();

                for (KeyValue keyvalue : list) {
                    result.add(new Tuple2(imm, keyvalue));
                }

                return result.iterator();
            }
        });


        // 创建HDFS的临时HFile文件目录
        String temp = "bulkload/"+ tableName.replace(":","/") + "_" + System.currentTimeMillis();

        javaPairRDD.saveAsNewAPIHadoopFile(temp,
                ImmutableBytesWritable.class,
                KeyValue.class,
                HFileOutputFormat2.class,
                hbaseConf);

        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConf);


        Job job = Job.getInstance();
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);

        Connection conn = ConnectionFactory.createConnection(hbaseConf);
        TableName tablename = TableName.valueOf(tableName);
        HRegionLocator regionLocator = new HRegionLocator(tablename, (ClusterConnection) conn);
        Table realTable = ((ClusterConnection) conn).getTable(tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, realTable, regionLocator);
        Admin admin = conn.getAdmin();


        try {
            loader.doBulkLoad(new Path(temp), admin, realTable, regionLocator);
        }finally {
            conn.close();
        }

        spark.stop();

    }

}
