package cn.creativearts.spark.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple10;
import scala.Tuple2;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by xuchl on 2018/10/9.
 */
public class HBaseSparkUtils {

    private static final Logger logger = LoggerFactory.getLogger(HBaseSparkUtils.class);

    public static void saveHDFSHbaseHFile(Configuration hbaseConf,
                                          SparkSession spark,  // spark session
                                          Dataset<Row> ds,   // 数据集
                                          String table_name,  //hbase表名
                                          Integer rowKeyIndex, //rowkey的索引id
                                          String fields) throws Exception { // 数据集的字段列表

        hbaseConf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 1024);
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, table_name);

        JavaRDD<Row> javaRDD = ds.javaRDD();

        JavaPairRDD<ImmutableBytesWritable,KeyValue> javaPairRDD =  javaRDD.mapToPair(new PairFunction<Row, ImmutableBytesWritable, List<KeyValue>>() {
            @Override
            public Tuple2<ImmutableBytesWritable, List<KeyValue>> call(Row row) throws Exception {
                List<KeyValue> tps = new ArrayList<>();

                String rowkey = row.getString(rowKeyIndex);
                ImmutableBytesWritable writable = new ImmutableBytesWritable(Bytes.toBytes(rowkey));

                // Sort Columns。这里需要对列进行排序，不然会报错
                ArrayList<Tuple2> tuple2s = new ArrayList<>();
                String[] columns = fields.split(",");
                for (int i = 0; i < columns.length; i++) {
                    tuple2s.add(new Tuple2(i, columns[i]));
                }

                // 遍历列名，组装Tuple2<ImmutableBytesWritable,KeyValue>
                for (Tuple2 t : tuple2s) {
                    String[] fieldNames = row.schema().fieldNames();
/*                    // 不将作为rowkey的字段存在列里面
                    if (t._2().equals(fieldNames[rowKeyIndex])) {
                        System.out.println(String.format("%s == %s continue", t._2(), fieldNames[rowKeyIndex]));
                        continue;
                    }*/

                    if ("main".equals(t._2())) {
                        continue;
                    }
                    String value = Java_Utils.getValue(row, Integer.valueOf(t._1().toString()));

                    KeyValue kv = new KeyValue(Bytes.toBytes(rowkey),
                            Bytes.toBytes("cf"),
                            Bytes.toBytes(t._2().toString()),
                            Bytes.toBytes(value));

                    tps.add(kv);
                }

/*                for (Tuple2 t : tuple2s) {
                    String value = TestUtils.getValue(row, Integer.valueOf(t._1().toString()));

                    if ("main".equals(t._2())) {  // filed == 'main'
                        KeyValue kv = new KeyValue(Bytes.toBytes(rowkey),
                                Bytes.toBytes("cf"),
                                Bytes.toBytes(t._2().toString()), Bytes.toBytes(value));
                        tps.add(new Tuple2<>(writable, kv));
                        break;
                    }
                }*/

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
        String temp = "/user/xucl/bulkload/" + table_name+ "_" + System.currentTimeMillis();
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
        TableName tableName = TableName.valueOf(table_name);
        HRegionLocator regionLocator = new HRegionLocator(tableName, (ClusterConnection) conn);
        Table realTable = ((ClusterConnection) conn).getTable(tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, realTable, regionLocator);
        Admin admin = conn.getAdmin();


        try {
            loader.doBulkLoad(new Path(temp), admin, realTable, regionLocator);
        }finally {
            conn.close();
        }

    }


    /**
     * 组合Rowkey数据导入
     * @param hbaseConf
     * @param rdd
     * @param table_name
     * @param rowKeyIndex
     * @param fields
     * @throws Exception
     */
    public static void saveHDFSHbaseHFile(Configuration hbaseConf,
                                          JavaRDD<Row> rdd,   // 数据集
                                          String table_name,  //hbase表名
                                          Integer rowKeyIndex, //rowkey的索引id
                                          String fields) throws Exception { // 数据集的字段列表

        hbaseConf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 1024);
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, table_name);

        JavaPairRDD<ImmutableBytesWritable,KeyValue> javaPairRDD =  rdd.mapToPair(new PairFunction<Row, ImmutableBytesWritable, List<KeyValue>>() {
            @Override
            public Tuple2<ImmutableBytesWritable, List<KeyValue>> call(Row row) throws Exception {
                List<KeyValue> tps = new ArrayList<>();

                String rowkey = row.getString(rowKeyIndex);
                ImmutableBytesWritable writable = new ImmutableBytesWritable(Bytes.toBytes(rowkey));

                // Sort Columns。这里需要对列进行排序，不然会报错
                ArrayList<Tuple2> tuple2s = new ArrayList<>();
                String[] columns = fields.split(",");
                for (int i = 1; i < columns.length; i++) {
                    tuple2s.add(new Tuple2(i, columns[i]));
                }

                // 遍历列名，组装Tuple2<ImmutableBytesWritable,KeyValue>
                for (Tuple2 t : tuple2s) {
                    // 不将作为rowkey的字段存在列里面
                    /*if (Integer.valueOf(t._1().toString()) == rowKeyIndex) {
                        //System.out.println(String.format("%s == %s continue", t._2(), fieldNames[rowKeyIndex]));
                        continue;
                    }*/
                    String value = Java_Utils.getValue(row, Integer.valueOf(t._1().toString()));

                    KeyValue kv = new KeyValue(Bytes.toBytes(rowkey),
                            Bytes.toBytes("cf"),
                            Bytes.toBytes(t._2().toString()),
                            Bytes.toBytes(value));

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
        // /user/xucl/bulkload/dsp:moxie_carrier_originalInfo_1539150500589 is not a valid DFS filename.
        // String temp = "/user/xucl/bulkload/" + table_name+ "_" + System.currentTimeMillis();
        String temp = "bulkload/"+ table_name.replace(":","/") + "_" + System.currentTimeMillis();

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
        TableName tableName = TableName.valueOf(table_name);
        HRegionLocator regionLocator = new HRegionLocator(tableName, (ClusterConnection) conn);
        Table realTable = ((ClusterConnection) conn).getTable(tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, realTable, regionLocator);
        Admin admin = conn.getAdmin();


        try {
            loader.doBulkLoad(new Path(temp), admin, realTable, regionLocator);
        }finally {
            conn.close();
        }

    }

    /**
     * 原始数据导入（取第一位为Rowkey）
     * @param hbaseConf
     * @param rdd
     * @param table_name
     * @param fields
     * @throws Exception
     */
    public static void saveHDFSHbaseHFile(Configuration hbaseConf,
                                          JavaRDD<Row> rdd,   // 数据集
                                          String table_name,  //hbase表名
                                          String fields) throws Exception { // 数据集的字段列表

        hbaseConf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 1024);
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, table_name);

        JavaPairRDD<ImmutableBytesWritable,KeyValue> javaPairRDD =  rdd.mapToPair(new PairFunction<Row, ImmutableBytesWritable, List<KeyValue>>() {
            @Override
            public Tuple2<ImmutableBytesWritable, List<KeyValue>> call(Row row) throws Exception {
                List<KeyValue> tps = new ArrayList<>();

                String rowkey = row.getString(0);
                ImmutableBytesWritable writable = new ImmutableBytesWritable(Bytes.toBytes(rowkey));

                // Sort Columns。这里需要对列进行排序，不然会报错
                ArrayList<Tuple2> tuple2s = new ArrayList<>();
                String[] columns = fields.split(",");
                for (int i = 0; i < columns.length; i++) {
                    tuple2s.add(new Tuple2(i, columns[i]));
                }

                // 遍历列名，组装Tuple2<ImmutableBytesWritable,KeyValue>
                for (Tuple2 t : tuple2s) {
                    // 不将作为rowkey的字段存在列里面
                    /*if (Integer.valueOf(t._1().toString()) == rowKeyIndex) {
                        //System.out.println(String.format("%s == %s continue", t._2(), fieldNames[rowKeyIndex]));
                        continue;
                    }*/
                    String value = Java_Utils.getValue(row, Integer.valueOf(t._1().toString()));

                    KeyValue kv = new KeyValue(Bytes.toBytes(rowkey),
                            Bytes.toBytes("cf"),
                            Bytes.toBytes(t._2().toString()),
                            Bytes.toBytes(value));

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
        // /user/xucl/bulkload/dsp:moxie_carrier_originalInfo_1539150500589 is not a valid DFS filename.
        // String temp = "/user/xucl/bulkload/" + table_name+ "_" + System.currentTimeMillis();
        String temp = "bulkload/"+ table_name.replace(":","/") + "_" + System.currentTimeMillis();

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
        TableName tableName = TableName.valueOf(table_name);
        HRegionLocator regionLocator = new HRegionLocator(tableName, (ClusterConnection) conn);
        Table realTable = ((ClusterConnection) conn).getTable(tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, realTable, regionLocator);
        Admin admin = conn.getAdmin();


        try {
            loader.doBulkLoad(new Path(temp), admin, realTable, regionLocator);
        }finally {
            conn.close();
        }

    }
}