package cn.creativearts.spark.data.dsp;

import cn.creativearts.spark.data.dsp.moxie.Calls;
import cn.creativearts.spark.data.dsp.moxie.Items;
import cn.creativearts.spark.data.dsp.moxie.MoxieCarrierOriginalInfoJson;
import cn.creativearts.spark.util.HBaseSparkUtils;
import cn.creativearts.spark.util.Java_Utils;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;


import java.util.*;


/**
 * Created by xuchl on 2018/9/28.
 */

public class Java_SparkReadParquetToHBase_dsp_moxie_carrier_originalInfo_group {
    private static Configuration hbaseConf;

    static {
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hadoop.security.authentication", "Kerberos");
        hbaseConf.set("hbase.zookeeper.quorum", "dev02,dev03,dev04");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");

    }

    public static void main(String[] args) throws Exception {
        final SparkSession spark = SparkSession.builder()
                //.config("spark.default.parallelism", "20")
                .config("spark.sql.shuffle.partitions", "1000")
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
        final String tableName = args[1];

        //Kerberos认证相关
        final String krb5confPath = args[2];
        final String principal = args[3];
        final String keytabPath = args[4];

        //认证
        Java_Utils.initKerberosENV(hbaseConf,krb5confPath,principal,keytabPath);

        //解析打平为结果10字段（rowkey,dial_type,duration,fee,id,location,location_type,mobile,peer_number,time）
        //过滤掉通话详情为空的记录
        JavaRDD<Row> javaRDD = ds.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                return row.getAs("moxie_carrier_originalInfo") != null;
            }
        }).flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row row) throws Exception {
                //返回Tuple10列表
                List<Row> list = new ArrayList<Row>();
                //Set<Row> list = new HashSet<Row>();

                //解析出嵌套结构对象
                String id = row.getAs("_id").toString();
                String mobile = row.getAs("mobile").toString();
                MoxieCarrierOriginalInfoJson moxieCarrierOriginalInfoJson = JSON.parseObject(row.getAs("moxie_carrier_originalInfo").toString(), MoxieCarrierOriginalInfoJson.class);
                //moxieCarrierOriginalInfoJson.getCalls().get(0).getItems().get(1).get

                //解析字段，封装Tuple10
                //遵循字段字典排序
                for (Calls call : moxieCarrierOriginalInfoJson.getCalls()) {
                    for (Items items : call.getItems()) {
                        String details_id = items.getDetails_id();

                        String dial_type = items.getDial_type();
                        String duration = items.getDuration();
                        String fee = String.valueOf(items.getFee());
                        String location = items.getLocation();
                        String location_type = items.getLocation_type();
                        //规范被/主叫格式
                        String peer_number = Java_Utils.trims(items.getPeer_number());
                        String time = items.getTime();

                        // rowkey：mobile+产品类型(14位userid ：ymt  12位userid：nyd)+yyyyMMddHHMM(差值倒排)+主被叫(0：主叫 1：被叫)
                        // +产品类型(1-nyd,2-ymt)+序号
                        String rowkey = new StringBuffer(mobile).reverse()
                                .append("_").append(Java_Utils.dateTrans(time))
                                .append("_").append(Java_Utils.dialType(dial_type))
                                //.append("_").append(Java_Utils.peerTail(peer_number))
                                //.append(Java_Utils.peerTail(String.valueOf(details_id.hashCode())))
                                //.append("_").append(Java_Utils.proType(id))
                                .append("_")
                                .toString();

                        //转为Row类型便于后续通用计算
                        //不包含schema信息， row.schema().fieldNames()会报null空指针错误
/*                        //过滤掉peer_number不合规的记录
                        if(!Java_Utils.isNumeric(peer_number)){
                            continue;
                        }*/
                        list.add(RowFactory.create(rowkey, dial_type, duration, fee, id, location, location_type, mobile, peer_number, time));
                        //list.add((Row)new Tuple10(rowkey,dial_type,duration,fee,id,location,location_type,mobile,peer_number,time));
                    }
                }
                return list.iterator();
            }
        });

        JavaRDD<Row> javaRDD_ordered = javaRDD.groupBy(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                String rowkey = row.get(0).toString();
                return rowkey;
            }
        }).flatMap(new FlatMapFunction<Tuple2<String, Iterable<Row>>, Row>() {
            @Override
            public Iterator<Row> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                List<Row> list = new ArrayList<Row>();
                List<Row> result = new ArrayList<Row>();

                Iterable<Row> rows = tuple._2();
                Iterator it = rows.iterator();

                while (it.hasNext()) {
                    list.add((Row)it.next());
                }

                //去除不同产品间的相同数据
                for(int i = 0;i<list.size()-1;i++){
                    for(int j = list.size()-1;j>i;j--){
                        //如果peer_number和time一致，视为相同数据，舍弃一个
                        if(list.get(j).get(8).toString().equals(list.get(i).get(8).toString()) && list.get(j).get(9).toString().equals(list.get(i).get(9).toString())){
                            list.remove(j);
                        }
                    }
                }

                //遍历，加上序号
                for(int i = 0;i<list.size();i++){
                    result.add(RowFactory.create(list.get(i).get(0).toString().concat(String.valueOf(i)),
                            Java_Utils.getValue(list.get(i), 1),Java_Utils.getValue(list.get(i), 2),Java_Utils.getValue(list.get(i),3),
                            Java_Utils.getValue(list.get(i), 4),Java_Utils.getValue(list.get(i), 5),Java_Utils.getValue(list.get(i),6),
                            Java_Utils.getValue(list.get(i), 7),Java_Utils.getValue(list.get(i),8),Java_Utils.getValue(list.get(i),9)));
                }

                return result.iterator();
            }});

        //执行写入
        HBaseSparkUtils.saveHDFSHbaseHFile(hbaseConf, javaRDD_ordered, tableName, 0, "rowkey,dial_type,duration,fee,id,location,location_type,mobile,peer_number,time");


        spark.stop();
    }
}

