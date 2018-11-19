package cn.creativearts.spark.data.dsp;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;


/**
 * Created by xuchl on 2018/9/28.
 */

public class Java_SparkReadMongoDBToHBase_dsp_moxie_carrier_originalInfo {

    public static void main(String[] args) throws Exception {
        final SparkSession spark = SparkSession.builder()
                //.config("spark.default.parallelism", "20")
                //.config("spark.sql.shuffle.partitions", "1000")
                //.config("spark.shuffle.consolidateFiles", "true")
                //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.mongodb.input.uri", "mongodb://dba_read:dba_read_456@10.25.162.152:37207/admin")
                .config("spark.mongodb.input.database", "dsp")
                .config("spark.mongodb.input.collection", "moxie_carrier_originalInfo")
                .appName(Thread.currentThread().getStackTrace()[1].getClassName())
                .master("local[4]")
                //.config("spark.sql.warehouse.dir", "/opt/spark-warehouse")
                //.enableHiveSupport()
                .getOrCreate();


        //读取指定Mongo表数据
        /*Dataset<Moxie_carrier_originalInfo> ds_doc = MongoSpark.load(JavaSparkContext.fromSparkContext(spark.sparkContext())).toDS(Moxie_carrier_originalInfo.class).limit(1000);

        ds_doc.printSchema();

        System.out.println(ds_doc.count());*/

    }
}
