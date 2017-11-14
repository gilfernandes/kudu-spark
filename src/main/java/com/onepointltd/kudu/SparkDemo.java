package com.onepointltd.kudu;

import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.*;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Used to test Spark using Kudu to create table, write and read data in Java.
 * Most examples are in Scala, but you can also perform operations in Kudu in Spark jobs as well in Java.
 * This class needs to be compiled and submitted to a Spark cluster.
 * See the {@code submit.sh} script to see how this can be done.
 */
public class SparkDemo {

    public static void main(String[] args) {
        String ipAddress = "quickstart.cloudera";
        SparkConf sparkConf = new SparkConf();
        SparkContext sparkContext = new SparkContext(sparkConf);
        JavaSparkContext sc = new JavaSparkContext(sparkContext);
        SQLContext sqlContext = new SQLContext(sc);
        String masters = String.format("%s:7051", ipAddress);
        KuduContext kuduContext = new KuduContext(masters, sparkContext);
        String kuduTable = "spark_kudu_tbl";
        // Check if the table exists, and drop it if it does
        if (kuduContext.tableExists(kuduTable)) {
            kuduContext.deleteTable(kuduTable);
        }
        createSampleKuduTable(kuduContext, kuduTable);
        createDummyData(sc, sqlContext, kuduContext, kuduTable, masters);
        readData(sqlContext, kuduTable, masters);
    }

    private static void createSampleKuduTable(KuduContext kuduContext, String kuduTable) {
        String key = "name";
        StructType structType = new StructType(new StructField[]{
                new StructField(key, StringType, false, Metadata.empty()),
                new StructField("age", IntegerType, false, Metadata.empty()),
                new StructField("city", StringType, false, Metadata.empty())
        });
        List<String> keyList = Collections.singletonList(key);
        Seq<String> keySeq = getSeqString(keyList);
        CreateTableOptions kuduTableOptions = new CreateTableOptions().
                setRangePartitionColumns(keyList).
                setNumReplicas(1);
        kuduContext.createTable(kuduTable, structType, keySeq, kuduTableOptions);
    }

    private static Seq<String> getSeqString(List<String> list) {
        return scala.collection.JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
    }

    private static void createDummyData(JavaSparkContext sparkContext, SQLContext sqlContext,
                                        KuduContext kuduContext, final String table, final String kuduMasters) {
        List<Customer> customers = Arrays.asList(new Customer("jane", 30, "new york"),
                new Customer("jordan", 18, "toronto"),
                new Customer("gil", 48, "london"));
        JavaRDD<Customer> rdd = sparkContext.parallelize(customers);
        DataFrame customersDf = sqlContext.createDataFrame(rdd, Customer.class);
        kuduContext.insertRows(customersDf, table);
        System.out.println("Inserted rows.");
    }

    private static void readData(SQLContext sqlContext, final String table, final String kuduMasters) {
        Map<String, String> options = new HashMap<String, String>() {
            {
                put("kudu.table", table);
                put("kudu.master", kuduMasters);
            }
        };
        sqlContext.read().options(options).format("org.apache.kudu.spark.kudu").load().save("/tmp/save");
    }

    public static class Customer implements Serializable {

        private String name;
        private int age;
        private String city;

        private Customer(String name, int age, String city) {
            this.name = name;
            this.age = age;
            this.city = city;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }
    }

}
