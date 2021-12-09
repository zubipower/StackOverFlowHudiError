import com.google.common.collect.Lists;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Date;
import java.sql.Timestamp;

public class StackOverflowErrorTest {

    public static void main(String[] args) {

        // give stack overflow

        //int numberOfFields = 2000;
        //String writeOperation = "bulk_insert";

        /*java.lang.StackOverflowError
        at java.io.ObjectOutputStream.defaultWriteFields(ObjectOutputStream.java:1522)
        at java.io.ObjectOutputStream.writeSerialData(ObjectOutputStream.java:1509)*/

        // works

        //int numberOfFields = 2000;
        //String writeOperation = "insert";

        int numberOfFields = 2000;
        String writeOperation = "upsert";

        try(SparkSession sparkSession = hudiSession()) {
            StructType schema = createTestSchema(numberOfFields);
            Dataset<Row> dataset = sparkSession.createDataFrame(Lists.newArrayList(createRow(numberOfFields)), schema);

            dataset.write().format("hudi").
                    option("hoodie.table.name", "test").
                    option("hoodie.datasource.write.recordkey.field", "id").
                    option("hoodie.datasource.write.precombine.field", "last_update_time").
                    option("hoodie.datasource.write.partitionpath.field", "creation_date").
                    option("hoodie.datasource.write.operation", writeOperation).
                    mode("overwrite").save("c:/tmp");
        }

    }

    public static Row createRow(int numberOfFields) {
        Object[] values = new Object[numberOfFields +3];
        values[0] = "1";
        Date date = Date.valueOf("2021-01-01");
        values[1] = date;
        values[2] = new Timestamp(date.getTime());
        for(int j = 0; j< numberOfFields; j++){
            values[j+3] = "value_"+j;
        }
        return RowFactory.create(values);
    }

    public static StructType createTestSchema(int numberOfFields) {
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("creation_date", DataTypes.DateType, false, Metadata.empty()),
                new StructField("last_update_time", DataTypes.TimestampType, false, Metadata.empty())
        });

        for(int i = 0; i< numberOfFields; i++){
            String fieldName = "field"+i;
            schema = schema.add(
                    new StructField(fieldName, DataTypes.StringType, false, Metadata.empty())
            );
        }
        return schema;
    }



    public static SparkSession hudiSession(){

        System.setProperty("HADOOP_USER_NAME", "test");

        SparkSession.Builder builder = SparkSession.builder()

                .config("spark.master", "local[*]")
                .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")

                .config("spark.kryoserializer.buffer.max","512m")

                .config("spark.sql.hive.convertMetastoreParquet","false")
                .config("spark.shuffle.service.enabled","true")
                .config("spark.rdd.compress","true")
                .config("spark.rdd.compresspark.executor.instances"," 300")
                .config("spark.driver.maxResultSize", "2g")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "6g")
                .config("spark.executor.cores", "1")


                .config("spark.submit.deployMode", "cluster")
                .config("spark.task.cpus", "1")
                .config("spark.task.maxFailures", "4")

                .config("spark.driver.extraJavaOptions", "-XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCTimeStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump.hprof")
                .config("spark.executor.extraJavaOptions", "-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump.hprof")

                .appName("hudi");
        return builder.getOrCreate();
    }

}
