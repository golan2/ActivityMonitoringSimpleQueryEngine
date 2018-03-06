package analytics.maintenance;

import analytics.SparkShared;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import org.apache.spark.api.java.JavaRDD;

public class CountDataCollector {

    private static final String APP_NAME = "Count rows in data_collector";

    public static void main(String[] args) {
        final SparkContextJavaFunctions javaFunctions = SparkShared.createJavaFunctions(APP_NAME);

        final long a = System.nanoTime();

        final JavaRDD<CassandraRow> dc = javaFunctions.cassandraTable("activity", "data_collector");
        final long data_collector = dc.count();
        final long b = System.nanoTime();
//        final JavaRDD<CassandraRow> dc2 = javaFunctions.cassandraTable("activity", "data_collector2");
//        final long data_collector2 = dc2.count();
        final long c = System.nanoTime();
        final JavaRDD<CassandraRow> hag = javaFunctions.cassandraTable("activity", "hourly_aggregator");
        final long hourly_aggregator = hag.count();
        final long d = System.nanoTime();
        final JavaRDD<CassandraRow> dag = javaFunctions.cassandraTable("activity", "daily_aggregator");
        final long daily_aggregator = dag.count();
        final long e = System.nanoTime();

        System.out.println("data_collector size=["+data_collector+"] time=["+nano2seconds(b-a)+"]");
//        System.out.println("data_collector2 size=["+data_collector2+"] time=["+nano2seconds(c-b)+"]");
        System.out.println("hourly_aggregator size=["+hourly_aggregator+"] time=["+nano2seconds(d-c)+"]");
        System.out.println("daily_aggregator size=["+daily_aggregator+"] time=["+nano2seconds(e-d)+"]");


    }

    private static double nano2seconds(long a2b) {
        return Math.round(a2b/1000_000_000.0*100)/100.0;    //convert to sec and round to 2 digits after the decimal point
    }

}
