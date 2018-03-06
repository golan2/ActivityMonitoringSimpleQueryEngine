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
        System.out.println("data_collector = " + dc.count());

        final long b = System.nanoTime();

        final JavaRDD<CassandraRow> dc2 = javaFunctions.cassandraTable("activity", "data_collector2");
        System.out.println("data_collector2 = " + dc2.count());

        final long c = System.nanoTime();

        System.out.println("a2b=["+nano2seconds(b-a)+"] b2c=["+nano2seconds(c-b)+"]");



    }

    private static double nano2seconds(long a2b) {
        return Math.round(a2b/1000_000_000.0*100)/100.0;    //convert to sec and round to 2 digits after the decimal point
    }

}
