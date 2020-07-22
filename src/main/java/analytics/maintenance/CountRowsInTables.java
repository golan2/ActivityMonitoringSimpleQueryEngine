package analytics.maintenance;

import analytics.SparkShared;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

public class CountRowsInTables {

    private static final String APP_NAME = "Count rows in tables";
    private static final String[] TABLES = {"data_collector", "hourly_aggregator", "daily_aggregator"};

    public static void main(String[] args) {
        final StringBuilder buf = new StringBuilder();
        SparkContext sparkContext = null;

        try {
            sparkContext = SparkShared.createSparkContext(APP_NAME);
            final SparkContextJavaFunctions javaFunctions = CassandraJavaUtil.javaFunctions(sparkContext);
            for (String tableName : TABLES) {
                final long a = System.nanoTime();
                final long size = countRowsInTable(javaFunctions, tableName);
                final long b = System.nanoTime();
                buf.append("\t"+tableName + " size=["+size+"] time=["+nano2seconds(b-a)+"]\n");
            }
        } catch (Exception e) {
            System.out.println("Partial table Sizes due to an error:\n" + buf.toString());
            throw e;
        } finally {
            if (sparkContext!=null) sparkContext.stop();
        }

        System.out.println("Table Sizes:\n" + buf.toString());
    }

    private static long countRowsInTable(SparkContextJavaFunctions javaFunctions, String data_collector1) {
        final JavaRDD<CassandraRow> dc = javaFunctions.cassandraTable("activity", data_collector1);
        return dc.count();
    }

    private static double nano2seconds(long a2b) {
        return Math.round(a2b/1000_000_000.0*100)/100.0;    //convert to sec and round to 2 digits after the decimal point
    }

}
