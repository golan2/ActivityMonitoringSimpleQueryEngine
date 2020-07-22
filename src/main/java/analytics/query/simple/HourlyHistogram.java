package analytics.query.simple;

import analytics.SparkShared;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import java.util.Map;

public class HourlyHistogram {

    private static final String APP_NAME = "Count how many devices";

    public static void main(String[] args) {
        final SparkContextJavaFunctions javaFunctions = CassandraJavaUtil.javaFunctions(SparkShared.createSparkContext(APP_NAME));
        final CassandraTableScanJavaRDD<CassandraRow> source = javaFunctions.cassandraTable("activity", "daily_aggregator");
        final JavaPairRDD<String, CassandraRow> rdd = source.keyBy((Function<CassandraRow, String>) cassandraRow -> cassandraRow.getString("device_id"));
        final Map<String, Long> counters = rdd.countByKey();
        System.out.println("~~~ devicesCount=["+counters.size()+"]");
        for (String deviceId : counters.keySet()) {
            System.out.println("~~~ " + deviceId + " => " + counters.get(deviceId));
        }
    }
}
