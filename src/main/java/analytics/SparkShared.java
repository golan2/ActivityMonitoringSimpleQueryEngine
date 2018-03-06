package analytics;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class SparkShared {

    public static SparkContext createSparkContext(String APP_NAME) {
        SparkConf conf = new SparkConf();
        conf.setAppName(APP_NAME);
        conf.setMaster("local[4]");
        conf.set("spark.cassandra.connection.host", "localhost");
        return new SparkContext(conf);
    }

    public static SparkContextJavaFunctions createJavaFunctions(String appName) {
        return CassandraJavaUtil.javaFunctions(createSparkContext(appName));
    }
}
