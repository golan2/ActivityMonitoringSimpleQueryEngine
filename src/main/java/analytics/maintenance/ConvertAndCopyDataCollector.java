package analytics.maintenance;

import analytics.SparkShared;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * We copy from [data_collector2] to [data_collector] and change a few fields along the way
 */
public class ConvertAndCopyDataCollector {

    private static final String APP_NAME = "Copy from data_collector2 to data_collector and map user to organization";

    public static void main(String[] args) {
        final SparkContextJavaFunctions javaFunctions = SparkShared.createJavaFunctions(APP_NAME);
        final CassandraTableScanJavaRDD<CassandraRow> source = javaFunctions.cassandraTable("activity", "data_collector2");
        final JavaRDD<DataCollector> target = source.map(new Old2NewMapper());
        CassandraJavaUtil.javaFunctions(target).writerBuilder("activity", "data_collector", CassandraJavaUtil.mapToRow(DataCollector.class)).saveToCassandra();
    }

    private static class Old2NewMapper implements Function<CassandraRow, DataCollector> {

        @Override
        public DataCollector call(CassandraRow row) {
            return new DataCollector(
                    row.getString("user_bucket"),
                    row.getString("project_bucket"),
                    row.getInt("year"),
                    row.getInt("month"),
                    row.getInt("day"),
                    row.getInt("hour"),
                    row.getString("user_id"),
                    row.getString("project_id"),
                    row.getString("environment"),
                    row.getInt("minutes"),
                    row.getInt("seconds"),
                    row.getString("device_id"),
                    0,
                    row.getString("device_firmware"),
                    row.getString("device_type"),
                    "txn_id",
                    0,
                    row.get("timestamp", CassandraJavaUtil.safeTypeConverter(Date.class)),
                    convertMap(row.getMap("user_param"))
            );
        }

        private Map<String, String> convertMap(Map<Object, Object> user_param) {
            return user_param.entrySet().stream().collect(Collectors.toMap(Object::toString, Object::toString));
        }
    }


    public static class DataCollector implements Serializable {
        Integer year;Integer month;Integer day;Integer hour;String org_bucket;String project_bucket;String org_id;String project_id;String environment;Integer minutes;Integer seconds;String device_id;Integer data_points;String device_firmware;String device_type;String transaction_id;Integer volume_size;Date timestamp;
        Map<String, String> user_param;

        public DataCollector(String org_bucket, String project_bucket, Integer year, Integer month, Integer day, Integer hour, String org_id, String project_id, String environment, Integer minutes, Integer seconds, String device_id, Integer data_points, String device_firmware, String device_type, String transaction_id, Integer volume_size, Date timestamp, Map<String, String> user_param) {
            this.org_bucket = org_bucket;
            this.month = month;
            this.day = day;
            this.year = year;
            this.hour = hour;
            this.project_bucket = project_bucket;
            this.project_id = project_id;
            this.device_firmware = device_firmware;
            this.seconds = seconds;
            this.device_type = device_type;
            this.data_points = data_points;
            this.transaction_id = transaction_id;
            this.device_id = device_id;
            this.volume_size = volume_size;
            this.environment = environment;
            this.org_id = org_id;
            this.minutes = minutes;
            this.timestamp = timestamp;
            this.user_param = user_param;
        }

        public Integer getYear() { return year; }public void setYear(Integer year) { this.year = year; }public Integer getMonth() { return month; }public void setMonth(Integer month) { this.month = month; }public Integer getDay() { return day; }public void setDay(Integer day) { this.day = day; }public Integer getHour() { return hour; }public void setHour(Integer hour) { this.hour = hour; }public String getOrg_bucket() { return org_bucket; }public void setOrg_bucket(String org_bucket) { this.org_bucket = org_bucket; }public String getProject_bucket() { return project_bucket; }public void setProject_bucket(String project_bucket) { this.project_bucket = project_bucket; }public String getOrg_id() { return org_id; }public void setOrg_id(String org_id) { this.org_id = org_id; }public String getProject_id() { return project_id; }public void setProject_id(String project_id) { this.project_id = project_id; }public String getEnvironment() { return environment; }public void setEnvironment(String environment) { this.environment = environment; }public Integer getMinutes() { return minutes; }public void setMinutes(Integer minutes) { this.minutes = minutes; }public Integer getSeconds() { return seconds; }public void setSeconds(Integer seconds) { this.seconds = seconds; }public String getDevice_id() { return device_id; }public void setDevice_id(String device_id) { this.device_id = device_id; }public Integer getData_points() { return data_points; }public void setData_points(Integer data_points) { this.data_points = data_points; }public String getDevice_firmware() { return device_firmware; }public void setDevice_firmware(String device_firmware) { this.device_firmware = device_firmware; }public String getDevice_type() { return device_type; }public void setDevice_type(String device_type) { this.device_type = device_type; }public String getTransaction_id() { return transaction_id; }public void setTransaction_id(String transaction_id) { this.transaction_id = transaction_id; }public Integer getVolume_size() { return volume_size; }public void setVolume_size(Integer volume_size) { this.volume_size = volume_size; }public Date getTimestamp() { return timestamp; }public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }public Map<String, String> getUser_param() { return user_param; }public void setUser_param(Map<String, String> user_param) { this.user_param = user_param; }


        @Override
        public String toString() {
            return "DataCollector{" +
                    "year=" + year +
                    ", month=" + month +
                    ", day=" + day +
                    ", hour=" + hour +
                    ", org_bucket='" + org_bucket + '\'' +
                    ", project_bucket='" + project_bucket + '\'' +
                    ", org_id='" + org_id + '\'' +
                    ", project_id='" + project_id + '\'' +
                    ", environment='" + environment + '\'' +
                    ", minutes=" + minutes +
                    ", seconds=" + seconds +
                    ", device_id='" + device_id + '\'' +
                    ", data_points=" + data_points +
                    ", device_firmware='" + device_firmware + '\'' +
                    ", device_type='" + device_type + '\'' +
                    ", transaction_id='" + transaction_id + '\'' +
                    ", volume_size=" + volume_size +
                    ", timestamp=" + timestamp +
                    ", user_param=" + user_param +
                    '}';
        }
    }
}

