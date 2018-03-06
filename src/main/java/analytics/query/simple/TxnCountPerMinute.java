package analytics.query.simple;

import com.datastax.driver.core.*;

import java.io.Closeable;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SuppressWarnings("MagicConstant")
public class TxnCountPerMinute implements Closeable {

    private static final String TABLE = "data_collector";

    private static final String SELECT_QUERY_TEMPLATE =
            "SELECT minutes, count(1) " +
                    "FROM " + TABLE + " " +
                    "WHERE year=%d and month=%d and day=%d and hour=%d " +
                    "AND user_bucket='%s' and project_bucket='%s' " +
                    "AND user_id='%s' and project_id='%s' and environment='%s' " +
                    "AND minutes>=_T_B_D_ AND minutes<_T_B_D_ " +
                    "GROUP BY year, month, day, hour, user_bucket, project_bucket, user_id, project_id, environment,minutes;";



    private final Cluster cluster;
    private final Session session;


    public TxnCountPerMinute(Collection<InetAddress> hosts, String keyspace) {
        this.cluster = initCluster(hosts);
        this.session = cluster.connect(keyspace);
    }

    @Override
    public void close() {
        try {
            this.session.close();
            this.cluster.close();
        }
        catch (Exception ignore) {}
    }

    /**
     * Calculate how many transactions per minute we had in a given hour.
     * The query is per single context.
     * The query will return data for the 60 minutes from the given time parameter (including) and backward.
     *
     * @param context user/project/etc.
     * @param time the last minute that you want to include in the result
     * @return 60 pairs holding the number of transactions per minute
     * @throws InterruptedException well don't interrupt me! :-)
     * @throws MultiProblemException in case some or all of the queries to Cassandra failed you will have here list of problems
     */
    public List<Map.Entry<Date, Long>> execute(ContextFields context, TimeFields time) throws InterruptedException, MultiProblemException {

        final ArrayList<Worker> workers = new ArrayList<>();
        final long[] counters = new long[60];

        //Workers for Current Hour
        final List<ResultSetFuture> currentFutures = new ArrayList<>();
        final List<String> current  = cqlsForCurrentHour(time, context);
        current.forEach(cql -> currentFutures.add(session.executeAsync(cql)));
        currentFutures.forEach(future -> workers.add( new Worker(future, counters) ));

        //Workers for Previous Hour
        final List<ResultSetFuture> previousFutures = new ArrayList<>();
        final List<String> previous = cqlsForPreviousHour(time, context);
        previous.forEach(cql -> previousFutures.add(session.executeAsync(cql)));
        previousFutures.forEach(future -> workers.add(new Worker(future, counters)));

        //Execute all and wait for them to end
        final ExecutorService tpe = Executors.newFixedThreadPool(2);
        workers.forEach(tpe::submit);
        tpe.shutdown();
        tpe.awaitTermination(120, TimeUnit.SECONDS);

        //Check for errors
        final List<Exception> problems = workers.stream().filter(Worker::failed).map(Worker::getError).collect(Collectors.toList());
        if (!problems.isEmpty()) {
            throw new MultiProblemException(problems);
        }

        return convertToPairs(time, counters);
    }

    /**
     * Expected form of result is pairs of Date and Count.
     * What we have is a simple array with the 60 values we need.
     * So we need to go over the array and attach relevant date to it as a pair.
     * @param time as given to the query
     * @param counters as was calculated
     * @return the list of pairs
     */
    private static List<Map.Entry<Date, Long>> convertToPairs(TimeFields time, long[] counters) {
        final List<Map.Entry<Date, Long>> result = new ArrayList<>();

        Calendar c = Calendar.getInstance();
        c.set(time.year, time.month-1, time.day, time.hour, time.minute);
        c.add(Calendar.HOUR_OF_DAY, -1);

        //previous
        for (int i = time.minute ; i<60 ; i++) {
            result.add(new AbstractMap.SimpleImmutableEntry<>(c.getTime(), counters[i]));
            c.add(Calendar.MINUTE, 1);
        }

        //current
        for (int j=0 ; j<time.minute ; j++) {
            result.add(new AbstractMap.SimpleImmutableEntry<>(c.getTime(), counters[j]));
            c.add(Calendar.MINUTE, 1);
        }
        return result;
    }


    private static Cluster initCluster(Collection<InetAddress> hosts) {
        return Cluster.builder()
                .addContactPoints(hosts)
                .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(30_000))
                .build();
    }

    /**
     * Get a list of CQLs to invoke several queries in parallel to bring all the data for the required time range.
     * The time range is for the given YMDH and the minutes range.
     */
    private static List<String> getCqls(TimeFields time, ContextFields context, int fromMinutes, int toMinutes) {
        final String partialCql = String.format(SELECT_QUERY_TEMPLATE,
                time.year, time.month, time.day, time.hour,
                context.user_bucket, context.project_bucket, context.user_id, context.project_id, context.environment)
                .replaceAll("_T_B_D_", "%d");

        List<String> cqls = new ArrayList<>();
        cqls.add(String.format(partialCql, fromMinutes, toMinutes));     //todo: consider breaking into multi queries
        return cqls;
    }

    private static List<String> cqlsForCurrentHour(TimeFields time, ContextFields context) {
        return getCqls(time, context, 0, time.minute);
    }

    private static List<String> cqlsForPreviousHour(TimeFields time, ContextFields context) {
        //calculate previous hour from the given "time"
        Calendar c = Calendar.getInstance();
        c.set(time.year, time.month-1, time.day, time.hour, 0);
        c.add(Calendar.HOUR_OF_DAY, -1);
        return getCqls(new TimeFields(c), context, time.minute, 60);
    }

    /**
     * A Runnable to extract the counters from the given {@link ResultSetFuture}.
     * Result is put into the {@link #counters} parameter in the correct place (minute "0" as the first place of the array)
     */
    private static class Worker implements Runnable {
        private final ResultSetFuture future;
        final long[] counters;
        private Exception error = null;

        Worker(ResultSetFuture future, long[] counters) {
            this.counters = counters;
            this.future = future;
        }

        @Override
        public void run() {
            try {
                final ResultSet rows = this.future.getUninterruptibly();
                for (Row row : rows) {
                    counters[row.getInt(0)]=row.getLong(1);
                }
            } catch (Exception e) {
                this.error = e;
            }
        }

        Exception getError() {
            return error;
        }

        boolean failed() {
            return this.error!=null;
        }
    }


    public static class TimeFields {
        private final int     year;
        private final int     month;
        private final int     day;
        private final int     hour;
        private final int     minute;

        public TimeFields(int year, int month, int day, int hour, int minute) {
            this.year   = year;
            this.month  = month;
            this.day    = day;
            this.hour   = hour;
            this.minute = minute;
        }

        private TimeFields(Calendar c) {
            this.year   = c.get(Calendar.YEAR        );
            this.month  = c.get(Calendar.MONTH) + 1;
            this.day    = c.get(Calendar.DAY_OF_MONTH);
            this.hour   = c.get(Calendar.HOUR_OF_DAY );
            this.minute = c.get(Calendar.MINUTE      );
        }
    }

    public static class ContextFields {
        private final String  user_bucket;
        private final String  project_bucket;
        private final String  user_id;
        private final String  project_id;
        private final String  environment;

        public ContextFields(String user_bucket, String project_bucket, String user_id, String project_id, String environment) {
            this.user_bucket = user_bucket;
            this.project_bucket = project_bucket;
            this.user_id = user_id;
            this.project_id = project_id;
            this.environment = environment;
        }
    }


    public static class MultiProblemException extends Exception {
        private final List<Exception> problems;

        MultiProblemException(List<Exception> problems) {
            super("Several (problems) when invoking queries to Cassandra.");
            this.problems = problems;
        }

        public List<Exception> getProblems() {
            return problems;
        }
    }
}
