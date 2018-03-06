package analytics.client;

import analytics.query.simple.TxnCountPerMinute;
import javafx.util.Pair;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

public class TestTxnCountPerMinute {

//    PARAMS: 2018 1 27 12 35 user_bucket project_bucket user_id project_id environment

        public static void main(String[] args) throws UnknownHostException {
            Collection<InetAddress> hosts = new ArrayList<>();
            hosts.add(InetAddress.getByName("localhost"));
            try (TxnCountPerMinute query = new TxnCountPerMinute(hosts, "bactivity")) {
                final TxnCountPerMinute.ContextFields context = new TxnCountPerMinute.ContextFields("user_bucket", "project_bucket", "user_id", "project_id", "environment");
                final TxnCountPerMinute.TimeFields time = new TxnCountPerMinute.TimeFields(2018, 1, 27, 12, 35);
                final List<Map.Entry<Date, Long>> result = query.execute(context, time);
                System.out.println(result.stream().map(e -> e.getKey() + " = " + e.getValue()).collect(Collectors.joining("\n")));
            } catch (InterruptedException e) {
                System.out.println("Interrupted during work!");
            } catch (TxnCountPerMinute.MultiProblemException e) {
                System.out.println(e.getMessage());
                System.out.println("List of problems: \n" + e.getProblems().stream().map(Throwable::getMessage).collect(Collectors.joining("\n")));
            }

        }}


