import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Member1 {

    public static String Table_Name = "CovidData";

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = new HTable(conf, Table_Name);

        // Query for all locations and count the number of tweets per location
        queryTweetsByLocation(hTable);

        // Query for verified users and count the number of tweets per location
        queryVerifiedTweetsByLocation(hTable);

        // Query for tweets that contain the hashtag #COVID19 and are posted from an Android device
        queryCovid19AndroidTweets(hTable);
    }

    private static void queryTweetsByLocation(HTable hTable) throws IOException {
        Map<String, Integer> locationCountMap = new HashMap<>();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("Users"));
        scan.addFamily(Bytes.toBytes("Tweets"));
        ResultScanner scanner = hTable.getScanner(scan);

        try {
            for (Result result : scanner) {
                String userLocation = Bytes.toString(result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_location")));
                if (userLocation != null && !userLocation.isEmpty()) {
                    locationCountMap.put(userLocation, locationCountMap.getOrDefault(userLocation, 0) + 1);
                }
            }
        } finally {
            scanner.close();
        }

        // Sort and print the results
        printSortedMap(locationCountMap);
    }

    private static void queryVerifiedTweetsByLocation(HTable hTable) throws IOException {
        Map<String, Integer> locationCountMap = new HashMap<>();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("Users"));
        scan.addFamily(Bytes.toBytes("Tweets"));
        ResultScanner scanner = hTable.getScanner(scan);

        try {
            for (Result result : scanner) {
                String userLocation = Bytes.toString(result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_location")));
                String userVerified = Bytes.toString(result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_verified")));
                if (userLocation != null && !userLocation.isEmpty() && "TRUE".equalsIgnoreCase(userVerified)) {
                    locationCountMap.put(userLocation, locationCountMap.getOrDefault(userLocation, 0) + 1);
                }
            }
        } finally {
            scanner.close();
        }

        // Sort and print the results
        System.out.println("\nVerified Users' Tweets by Location:");
        printSortedMap(locationCountMap);
    }

    private static void queryCovid19AndroidTweets(HTable hTable) throws IOException {
        List<String> covid19Tweets = new ArrayList<>();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("Tweets"));
        scan.addFamily(Bytes.toBytes("Extra"));
        ResultScanner scanner = hTable.getScanner(scan);

        try {
            for (Result result : scanner) {
                String hashtags = Bytes.toString(result.getValue(Bytes.toBytes("Tweets"), Bytes.toBytes("hashtags")));
                String source = Bytes.toString(result.getValue(Bytes.toBytes("Extra"), Bytes.toBytes("source")));

                if (hashtags != null && hashtags.contains("#COVID19") && source != null && source.contains("Android")) {
                    String tweetText = Bytes.toString(result.getValue(Bytes.toBytes("Tweets"), Bytes.toBytes("text")));
                    covid19Tweets.add(tweetText);
                }
            }
        } finally {
            scanner.close();
        }

        // Print the results
        System.out.println("\nTweets with #COVID19 from Android devices:");
        for (String tweet : covid19Tweets) {
            System.out.println(tweet);
        }
    }

    private static void printSortedMap(Map<String, Integer> map) {
        // Convert map to a list of entries for sorting
        List<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());

        // Sort the list in descending order of tweet count
        list.sort(new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
                return e2.getValue().compareTo(e1.getValue());
            }
        });

        // Print the sorted map
        for (Map.Entry<String, Integer> entry : list) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
}
