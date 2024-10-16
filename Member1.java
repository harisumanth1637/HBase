import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
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

    // Query to count the number of tweets per location
    private static void queryTweetsByLocation(HTable hTable) throws IOException {
        Map<String, Integer> locationCountMap = new HashMap<>();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("Extra")); // Retrieve "Extra" family for user_location
        scan.addFamily(Bytes.toBytes("Tweets")); // Retrieve "Tweets" family for tweet text
        ResultScanner scanner = hTable.getScanner(scan);

        try {
            for (Result result : scanner) {
                String userLocation = Bytes.toString(result.getValue(Bytes.toBytes("Extra"), Bytes.toBytes("user_location")));
                String tweetText = Bytes.toString(result.getValue(Bytes.toBytes("Tweets"), Bytes.toBytes("text")));
                
                // Skip if either userLocation or tweetText is null or empty
                if (userLocation != null && !userLocation.isEmpty() && tweetText != null && !tweetText.isEmpty()) {
                    locationCountMap.put(userLocation, locationCountMap.getOrDefault(userLocation, 0) + 1);
                }
            }
        } finally {
            scanner.close();
        }

        // Write the results to a file
        writeMapToFile("TweetsByLocation.txt", locationCountMap);
    }

    // Query to count the number of tweets per location for verified users
    private static void queryVerifiedTweetsByLocation(HTable hTable) throws IOException {
        Map<String, Integer> locationCountMap = new HashMap<>();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("Users"));  // Retrieve "Users" family for user_verified
        scan.addFamily(Bytes.toBytes("Extra"));  // Retrieve "Extra" family for user_location
        scan.addFamily(Bytes.toBytes("Tweets")); // Retrieve "Tweets" family for tweet text
        ResultScanner scanner = hTable.getScanner(scan);

        try {
            for (Result result : scanner) {
                String userLocation = Bytes.toString(result.getValue(Bytes.toBytes("Extra"), Bytes.toBytes("user_location")));
                String userVerified = Bytes.toString(result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_verified")));
                String tweetText = Bytes.toString(result.getValue(Bytes.toBytes("Tweets"), Bytes.toBytes("text")));
                
                // Skip if userLocation, tweetText are empty, or if user is not verified
                if (userLocation != null && !userLocation.isEmpty() && tweetText != null && !tweetText.isEmpty()
                        && "TRUE".equalsIgnoreCase(userVerified)) {
                    locationCountMap.put(userLocation, locationCountMap.getOrDefault(userLocation, 0) + 1);
                }
            }
        } finally {
            scanner.close();
        }

        // Write the results to a file
        writeMapToFile("VerifiedTweetsByLocation.txt", locationCountMap);
    }

    // Query to retrieve tweets containing hashtag #COVID19 and posted from an Android device
    private static void queryCovid19AndroidTweets(HTable hTable) throws IOException {
        List<String> covid19Tweets = new ArrayList<>();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("Tweets")); // Retrieve "Tweets" family for hashtags and tweet text
        scan.addFamily(Bytes.toBytes("Extra"));  // Retrieve "Extra" family for source
        ResultScanner scanner = hTable.getScanner(scan);

        try {
            for (Result result : scanner) {
                String hashtags = Bytes.toString(result.getValue(Bytes.toBytes("Tweets"), Bytes.toBytes("hashtags")));
                String source = Bytes.toString(result.getValue(Bytes.toBytes("Extra"), Bytes.toBytes("source")));
                String tweetText = Bytes.toString(result.getValue(Bytes.toBytes("Tweets"), Bytes.toBytes("text")));

                // Skip if tweetText is null or empty
                if (hashtags != null && hashtags.contains("COVID19") && source != null && source.contains("Android")
                        && tweetText != null && !tweetText.isEmpty()) {
                    covid19Tweets.add(tweetText);
                }
            }
        } finally {
            scanner.close();
        }

        // Write the results to a file
        writeListToFile("Covid19AndroidTweets.txt", covid19Tweets);
    }

    // Helper method to sort and write map to file
    private static void writeMapToFile(String fileName, Map<String, Integer> map) throws IOException {
        List<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());

        // Sort the list in descending order of tweet count
        list.sort(new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
                return e2.getValue().compareTo(e1.getValue());
            }
        });

        // Write the sorted results to a text file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            for (Map.Entry<String, Integer> entry : list) {
                writer.write(entry.getKey() + ": " + entry.getValue());
                writer.newLine();
            }
        }
    }

    // Helper method to write a list of strings to a file
    private static void writeListToFile(String fileName, List<String> list) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            for (String tweet : list) {
                writer.write(tweet);
                writer.newLine();
            }
        }
    }
}
