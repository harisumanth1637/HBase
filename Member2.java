import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Member2 {

    public static String Table_Name = "CovidData";

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = new HTable(conf, Table_Name);

        // Task a: Compare the number of tweets posted by verified and non-verified users
        compareVerifiedNonVerifiedTweets(hTable);

        // Task b: Select the influential users who have more than 10,000 followers and are verified
        selectInfluentialUsers(hTable);
    }

    // Task a: Compare the number of tweets posted by verified users vs non-verified users
    private static void compareVerifiedNonVerifiedTweets(HTable hTable) throws IOException {
        int verifiedTweetsCount = 0;
        int nonVerifiedTweetsCount = 0;

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("Users"));  // For user_verified
        scan.addFamily(Bytes.toBytes("Tweets")); // For tweet text
        ResultScanner scanner = hTable.getScanner(scan);

        try {
            for (Result result : scanner) {
                String userVerified = Bytes.toString(result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_verified")));
                String tweetText = Bytes.toString(result.getValue(Bytes.toBytes("Tweets"), Bytes.toBytes("text")));

                // Skip if tweet text is null or empty
                if (tweetText == null || tweetText.isEmpty()) {
                    continue;
                }

                // Count tweets by verified or non-verified users
                if ("TRUE".equalsIgnoreCase(userVerified)) {
                    verifiedTweetsCount++;
                } else {
                    nonVerifiedTweetsCount++;
                }
            }
        } finally {
            scanner.close();
        }

        // Write the results to a file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("compareVerifiedNonVerifiedTweets.txt"))) {
            writer.write("Total Tweets by Verified Users: " + verifiedTweetsCount);
            writer.newLine();
            writer.write("Total Tweets by Non-Verified Users: " + nonVerifiedTweetsCount);
            writer.newLine();
        }

        System.out.println("Task a: Comparison written to file 'compareVerifiedNonVerifiedTweets.txt'");
    }

    // Task b: Select the influential users who have more than 10,000 followers and are verified
    private static void selectInfluentialUsers(HTable hTable) throws IOException {
        Map<String, Integer> influentialUsers = new HashMap<>();  // Use Map to store usernames and their followers count
        Set<String> seenUsernames = new HashSet<>(); // Track already seen usernames to prevent duplicates
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("Users"));  // Add Users family
        scan.addFamily(Bytes.toBytes("Extra"));  // Add Extra family

        ResultScanner scanner = hTable.getScanner(scan);
        try {
            for (Result result : scanner) {
                // Extract the user_verified and user_followers
                String userVerified = Bytes.toString(result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_verified")));
                String userFollowersStr = Bytes.toString(result.getValue(Bytes.toBytes("Extra"), Bytes.toBytes("user_followers")));

                // Skip if either user_verified or user_followers is missing
                if (userVerified == null || userFollowersStr == null) {
                    continue;  // Skip this row if required fields are missing
                }

                // Convert followers to integer
                int userFollowers;
                try {
                    userFollowers = Integer.parseInt(userFollowersStr);  // Parse followers count
                } catch (NumberFormatException e) {
                    continue;  // Skip this row if followers count is not a valid number
                }

                // Check if user is verified and has more than 10,000 followers
                if ("TRUE".equalsIgnoreCase(userVerified) && userFollowers > 10000) {
                    String userName = Bytes.toString(result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_name")));
                    if (userName != null && !seenUsernames.contains(userName)) {
                        seenUsernames.add(userName);  // Ensure no duplicates
                        influentialUsers.put(userName, userFollowers);  // Add to the map with followers count
                    }
                }
            }
        } finally {
            scanner.close();
        }

        // Write the influential users and their follower counts to a file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("selectInfluentialUsers.txt"))) {
            writer.write("Influential Users with more than 10,000 followers and verified:");
            writer.newLine();

            for (Map.Entry<String, Integer> entry : influentialUsers.entrySet()) {
                writer.write("Username: " + entry.getKey() + ", Followers: " + entry.getValue());
                writer.newLine();
            }
        }

        System.out.println("Task b: Influential users written to 'selectInfluentialUsers.txt'");
    }

}
