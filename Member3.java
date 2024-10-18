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

public class Member3 {

    public static String tableName = "CovidData";

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        HTable hTable = new HTable(config, tableName);

        // Task 1: Compare the number of tweets posted by verified and non-verified users
        analyzeTweetCountsByVerificationStatus(hTable);

        // Task 2: Select the influential users who have more than 10,000 followers and are verified
        identifyInfluentialUsers(hTable);
    }

    // Task 1: Compare the number of tweets posted by verified users vs non-verified users
    private static void analyzeTweetCountsByVerificationStatus(HTable hTable) throws IOException {
        int verifiedUserTweetCount = 0;
        int nonVerifiedUserTweetCount = 0;

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("Users"));  // For user_verified
        scan.addFamily(Bytes.toBytes("Tweets")); // For tweet content
        ResultScanner resultScanner = hTable.getScanner(scan);

        try {
            for (Result result : resultScanner) {
                String isUserVerified = Bytes.toString(result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_verified")));
                String tweetContent = Bytes.toString(result.getValue(Bytes.toBytes("Tweets"), Bytes.toBytes("text")));

                // Skip if tweet content is null or empty
                if (tweetContent == null || tweetContent.isEmpty()) {
                    continue;
                }

                // Count tweets by verified or non-verified users
                if ("TRUE".equalsIgnoreCase(isUserVerified)) {
                    verifiedUserTweetCount++;
                } else {
                    nonVerifiedUserTweetCount++;
                }
            }
        } finally {
            resultScanner.close();
        }

        // Write the results to a file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("tweetCountsByVerificationStatus.txt"))) {
            writer.write("Total Tweets by Verified Users: " + verifiedUserTweetCount);
            writer.newLine();
            writer.write("Total Tweets by Non-Verified Users: " + nonVerifiedUserTweetCount);
            writer.newLine();
        }

        System.out.println("Task 1: Comparison written to file 'tweetCountsByVerificationStatus.txt'");
    }

    // Task 2: Select the influential users who have more than 10,000 followers and are verified
    private static void identifyInfluentialUsers(HTable hTable) throws IOException {
        Map<String, Integer> influentialUserMap = new HashMap<>();  // Use Map to store usernames and their followers count
        Set<String> processedUsernames = new HashSet<>(); // Track already processed usernames to prevent duplicates
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("Users"));  // Add Users family
        scan.addFamily(Bytes.toBytes("Extra"));  // Add Extra family

        ResultScanner resultScanner = hTable.getScanner(scan);
        try {
            for (Result result : resultScanner) {
                // Extract the user_verified and user_followers
                String isUserVerified = Bytes.toString(result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_verified")));
                String userFollowersString = Bytes.toString(result.getValue(Bytes.toBytes("Extra"), Bytes.toBytes("user_followers")));

                // Skip if either user_verified or user_followers is missing
                if (isUserVerified == null || userFollowersString == null) {
                    continue;  // Skip this row if required fields are missing
                }

                // Convert followers to integer
                int userFollowers;
                try {
                    userFollowers = Integer.parseInt(userFollowersString);  // Parse followers count
                } catch (NumberFormatException e) {
                    continue;  // Skip this row if followers count is not a valid number
                }

                // Check if user is verified and has more than 10,000 followers
                if ("TRUE".equalsIgnoreCase(isUserVerified) && userFollowers > 10000) {
                    String userName = Bytes.toString(result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_name")));
                    if (userName != null && !processedUsernames.contains(userName)) {
                        processedUsernames.add(userName);  // Ensure no duplicates
                        influentialUserMap.put(userName, userFollowers);  // Add to the map with followers count
                    }
                }
            }
        } finally {
            resultScanner.close();
        }

        // Write the influential users and their follower counts to a file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("influentialUsers.txt"))) {
            writer.write("Influential Users with more than 10,000 followers and verified:");
            writer.newLine();

            for (Map.Entry<String, Integer> entry : influentialUserMap.entrySet()) {
                writer.write("Username: " + entry.getKey() + ", Followers: " + entry.getValue());
                writer.newLine();
            }
        }

        System.out.println("Task 2: Influential users written to 'influentialUsers.txt'");
    }
}
