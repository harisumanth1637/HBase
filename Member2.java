import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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
        Set<String> uniqueInfluentialUsers = new HashSet<>();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("Users"));  // For user_verified and user_followers
        scan.addFamily(Bytes.toBytes("Extra"));  // For followers count
        ResultScanner scanner = hTable.getScanner(scan);

        try {
            for (Result result : scanner) {
                String userName = Bytes.toString(result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_name")));
                String userVerified = Bytes.toString(result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_verified")));
                String userFollowersStr = Bytes.toString(result.getValue(Bytes.toBytes("Extra"), Bytes.toBytes("user_followers")));

                // Skip if user is not verified or if followers data is invalid
                if (!"TRUE".equalsIgnoreCase(userVerified) || userFollowersStr == null || userFollowersStr.isEmpty()) {
                    continue;
                }

                int userFollowers;
                try {
                    userFollowers = Integer.parseInt(userFollowersStr);
                } catch (NumberFormatException e) {
                    continue; // Skip if followers count is not a valid number
                }

                // Add to influential users if followers > 10,000
                if (userFollowers > 10,000) {
                    uniqueInfluentialUsers.add(userName);
                }
            }
        } finally {
            scanner.close();
        }

        // Write the influential users to a file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("selectInfluentialUsers.txt"))) {
            writer.write("Influential Verified Users with >10,000 Followers:");
            writer.newLine();
            for (String user : uniqueInfluentialUsers) {
                writer.write(user);
                writer.newLine();
            }
        }

        System.out.println("Task b: Influential users written to file 'selectInfluentialUsers.txt'");
    }
}
