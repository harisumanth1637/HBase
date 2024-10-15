import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InsertData extends Configured implements Tool {

    public static String Table_Name = "CovidData";

    @Override
    public int run(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);

        // Check if the table exists, if not, create it
        if (!admin.tableExists(Table_Name)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(Table_Name);
            hTableDescriptor.addFamily(new HColumnDescriptor("Users"));
            hTableDescriptor.addFamily(new HColumnDescriptor("Tweets"));
            hTableDescriptor.addFamily(new HColumnDescriptor("Extra"));
            admin.createTable(hTableDescriptor);
            System.out.println("Table created successfully.");
        }

        int row_count = 0;

        try (BufferedReader br = new BufferedReader(new FileReader("top_covid19_tweets.csv"))) {
            StringBuilder currentLine = new StringBuilder();
            String line;

            // Assuming the first line is the header, skip it
            br.readLine();

            while ((line = br.readLine()) != null) {
                // Append line to currentLine buffer
                currentLine.append(line);

                // Parse the line (handling multiline fields)
                String[] data = parseCSVLine(currentLine.toString());

                // Ensure there are 13 fields
                if (data.length == 13) {
                    processLine(data, conf);
                    currentLine.setLength(0);  // Clear buffer for the next record
                } else {
                    // If less than 13 fields, continue appending to handle multiline fields
                    currentLine.append("\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Print the number of rows inserted
        System.out.println("Inserted " + row_count + " rows.");

        return 0;
    }

    // Helper method to parse a CSV line while handling commas in quotes and newlines inside fields
    private static String[] parseCSVLine(String line) {
        // Use regex to split by commas but ignore commas inside quotes
        return line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);  // Split by commas while ignoring commas inside quotes
    }

    // Process each line, print and insert data into HBase
    private static void processLine(String[] data, Configuration conf) throws IOException {
        // Clean and trim each field to remove unwanted whitespace
        for (int i = 0; i < data.length; i++) {
            data[i] = data[i].replace("\n", " ").replace("\r", " ").trim();
        }

        // Extract data from the line
        String user_name = data[0].isEmpty() ? "" : data[0];
        String user_location = data[1].isEmpty() ? "" : data[1];
        String user_description = data[2].isEmpty() ? "" : data[2];
        String user_created = data[3].isEmpty() ? "" : data[3];
        String user_followers = data[4].isEmpty() ? "" : data[4];
        String user_friends = data[5].isEmpty() ? "" : data[5];
        String user_favourites = data[6].isEmpty() ? "" : data[6];
        String user_verified = data[7].isEmpty() ? "" : data[7];
        String date = data[8].isEmpty() ? "" : data[8];
        String tweet_text = data[9].isEmpty() ? "" : data[9];
        String hashtags = data[10].isEmpty() ? "" : data[10];
        String source = data[11].isEmpty() ? "" : data[11];
        String is_retweet = data[12].isEmpty() ? "" : data[12];

        // Skip row if username is missing and print a message
        if (user_name.isEmpty()) {
            System.out.println("Skipping row due to missing username.");
            return;  // Skip the row
        }

        // Convert user_followers, user_friends, and user_favourites to integers, default to 0 if invalid
        int followers_count = convertToInt(user_followers);
        int friends_count = convertToInt(user_friends);
        int favourites_count = convertToInt(user_favourites);

        // Create a unique row key using user_name and the date (even if the date is empty)
        String row_key = user_name + "_" + date;
        row_key = row_key.replaceAll("\\s+", "_");  // Clean row key by replacing spaces with underscores

        // Insert data into HBase
        try (HTable hTable = new HTable(conf, Table_Name)) {
            Put put = new Put(Bytes.toBytes(row_key));

            // Insert into Users family
            put.add(Bytes.toBytes("Users"), Bytes.toBytes("user_name"), Bytes.toBytes(user_name));
            put.add(Bytes.toBytes("Users"), Bytes.toBytes("user_verified"), Bytes.toBytes(user_verified));
            put.add(Bytes.toBytes("Users"), Bytes.toBytes("user_created"), Bytes.toBytes(user_created));

            // Insert the integer values into Extra family
            put.add(Bytes.toBytes("Extra"), Bytes.toBytes("user_followers"), Bytes.toBytes(followers_count));
            put.add(Bytes.toBytes("Extra"), Bytes.toBytes("user_friends"), Bytes.toBytes(friends_count));
            put.add(Bytes.toBytes("Extra"), Bytes.toBytes("user_favourites"), Bytes.toBytes(favourites_count));

            // Insert into Tweets family
            put.add(Bytes.toBytes("Tweets"), Bytes.toBytes("text"), Bytes.toBytes(tweet_text));
            put.add(Bytes.toBytes("Tweets"), Bytes.toBytes("hashtags"), Bytes.toBytes(hashtags));
            put.add(Bytes.toBytes("Tweets"), Bytes.toBytes("is_retweet"), Bytes.toBytes(is_retweet));

            // Insert into Extra family
            put.add(Bytes.toBytes("Extra"), Bytes.toBytes("source"), Bytes.toBytes(source));
            put.add(Bytes.toBytes("Extra"), Bytes.toBytes("user_location"), Bytes.toBytes(user_location));
            put.add(Bytes.toBytes("Extra"), Bytes.toBytes("date"), Bytes.toBytes(date));

            hTable.put(put);
            System.out.println("Inserted row with key: " + row_key);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Helper method to check if a string is null or empty
    private static boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty();
    }

    // Helper method to convert strings to integers with default value of 0
    private static int convertToInt(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;  // Return 0 if conversion fails (e.g., empty string or invalid number)
        }
    }

    public static void main(String[] argv) throws Exception {
        int ret = ToolRunner.run(new InsertData(), argv);
        System.exit(ret);
    }
}
