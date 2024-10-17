import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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

import java.io.FileReader;
import java.io.IOException;

public class InsertData extends Configured implements Tool {

    public static String Table_Name = "CovidData";

    @Override
    public int run(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        @SuppressWarnings("resource")
        HBaseAdmin admin = new HBaseAdmin(conf);

        // Check if the table exists, if not, create it
        if (!admin.tableExists(Table_Name)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(Table_Name);
            HColumnDescriptor usersColumnFamily = new HColumnDescriptor("Users");
            usersColumnFamily.setMaxVersions(4);  // Allow versioning with max 4 versions
            hTableDescriptor.addFamily(usersColumnFamily);
            hTableDescriptor.addFamily(new HColumnDescriptor("Tweets"));
            hTableDescriptor.addFamily(new HColumnDescriptor("Extra"));
            admin.createTable(hTableDescriptor);
            System.out.println("Table created successfully.");
        }

        int row_count = 0;
        String csvFilePath = "covid19_tweets.csv";  

        try (FileReader reader = new FileReader(csvFilePath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withTrim())) {

            for (CSVRecord csvRecord : csvParser) {
                // Process each CSV record
                processLine(csvRecord, conf);
                row_count++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        // Print the number of rows inserted
        System.out.println("Inserted " + row_count + " rows.");

        return 0;
    }

    // Process each CSV record and insert data into HBase
    private static void processLine(CSVRecord record, Configuration conf) throws IOException {
        // Extract data from the record
        String user_name = record.get("user_name");
        String user_location = record.get("user_location");
        String user_description = record.get("user_description");
        String user_created = record.get("user_created");
        String user_followers = record.get("user_followers");
        String user_friends = record.get("user_friends");
        String user_favourites = record.get("user_favourites");
        String user_verified = record.get("user_verified");
        String date = record.get("date");
        String tweet_text = record.get("text");
        String hashtags = record.get("hashtags");
        String source = record.get("source");
        String is_retweet = record.get("is_retweet");

        // Skip row if username is missing and print a message
        if (user_name == null || user_name.isEmpty()) {
            System.out.println("Skipping row due to missing username.");
            return;  // Skip the row
        }



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
            put.add(Bytes.toBytes("Extra"), Bytes.toBytes("user_followers"), Bytes.toBytes(user_followers));
            put.add(Bytes.toBytes("Extra"), Bytes.toBytes("user_friends"), Bytes.toBytes(user_friends));
            put.add(Bytes.toBytes("Extra"), Bytes.toBytes("user_favourites"), Bytes.toBytes(user_favourites));

            // Insert into Tweets family
            put.add(Bytes.toBytes("Tweets"), Bytes.toBytes("text"), Bytes.toBytes(tweet_text));
            put.add(Bytes.toBytes("Tweets"), Bytes.toBytes("hashtags"), Bytes.toBytes(hashtags));
            put.add(Bytes.toBytes("Tweets"), Bytes.toBytes("is_retweet"), Bytes.toBytes(is_retweet));

            // Insert into Extra family
            put.add(Bytes.toBytes("Extra"), Bytes.toBytes("source"), Bytes.toBytes(source));
            put.add(Bytes.toBytes("Extra"), Bytes.toBytes("user_location"), Bytes.toBytes(user_location));
            put.add(Bytes.toBytes("Extra"), Bytes.toBytes("date"), Bytes.toBytes(date));

            hTable.put(put);
            
        } catch (IOException e) {
            e.printStackTrace();
        }
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
