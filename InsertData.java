import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertData {

    public static String Table_Name = "TwitterData";
    
    public static void main(String[] args) throws IOException {
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
        
        HTable hTable = new HTable(conf, Table_Name);

        // Reading the dataset (CSV file)
        BufferedReader br = new BufferedReader(new FileReader("covid19_tweets.csv"));
        String line;
        
        // Assuming the first line is the header, skip it
        br.readLine();
        
        while ((line = br.readLine()) != null) {
            // Split the line by commas (CSV format)
            String[] data = line.split(",");
            if (data.length < 13) continue;
            
            // Extract data from the line (CSV format)
            String user_name = data[0];
            String user_location = data[1];
            String user_description = data[2];
            String user_created = data[3];
            String user_followers = data[4];
            String user_friends = data[5];
            String user_favourites = data[6];
            String user_verified = data[7];
            String date = data[8];
            String tweet_text = data[9];
            String hashtags = data[10];  // This could be empty, handle as null
            String source = data[11];
            String is_retweet = data[12];
            
            // Creating a unique row key using user_name and the date to ensure uniqueness
            String row_key = user_name + "_" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
            Put put = new Put(Bytes.toBytes(row_key));
            
            // Insert into Users family
            if (!isNullOrEmpty(user_name)) {
                put.add(Bytes.toBytes("Users"), Bytes.toBytes("user_name"), Bytes.toBytes(user_name));
            }
            if (!isNullOrEmpty(user_verified)) {
                put.add(Bytes.toBytes("Users"), Bytes.toBytes("user_verified"), Bytes.toBytes(user_verified));
            }
            if (!isNullOrEmpty(user_created)) {
                put.add(Bytes.toBytes("Users"), Bytes.toBytes("user_created"), Bytes.toBytes(user_created));
            }
            
            // Insert into Tweets family
            if (!isNullOrEmpty(tweet_text)) {
                put.add(Bytes.toBytes("Tweets"), Bytes.toBytes("text"), Bytes.toBytes(tweet_text));
            }
            if (!isNullOrEmpty(hashtags)) {
                put.add(Bytes.toBytes("Tweets"), Bytes.toBytes("hashtags"), Bytes.toBytes(hashtags));
            }
            if (!isNullOrEmpty(is_retweet)) {
                put.add(Bytes.toBytes("Tweets"), Bytes.toBytes("is_retweet"), Bytes.toBytes(is_retweet));
            }
            
            // Insert into Extra family
            if (!isNullOrEmpty(source)) {
                put.add(Bytes.toBytes("Extra"), Bytes.toBytes("source"), Bytes.toBytes(source));
            }
            if (!isNullOrEmpty(user_location)) {
                put.add(Bytes.toBytes("Extra"), Bytes.toBytes("user_location"), Bytes.toBytes(user_location));
            }
            if (!isNullOrEmpty(user_followers)) {
                put.add(Bytes.toBytes("Extra"), Bytes.toBytes("user_followers"), Bytes.toBytes(user_followers));
            }
            if (!isNullOrEmpty(user_friends)) {
                put.add(Bytes.toBytes("Extra"), Bytes.toBytes("user_friends"), Bytes.toBytes(user_friends));
            }
            if (!isNullOrEmpty(user_favourites)) {
                put.add(Bytes.toBytes("Extra"), Bytes.toBytes("user_favourites"), Bytes.toBytes(user_favourites));
            }
            if (!isNullOrEmpty(date)) {
                put.add(Bytes.toBytes("Extra"), Bytes.toBytes("date"), Bytes.toBytes(date));
            }
            
            // Put the data into the table
            hTable.put(put);
        }
        
        // Close resources
        hTable.close();
        br.close();
        System.out.println("Data inserted successfully.");
    }

    // Helper method to check if a string is null or empty
    private static boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty();
    }
}
