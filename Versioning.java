import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

public class Versioning {

    public static String Table_Name = "CovidData";
    
    public static void main(String[] argv) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = new HTable(conf, Table_Name);

        // Sample row key to insert and retrieve data (to be provided later)
        String row_key = "user1_tweet123";

        // Task for Member 1: Insert multiple versions of the user_description
        insertUserDescriptions(hTable, row_key);

        // Task for Member 1b: Retrieve all versions of the user description
        getUserDescriptionVersions(hTable, row_key);

        // Other member tasks can be implemented similarly here
        // Example: Query tweets by location, get verified users, etc.

        // Close the table connection
        hTable.close();
    }

    // Task 1a: Insert multiple descriptions for the same user (to demonstrate versioning)
    public static void insertUserDescriptions(HTable hTable, String rowKey) throws IOException {
        // Example: Adding multiple versions of user descriptions for a user
        Put put = new Put(Bytes.toBytes(rowKey));
        
        // Insert first version of user_description
        put.add(Bytes.toBytes("Users"), Bytes.toBytes("user_description"), Bytes.toBytes("First description"));
        hTable.put(put);
        
        // Insert second version of user_description
        put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes("Users"), Bytes.toBytes("user_description"), Bytes.toBytes("Updated description"));
        hTable.put(put);

        // Insert third version of user_description
        put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes("Users"), Bytes.toBytes("user_description"), Bytes.toBytes("Final description"));
        hTable.put(put);

        System.out.println("Inserted multiple versions of user descriptions for row key: " + rowKey);
    }

    // Task 1b: Retrieve all versions of user description
    public static void getUserDescriptionVersions(HTable hTable, String rowKey) throws IOException {
        // Get the maximum of 3 versions of user_description
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_description"));
        get.setMaxVersions(3);  // Set the number of versions to retrieve
        
        Result result = hTable.get(get);
        List<KeyValue> allResults = result.getColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_description"));
        
        System.out.println("Retrieving multiple versions of user descriptions for row key: " + rowKey);
        for (KeyValue kv : allResults) {
            System.out.println("Version: " + kv.getTimestamp() + " Value: " + new String(kv.getValue()));
        }
    }

  
    // Helper function to check if a string is null or empty
    private static boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty();
    }
}
