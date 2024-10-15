import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class Versioning {

    public static String Table_Name = "CovidData";

    @SuppressWarnings("deprecation")
    public static void main(String[] argv) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        @SuppressWarnings({ "resource" })
        HTable hTable = new HTable(conf, Table_Name);
        
        String row_key = "user123";  

        // Insert multiple descriptions for the same user
        insertUserDescription(hTable, row_key, "User description version 1");
        insertUserDescription(hTable, row_key, "User description version 2");
        insertUserDescription(hTable, row_key, "User description version 3");

        // Retrieve and display the different versions of the user description
        getUserDescriptionVersions(hTable, row_key);
    }

    // Method to insert a user description for a specific user
    private static void insertUserDescription(HTable hTable, String row_key, String user_description) throws Exception {
        Put put = new Put(Bytes.toBytes(row_key));
        put.add(Bytes.toBytes("Users"), Bytes.toBytes("user_description"), Bytes.toBytes(user_description));
        hTable.put(put);
        System.out.println("Inserted user description: " + user_description);
    }

    // Method to retrieve and display different versions of the user description
    private static void getUserDescriptionVersions(HTable hTable, String row_key) throws Exception {
        Get get = new Get(Bytes.toBytes(row_key));
        get.setMaxVersions(4);  // Retrieve up to 4 versions

        Result result = hTable.get(get);
        List<KeyValue> allVersions = result.getColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_description"));

        // Display each version of the user description
        System.out.println("Retrieved versions for user_description:");
        for (KeyValue kv : allVersions) {
            System.out.println(new String(kv.getValue()));
        }
    }
}
