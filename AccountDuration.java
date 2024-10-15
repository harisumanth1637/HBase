import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

public class AccountDuration {

    public static String Table_Name = "CovidData";

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = new HTable(conf, Table_Name);

        // Initialize counts for categories and account durations
        int lessThan1Year = 0, oneToThreeYears = 0, greaterThan3Years = 0;
        int verifiedLessThan1Year = 0, verifiedOneToThreeYears = 0, verifiedGreaterThan3Years = 0;
        int nonVerifiedLessThan1Year = 0, nonVerifiedOneToThreeYears = 0, nonVerifiedGreaterThan3Years = 0;

        long totalVerifiedYears = 0, totalNonVerifiedYears = 0;
        int verifiedCount = 0, nonVerifiedCount = 0;

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date currentDate = new Date();  // Current date to compare

        // Scan all rows in the table
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("Users")); // Only retrieve "Users" column family (to reduce data transfer)
        ResultScanner scanner = hTable.getScanner(scan);

        try {
            for (Result result : scanner) {
                // Extract the user_created and user_verified fields from the table
                String user_created = Bytes.toString(result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_created")));
                String user_verified = Bytes.toString(result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_verified")));

                if (user_created == null || user_verified == null) {
                    continue;  // Skip if either field is missing
                }

                try {
                    // Parse the account creation date
                    Date createdDate = dateFormat.parse(user_created);
                    // Calculate the duration in years
                    long accountDurationYears = calculateYearsBetween(createdDate, currentDate);

                    // Categorize based on account age
                    if (accountDurationYears < 1) {
                        lessThan1Year++;
                        if (user_verified.equalsIgnoreCase("TRUE")) {
                            verifiedLessThan1Year++;
                        } else {
                            nonVerifiedLessThan1Year++;
                        }
                    } else if (accountDurationYears >= 1 && accountDurationYears <= 3) {
                        oneToThreeYears++;
                        if (user_verified.equalsIgnoreCase("TRUE")) {
                            verifiedOneToThreeYears++;
                        } else {
                            nonVerifiedOneToThreeYears++;
                        }
                    } else {
                        greaterThan3Years++;
                        if (user_verified.equalsIgnoreCase("TRUE")) {
                            verifiedGreaterThan3Years++;
                        } else {
                            nonVerifiedGreaterThan3Years++;
                        }
                    }

                    // Calculate the total number of years for verified and non-verified users
                    if (user_verified.equalsIgnoreCase("TRUE")) {
                        totalVerifiedYears += accountDurationYears;
                        verifiedCount++;
                    } else {
                        totalNonVerifiedYears += accountDurationYears;
                        nonVerifiedCount++;
                    }

                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

        } finally {
            scanner.close();  // Ensure scanner is closed after usage
        }

        // Output the categorization
        System.out.println("Account Age Categories:");
        System.out.println("Less than 1 year: " + lessThan1Year);
        System.out.println("1-3 years: " + oneToThreeYears);
        System.out.println("Greater than 3 years: " + greaterThan3Years);

        // Compare verified vs non-verified account durations
        System.out.println("\nVerified Users:");
        System.out.println("Less than 1 year: " + verifiedLessThan1Year);
        System.out.println("1-3 years: " + verifiedOneToThreeYears);
        System.out.println("Greater than 3 years: " + verifiedGreaterThan3Years);

        System.out.println("\nNon-Verified Users:");
        System.out.println("Less than 1 year: " + nonVerifiedLessThan1Year);
        System.out.println("1-3 years: " + nonVerifiedOneToThreeYears);
        System.out.println("Greater than 3 years: " + nonVerifiedGreaterThan3Years);

        // Calculate average duration for verified and non-verified users
        double avgVerifiedYears = verifiedCount > 0 ? (double) totalVerifiedYears / verifiedCount : 0;
        double avgNonVerifiedYears = nonVerifiedCount > 0 ? (double) totalNonVerifiedYears / nonVerifiedCount : 0;

        System.out.println("\nAverage account age for verified users: " + avgVerifiedYears + " years");
        System.out.println("Average account age for non-verified users: " + avgNonVerifiedYears + " years");
    }

    // Helper method to calculate the number of years between two dates
    private static long calculateYearsBetween(Date startDate, Date endDate) {
        LocalDate startLocalDate = startDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        LocalDate endLocalDate = endDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        return ChronoUnit.YEARS.between(startLocalDate, endLocalDate);
    }
}
