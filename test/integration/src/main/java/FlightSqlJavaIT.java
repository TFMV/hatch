import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.IntVector;

public class FlightSqlJavaIT {
    public static void main(String[] args) throws Exception {
        RootAllocator allocator = new RootAllocator();
        try (FlightClient client = FlightClient.builder()
                .allocator(allocator)
                .location(Location.forGrpcInsecure("localhost", 32010))
                .build();
             FlightSqlClient sqlClient = new FlightSqlClient(client)) {

            System.out.println("Connected to Porter Flight SQL server");

            // Execute simple queries without prepared statements for now
            sqlClient.executeUpdate("DROP TABLE IF EXISTS java_roundtrip");
            sqlClient.executeUpdate("CREATE TABLE java_roundtrip(id INTEGER)");
            System.out.println("Created table");
            
            sqlClient.executeUpdate("INSERT INTO java_roundtrip VALUES (42)");
            System.out.println("Inserted data");

            // Execute a simple query - try the simplest possible query first
            FlightInfo info = sqlClient.execute("SELECT * FROM java_roundtrip");
            System.out.println("Got flight info with " + info.getEndpoints().size() + " endpoints");
            
            try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
                VectorSchemaRoot root = stream.getRoot();
                System.out.println("Schema: " + root.getSchema());
                
                while (stream.next()) {
                    IntVector idVector = (IntVector) root.getVector("id");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        int value = idVector.get(i);
                        System.out.println("Retrieved value: " + value);
                        if (value != 42) {
                            throw new IllegalStateException("Unexpected value: " + value);
                        }
                    }
                }
            }
            
            System.out.println("Test completed successfully!");
        }
    }
}
