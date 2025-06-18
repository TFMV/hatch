import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public class FlightSqlJavaIT {
    public static void main(String[] args) throws Exception {
        RootAllocator allocator = new RootAllocator();
        try (FlightClient client = FlightClient.builder()
                .allocator(allocator)
                .location(Location.forGrpcInsecure("localhost", 32010))
                .build();
             FlightSqlClient sqlClient = new FlightSqlClient(client)) {

            sqlClient.executeUpdate("CREATE TABLE java_roundtrip(id INTEGER)");
            sqlClient.executeUpdate("INSERT INTO java_roundtrip VALUES (42)");

            try (FlightSqlClient.PreparedStatement stmt =
                         sqlClient.prepare("SELECT id FROM java_roundtrip WHERE id = ?")) {
                stmt.setBigInt(1, 42);
                FlightInfo info = stmt.executeQuery();
                try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
                    VectorSchemaRoot root = stream.getRoot();
                    while (stream.next()) {
                        int v = root.getVector(0).getInt(0);
                        if (v != 42) {
                            throw new IllegalStateException("Unexpected value: " + v);
                        }
                    }
                }
            }
        }
    }
}
