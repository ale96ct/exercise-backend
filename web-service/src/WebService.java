import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;

public class WebService
{
    public static void main(String[] args) {

        try {
            DatabaseClient databaseClient = new DatabaseClient();
            String url = "jdbc:sqlite:./../database/exercise01.sqlite";
            databaseClient.connect(url);

            HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
            server.createContext("/", new WebHandler(databaseClient));
            server.setExecutor(null);
            server.start();
        }
        catch(SQLException | IOException e) {
            System.err.println(e.getMessage());
        }
    }
}