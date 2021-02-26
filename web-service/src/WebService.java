import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class WebService
{
    public static void main(String[] args) {

        // test database driver
        Connection connection = null;
        try {
            // create a database connection
            connection = DriverManager.getConnection("jdbc:sqlite:./../database/exercise01.sqlite");
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);

            ResultSet rs = statement.executeQuery("select * from countries limit 20");
            while(rs.next()) {
                // read the result set
                System.out.println("id = " + rs.getInt("id"));
                System.out.println("name = " + rs.getString("name"));
            }
        }
        catch(SQLException e) {
            // if the error message is "out of memory",
            // it probably means no database file is found
            System.err.println(e.getMessage());
        }
        finally {
            try {
                if(connection != null)
                    connection.close();
            }
            catch(SQLException e) {
                // connection close failed
                System.err.println(e.getMessage());
            }
        }

        // test http server
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
            server.createContext("/", new WebHandler());
            server.setExecutor(null); // creates a default executor
            server.start();
        }
        catch(IOException e) {
            System.err.println(e.getMessage());
        }
    }
}