import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class DatabaseClient {

    private Connection connection;

    public void connect(String url) throws SQLException {
        connection = DriverManager.getConnection(url);
    }

    public void disconnect() throws SQLException{
        if(connection != null)
            connection.close();
    }

    public ArrayList<HashMap> test(String count) throws SQLException{
        String queryString = "select * from records limit ?";
        PreparedStatement statement = connection.prepareStatement(queryString);
        statement.setString(1, count);
        statement.setQueryTimeout(30);
        ResultSet rs = statement.executeQuery();

        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        HashMap<String, String> result = new HashMap<>();
        ArrayList<HashMap> results = new ArrayList<>();
        while(rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                result.put(rsmd.getColumnName(i), rs.getString(i));
            }
            results.add(result);
        }
        return results;
    }
}
