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

    public ArrayList<HashMap> getRecords(String offset, String count) throws SQLException{
        String queryString = String.join("\n",
            "SELECT",
                "records.id,",
                "records.age,",
                "workclasses.name AS workclass,",
                "education_levels.name AS education_level,",
                "records.education_num,",
                "marital_statuses.name AS marital_status,",
                "occupations.name AS occupation,",
                "races.name AS race,",
                "sexes.name AS sex,",
                "records.capital_gain,",
                "records.capital_loss,",
                "records.hours_week,",
                "countries.name AS country,",
                "records.over_50k",
            "FROM records",
            "JOIN workclasses ON workclasses.id = records.workclass_id",
            "JOIN education_levels ON education_levels.id = records.education_level_id",
            "JOIN marital_statuses ON marital_statuses.id = records.marital_status_id",
            "JOIN occupations ON occupations.id = records.occupation_id",
            "JOIN races ON races.id = records.race_id",
            "JOIN sexes ON sexes.id = records.sex_id",
            "JOIN countries ON countries.id = records.country_id",
            "WHERE records.id > ?",
            "LIMIT ?",
            ";"
        );
        PreparedStatement statement = connection.prepareStatement(queryString);
        statement.setString(1, offset);
        statement.setString(2, count);

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
            results.add(new HashMap(result));
            result.clear();
        }
        return results;
    }

    public HashMap<String, String> getStats(String aggregType, String aggregValue) throws SQLException {
        String queryString = String.join("\n",
            "SELECT",
                "sum(capital_gain),",
                "avg(capital_gain),",
                "sum(capital_loss),",
                "avg(capital_loss),",
                "sum(case when over_50k = 1 then 1 else 0 end) AS count_over_50k,",
                "sum(case when over_50k = 0 then 1 else 0 end) AS count_under_50k",
            "FROM records",
            "WHERE " + aggregType + " = ?",
            "GROUP BY " + aggregType,
            ";"
        );
        PreparedStatement statement = connection.prepareStatement(queryString);
        statement.setString(1, aggregValue);

        statement.setQueryTimeout(30);
        ResultSet rs = statement.executeQuery();

        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        HashMap<String, String> results = new HashMap<>();
        while(rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                results.put(rsmd.getColumnName(i), rs.getString(i));
            }
        }
        return results;
    }
}
