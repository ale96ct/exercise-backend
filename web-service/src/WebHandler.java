import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.sql.SQLException;
import java.util.*;

import com.google.gson.*;
import java.io.File;
import java.io.FileWriter;

class WebHandler implements HttpHandler {

    DatabaseClient databaseClient;
    Gson serializer;

    public WebHandler(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
        GsonBuilder builder = new GsonBuilder();
        builder.setPrettyPrinting();
        this.serializer = builder.create();
    }

    public void handle(HttpExchange exchange) {
        try {
            URI uri = exchange.getRequestURI();
            if ("GET".equals(exchange.getRequestMethod())) {
                if("/records".equals(uri.getPath())) {
                    handleGetRecords(exchange);
                    return;
                }
                if("/stats".equals(uri.getPath())) {
                    handleGetStats(exchange);
                    return;
                }
                if("/csv".equals(uri.getPath())) {
                    handleGetCsv(exchange);
                    return;
                }
            }

            String response = "400 Bad Request Error";
            exchange.sendResponseHeaders(404, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
        catch (SQLException | IOException e) {
            System.err.println(e.getMessage());
        }
    }

    private Map<String, String> getParams(String query) {
        Map<String, String> params = new HashMap<>();
        String[] pairs = query.split("&");
        for (String pair : pairs){
            String[] split_pair = pair.split("=");
            params.put(split_pair[0], split_pair[1]);
        }
        return params;
    }

    private void handleGetRecords(HttpExchange exchange) throws IOException, SQLException {
        URI uri = exchange.getRequestURI();
        Map<String, String> params = getParams(uri.getQuery());
        String offset = params.get("offset");
        String count = params.get("count");

        ArrayList<HashMap> results = databaseClient.getRecords(offset, count);
        HashMap<String, ArrayList> response = new HashMap();
        response.put("denormalized_records", results);
        String json_response = serializer.toJson(response);

        exchange.sendResponseHeaders(200, json_response.length());
        OutputStream os = exchange.getResponseBody();
        os.write(json_response.getBytes());
        os.close();
    }

    private void handleGetStats(HttpExchange exchange) throws IOException, SQLException {
        URI uri = exchange.getRequestURI();
        Map<String, String> params = getParams(uri.getQuery());
        String aggregType = params.get("aggregationType");
        String aggregValue = params.get("aggregationValue");
        HashMap<String, String> results = new HashMap<>();
        String json_response;

        if (aggregType.equals("age") ||
                aggregType.equals("education_level_id") ||
                aggregType.equals("occupation_id")) {
            results = databaseClient.getStats(aggregType, aggregValue);
            results.put("aggregationType", aggregType);
            results.put("aggregationValue", aggregValue);
        }
        else {
            results.put("error", "Aggregation type is not valid");
        }

        json_response = serializer.toJson(results);
        exchange.sendResponseHeaders(200, json_response.length());
        OutputStream os = exchange.getResponseBody();
        os.write(json_response.getBytes());
        os.close();
    }

    private void handleGetCsv(HttpExchange exchange) throws IOException, SQLException {
        String csvPath = "./../database/all_records.csv";
        ArrayList<HashMap> results = databaseClient.getAllRecords();
        File csv = new File(csvPath);
        csv.createNewFile();
        FileWriter csvWriter = new FileWriter(csvPath);

        Set<String> columns = results.get(0).keySet();
        String columnsRow = "";
        Iterator<String> it1 = columns.iterator();
        while(it1.hasNext()) {
            columnsRow = columnsRow.concat(it1.next() + ",");
        }
        csvWriter.write(columnsRow + "\n");

        Iterator<HashMap> it2 = results.iterator();
        Collection values;
        while(it2.hasNext()) {
            values = it2.next().values();
            String valuesRow = "";
            Iterator<String> it3= values.iterator();
            while(it3.hasNext()) {
                valuesRow = valuesRow.concat(it3.next() + ",");
            }
            csvWriter.write(valuesRow + "\n");

        }

        csvWriter.close();
        String response = "Csv downloaded in: " + csvPath;
        exchange.sendResponseHeaders(200, response.length());
        OutputStream os = exchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
    }
}