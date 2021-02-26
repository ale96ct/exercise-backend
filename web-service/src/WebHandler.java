import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import com.google.gson.*;

class WebHandler implements HttpHandler {

    DatabaseClient databaseClient;
    Gson serializer;

    public WebHandler(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
        GsonBuilder builder = new GsonBuilder();
        builder.setPrettyPrinting();
        this.serializer = builder.create();
    }

    public void handle(HttpExchange exchange) throws IOException {
        try {
            URI uri = exchange.getRequestURI();
            System.out.println("Path: " + uri.getPath());

            if ("GET".equals(exchange.getRequestMethod())) {
                if ("/test".equals(uri.getPath())) {
                    handleTest(exchange);
                    return;
                }
            }

            String response = "400 Bad Request Error";
            exchange.sendResponseHeaders(404, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
        catch (SQLException e) {
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

    private void handleTest(HttpExchange exchange) throws IOException, SQLException {
        URI uri = exchange.getRequestURI();
        System.out.println("Query: " + uri.getQuery());
        Map<String, String> params = getParams(uri.getQuery());
        String count = params.get("count");
        ArrayList<HashMap> results = databaseClient.test(count);
        HashMap<String, ArrayList> response = new HashMap();
        response.put("records", results);
        String json_response = serializer.toJson(response);
        exchange.sendResponseHeaders(200, json_response.length());
        OutputStream os = exchange.getResponseBody();
        os.write(json_response.getBytes());
        os.close();
    }
}