import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

class WebHandler implements HttpHandler {

    public void handle(HttpExchange t) throws IOException {

        InputStream is = t.getRequestBody();
        // ... read the request body

        String response = "Service is running";
        t.sendResponseHeaders(200, response.length());
        OutputStream os = t.getResponseBody();
        os.write(response.getBytes());
        os.close();
    }
}