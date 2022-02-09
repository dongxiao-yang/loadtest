package com.conviva;


import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import org.apache.commons.io.IOUtils;
import org.apache.http.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.log4j.Logger;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;


public class DruidHttpTest {

    private static final Logger logger = Logger
            .getLogger(DruidHttpTest.class);

    public static void main(String[] args) throws IOException {

        String queryfile = "part-00000";
        String host = "10.72.1.184:8888";
//        String host = "druid-console.gke-shared-1.us-east4.prod.gcp.conviva.com";
        if (args.length > 0) {
            queryfile = args[0];
            host = args[1];
        }

        String url = "http://"+host+"//druid/v2/";

        logger.info(queryfile);
        logger.info(url);
        List<String> content = Files.readAllLines(Paths.get(queryfile));

        CloseableHttpClient httpClient = getCloseableHttpClient();

        long start = System.nanoTime();
        for (String query : content) {


            HttpPost post = new HttpPost(url);

            post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
            post.setEntity(new StringEntity(query, "UTF-8"));


            long s = System.nanoTime();
            CloseableHttpResponse response = httpClient.execute(post);
            long timecost = (System.nanoTime() - s) / 1000000;


            HttpEntity entity = response.getEntity();

//                       logger.info(query);
            logger.info(response.getStatusLine().getStatusCode() + ":" + response.getStatusLine().getReasonPhrase());

            logger.info(IOUtils.toString(entity.getContent()));


            logger.info("cost time: " + timecost + "ms");
            post.releaseConnection();

        }

        long totalcost = (System.nanoTime() - start) / 1000000;

        logger.info("total cost time: " + totalcost + "ms");
    }


    public static CloseableHttpClient getCloseableHttpClient() {

        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(500);
        connectionManager.setDefaultMaxPerRoute(50);

        ConnectionKeepAliveStrategy myStrategy = new ConnectionKeepAliveStrategy() {

            public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
                // Honor 'keep-alive' header
                HeaderElementIterator it = new BasicHeaderElementIterator(
                        response.headerIterator(HTTP.CONN_KEEP_ALIVE));
                while (it.hasNext()) {
                    HeaderElement he = it.nextElement();
                    String param = he.getName();
                    String value = he.getValue();
                    if (value != null && param.equalsIgnoreCase("timeout")) {
                        try {
                            return Long.parseLong(value) * 1000;
                        } catch (NumberFormatException ignore) {
                        }
                    }
                }
                return 3600 * 1000;
            }

        };
        CloseableHttpClient httpClient = null;

        httpClient = HttpClients.custom().
                setConnectionManager(connectionManager).
                setKeepAliveStrategy(myStrategy).build();

        return httpClient;
    }
}
