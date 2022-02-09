package com.conviva;

import org.apache.commons.io.IOUtils;
import org.apache.http.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DruidTimerHttpTest {

    private static final Logger logger = Logger
            .getLogger(DruidTimerHttpTest.class);

    public static void main(String[] args) throws IOException {


        String queryfile = "TopNQuery";

        String host = "druid-console.gke-shared-1.us-east4.prod.gcp.conviva.com";
        if (args.length > 0) {
            queryfile = args[0];
            host = args[1];
        }

        DateTime dateTime = new DateTime(ISOChronology.getInstanceUTC());
        int window = 4;
        int round = 30;
        if (args.length > 2) {
//            dateTime = new DateTime(args[2], ISOChronology.getInstanceUTC());
            window = Integer.parseInt(args[2]);
            round = Integer.parseInt(args[3]);
        }


        String url = "http://" + host + "//druid/v2/";

        logger.info(queryfile);
        logger.info(url);
        List<String> content = Files.readAllLines(Paths.get(queryfile));

        CloseableHttpClient httpClient = getCloseableHttpClient();
        final DateTimeFormatter TIMESTAMP_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'.000Z'");

//        long start = System.nanoTime();

        List<String> intervallist = new ArrayList<>();

        int firstend = 0;
        int firststart = window;
        int step = window/2;
        for (int i = 0; i < round; i++) {
            String starttime = TIMESTAMP_FMT.print(dateTime.minusMinutes(firststart + step*i ));
            String endtime = TIMESTAMP_FMT.print(dateTime.minusMinutes(firstend + i*step ));
            String interval = starttime + "/" + endtime;
            intervallist.add(interval);
        }


        List<String> querylist = new ArrayList<>();


        for (String interval : intervallist) {

            for (String query : content) {

                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
                JsonNode dataNode = mapper.readValue(query, JsonNode.class);
                JsonNode intervalNode = dataNode.get("intervals");
                ObjectNode tmp1 = (ObjectNode) intervalNode;


                ArrayNode arrayNode = mapper.createArrayNode();
                arrayNode.add(interval);
                tmp1.put("intervals", arrayNode);

                querylist.add(dataNode.toString());


            }
        }

        long start = System.nanoTime();

        for (String finalquery : querylist) {
            logger.info(finalquery);

            HttpPost post = new HttpPost(url);

            post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
            post.setEntity(new StringEntity(finalquery, "UTF-8"));


            long s = System.nanoTime();
            CloseableHttpResponse response = httpClient.execute(post);
            long timecost = (System.nanoTime() - s) / 1000000;
//            HttpEntity entity = response.getEntity();
            logger.info(response.getStatusLine().getStatusCode() + ":" + response.getStatusLine().getReasonPhrase());

//            logger.info(IOUtils.toString(entity.getContent()));

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
