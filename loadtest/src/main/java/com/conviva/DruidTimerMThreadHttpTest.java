package com.conviva;

import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
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
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DruidTimerMThreadHttpTest {


    private static final Logger logger = Logger
            .getLogger(DruidTimerMThreadHttpTest.class);

    public static void main(String[] args) throws IOException, InterruptedException {


        String queryfile = "TimeseriesQuery";

        String host = "druid-console.gke-shared-1.us-east4.prod.gcp.conviva.com";
        if (args.length > 0) {
            queryfile = args[0];
            host = args[1];
        }

        DateTime dateTime = new DateTime(ISOChronology.getInstanceUTC());
        int window = 20;
        int round = 5;
        int threadcont = 5;
        if (args.length > 2) {
//            dateTime = new DateTime(args[2], ISOChronology.getInstanceUTC());
            window = Integer.parseInt(args[2]);
            round = Integer.parseInt(args[3]);
            threadcont = Integer.parseInt(args[4]);
        }


        String url = "http://" + host + "//druid/v2/";

        logger.info(queryfile);
        logger.info(url);
        List<String> content = Files.readAllLines(Paths.get(queryfile));


        final DateTimeFormatter TIMESTAMP_FMT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'.000Z'");

//        long start = System.nanoTime();

        List<String> intervallist = new ArrayList<>();

        int firstend = 0;
        int firststart = window;
        int step = window / 2;

        for (int i = 0; i < round; i++) {
            String starttime = TIMESTAMP_FMT.print(dateTime.minusMinutes(firststart + step * i));
            String endtime = TIMESTAMP_FMT.print(dateTime.minusMinutes(firstend + i * step));
            String interval = starttime + "/" + endtime;
            intervallist.add(interval);
        }


        List<String> querylist = new ArrayList<String>();


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


        String[] aa =  querylist.toArray(new String[0]);

        int queryNumPerThread = aa.length/threadcont;

        List<SendQueryThread> tlist = new ArrayList<>();

        for(int i=0 ; i< threadcont ; i ++)
        {
            CloseableHttpClient httpClient = getCloseableHttpClient();
            List<String> querylistPerThread = new ArrayList<>();

            for(int j= queryNumPerThread*i ; j < queryNumPerThread*(i+1) ; j ++)
            {
               String query = aa[j];
                querylistPerThread.add(query);
            }
            SendQueryThread st = new  SendQueryThread(querylistPerThread,httpClient,url);

            tlist.add(st);

        }

        ExecutorService es = Executors.newFixedThreadPool(threadcont);
        long start = System.nanoTime();
        for(SendQueryThread task :tlist)
        {
            es.execute(task);
        }

        es.shutdown();

        boolean finished = es.awaitTermination(10, TimeUnit.MINUTES);

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


   static class SendQueryThread extends Thread {

        List<String> querylist = null;
        CloseableHttpClient httpclient = null;
        String url = null;

        public SendQueryThread(List<String> querylist, CloseableHttpClient httpclient, String url) {
            this.querylist = querylist;
            this.httpclient = httpclient;
            this.url = url;
        }

        @Override
        public void run() {

            for (String finalquery : querylist) {
                logger.info(finalquery);
                HttpPost post = new HttpPost(url);
                post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
                post.setEntity(new StringEntity(finalquery, "UTF-8"));


                long s = System.nanoTime();
                CloseableHttpResponse response = null;
                try {
                    response = httpclient.execute(post);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                long timecost = (System.nanoTime() - s) / 1000000;
                logger.info(response.getStatusLine().getStatusCode() + ":" + response.getStatusLine().getReasonPhrase());

                logger.info("cost time: " + timecost + "ms");
                post.releaseConnection();

            }

            logger.info("all task in thread finish");
        }
    }

}
