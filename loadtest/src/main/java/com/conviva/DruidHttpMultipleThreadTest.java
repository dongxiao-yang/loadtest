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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class DruidHttpMultipleThreadTest {


    private static final Logger logger = Logger
            .getLogger(DruidHttpTest.class);

    public static void main(String[] args) throws IOException {

        String queryfile = "part-00000";
        int threadcnt = 5;
        if (args.length > 0) {
            queryfile = args[0];
        }

        List<String> content = Files.readAllLines(Paths.get(queryfile));

        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(content.size());

        queue.addAll(content);
//        ExecutorService service = Executors.newCachedThreadPool();


        for (int i = 1; i < threadcnt; i++) {
            HttpSender hs = new DruidHttpMultipleThreadTest().new HttpSender(queue);
            hs.start();

        }




    }

    class HttpSender extends Thread {

        private LinkedBlockingQueue<String> queue;
        private CloseableHttpClient httpClient;

        public HttpSender(LinkedBlockingQueue<String> queue) {
            this.queue = queue;
            httpClient = getCloseableHttpClient();
        }

        @Override
        public void run() {

            long start = System.nanoTime();
            while (true) {
                String query = queue.poll();
                if (query != null) {


                    HttpPost post = new HttpPost("http://10.72.1.155:8888//druid/v2/");

                    post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
                    post.setEntity(new StringEntity(query, "UTF-8"));


                    long s = System.nanoTime();
                    try {
                        CloseableHttpResponse response = httpClient.execute(post);
                        long timecost = (System.nanoTime() - s) / 1000000;


                        HttpEntity entity = response.getEntity();

//                       logger.info(query);
                        logger.info(response.getStatusLine().getStatusCode() + ":" + response.getStatusLine().getReasonPhrase());

                        logger.info(IOUtils.toString(entity.getContent()));


                        logger.info("cost time: " + timecost + "ms");
                        post.releaseConnection();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }


                } else {
                    break;
                }
            }
            long totalcost = (System.nanoTime() - start) / 1000000;
            logger.info("total cost time: " + totalcost + "ms");
        }


        private CloseableHttpClient getCloseableHttpClient() {

            PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
            connectionManager.setMaxTotal(
                    100);
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


}
