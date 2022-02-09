package com.conviva;

import org.apache.commons.io.IOUtils;
import org.apache.http.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;

public class DruidHttpsTest {

    public static void main(String[] args) throws IOException {

        String queryfile = "part-00000.log";
        if (args.length > 0 )
        {
            queryfile = args[0];
        }

        List<String> content = Files.readAllLines(Paths.get(queryfile));


        for (String query : content) {


            HttpPost post = new HttpPost("https://druid-routers.gke-shared-1.us-east4.prod.gcp.conviva.com/druid/v2/");

            post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
            post.setEntity(new StringEntity(query, "UTF-8"));


            CloseableHttpClient httpClient = getCloseableHttpClient();
            long s = System.nanoTime();
            CloseableHttpResponse response = httpClient.execute(post);
            long timecost = (System.nanoTime() - s) / 1000000;


            HttpEntity entity = response.getEntity();

//            System.out.println(query);
            System.out.println(response.getStatusLine().getStatusCode() + ":" + response.getStatusLine().getReasonPhrase());


            System.out.println(IOUtils.toString(entity.getContent()));

            System.out.println("cost time: " + timecost + "ms");

        }
    }


    public static CloseableHttpClient getCloseableHttpClient() {

//        ConnectionKeepAliveStrategy myStrategy = new ConnectionKeepAliveStrategy() {
//
//            public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
//                // Honor 'keep-alive' header
//                HeaderElementIterator it = new BasicHeaderElementIterator(
//                        response.headerIterator(HTTP.CONN_KEEP_ALIVE));
//                while (it.hasNext()) {
//                    HeaderElement he = it.nextElement();
//                    String param = he.getName();
//                    String value = he.getValue();
//                    if (value != null && param.equalsIgnoreCase("timeout")) {
//                        try {
//                            return Long.parseLong(value) * 1000;
//                        } catch (NumberFormatException ignore) {
//                        }
//                    }
//                }
//                return 3600 * 1000;
//            }
//
//        };
        CloseableHttpClient httpClient = null;
        try {
            httpClient = HttpClients.custom().
//                    setKeepAliveStrategy(myStrategy).
                    setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE).
                    setSSLContext(new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
                        public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
                            return true;
                        }
                    })
                            .build()).build();
        } catch (KeyManagementException e) {
            System.out.println("KeyManagementException in creating http client instance" + e);
        } catch (NoSuchAlgorithmException e) {
            System.out.println("NoSuchAlgorithmException in creating http client instance" + e);
        } catch (KeyStoreException e) {
            System.out.println("KeyStoreException in creating http client instance" + e);
        }
        return httpClient;
    }
}
