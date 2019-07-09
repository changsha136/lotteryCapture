package javautils.http;

import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * http通用工具类，适用普通https和http(非连接池)，如果是带ssl密码的，请参考{@link lottery.domains.content.api.pt.Win88PTAPI}
 * Created by Nick on 2017-06-15.
 */
public class HttpClientUtil {
    private static Logger logger = LoggerFactory.getLogger(HttpClientUtil.class);    //日志记录

    private static CloseableHttpClient getHttpClient(int timeout) {
        HttpClientBuilder clientBuilder = HttpClientBuilder.create();

        // 是否要keep alive
        ConnectionKeepAliveStrategy keepAliveStrategy = HttpConfigBuilder.getInstance().buildKeepAliveStrategy(60 * 1000);
        clientBuilder.setKeepAliveStrategy(keepAliveStrategy);

        RequestConfig requestConfig = HttpConfigBuilder.getInstance().buildRequestConfig(timeout);
        clientBuilder.setDefaultRequestConfig(requestConfig);

        // 连接池
        // PoolingHttpClientConnectionManager connMgr = HttpConfigBuilder.getInstance().buildPoolingHttpClientConnectionManager(100, 100);
        // clientBuilder.setConnectionManager(connMgr);

        CloseableHttpClient httpClient = clientBuilder.build();

        return httpClient;
    }

    private static CloseableHttpClient getHttpsClient(int timeout, String sslVersion) throws NoSuchAlgorithmException, KeyManagementException {
        HttpClientBuilder clientBuilder = HttpClientBuilder.create();

        // 是否要keep alive
        ConnectionKeepAliveStrategy keepAliveStrategy = HttpConfigBuilder.getInstance().buildKeepAliveStrategy(60 * 1000);
        clientBuilder.setKeepAliveStrategy(keepAliveStrategy);

        RequestConfig requestConfig = HttpConfigBuilder.getInstance().buildRequestConfig(timeout);
        clientBuilder.setDefaultRequestConfig(requestConfig);

        // ssl连接
        BasicHttpClientConnectionManager connMgr;
        if (sslVersion == null) {
            connMgr = HttpConfigBuilder.getInstance().buildBasicHttpsClientConnectionManager();
        }
        else {
            connMgr = HttpConfigBuilder.getInstance().buildBasicHttpsClientConnectionManager(sslVersion);
        }
        clientBuilder.setConnectionManager(connMgr);

        CloseableHttpClient httpClient = clientBuilder.build();

        return httpClient;
    }

    public static String get(String url ,Map<String ,String > httpHeader, int timeout) throws Exception{
        CloseableHttpClient httpClient;
        if (url.startsWith("https://")) {
            httpClient = getHttpsClient(timeout, null);
        }
        else {
            httpClient = getHttpClient(timeout);
        }

        return get(httpClient, url, httpHeader);
    }

    public static String get(CloseableHttpClient httpClient, String url ,Map<String ,String > httpHeader) throws IOException{
        String strResult = null;
        try {
            //发送get请求
            HttpGet request = new HttpGet(url);

            //设置http头参数
            if(httpHeader!=null && httpHeader.size()>0){
                for(Iterator<Map.Entry<String, String>> ies = httpHeader.entrySet().iterator(); ies.hasNext();){
                    Map.Entry<String, String> entry = ies.next();
                    String key =entry.getKey();
                    String value = entry.getValue();
                    request.addHeader(key, value);
                }
            }
            CloseableHttpResponse response = httpClient.execute(request);

            /**请求发送成功，并得到响应**/
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                try {
                    /**读取服务器返回过来的json字符串数据**/
                    strResult = EntityUtils.toString(response.getEntity(),"UTF-8");
                    url = URLDecoder.decode(url, "UTF-8");
                }catch(Exception e) {
                    logger.error("get请求提交失败:" + url);
                }finally {
                    response.close();
                }
            }

        } catch (IOException e) {
            logger.error("get请求提交失败:" + url, e);
        } finally {
            httpClient.close();
        }
        return strResult;
    }

    public static String post(String url, Map<String, String> params, Map<String, String> headers, int timeout) throws Exception{
        CloseableHttpClient httpClient;
        if (url.startsWith("https://")) {
            httpClient = getHttpsClient(timeout, null);
        }
        else {
            httpClient = getHttpClient(timeout);
        }

        return post(httpClient, url, params, headers);
    }

    public static String postAsStream(String url, Map<String, String> params, Map<String, String> headers, int timeout) throws Exception{
        CloseableHttpClient httpClient;
        if (url.startsWith("https://")) {
            httpClient = getHttpsClient(timeout, null);
        }
        else {
            httpClient = getHttpClient(timeout);
        }

        return postAsStream(httpClient, url, params, headers);
    }

    public static String postSSL(String url, Map<String, String> params, Map<String, String> headers, int timeout, String sslVersion) throws Exception{
        CloseableHttpClient httpClient;
        if (url.startsWith("https://")) {
            httpClient = getHttpsClient(timeout, sslVersion);
        }
        else {
            httpClient = getHttpClient(timeout);
        }

        return post(httpClient, url, params, headers);
    }

    public static String post(CloseableHttpClient httpClient, String url, Map<String, String> params, Map<String, String> headers) throws IOException {
        String result = null;
        try {
            HttpPost request = new HttpPost(url);

            //设置http头参数
            if(headers!=null && headers.size()>0) {
                for(Iterator<Map.Entry<String, String>> ies = headers.entrySet().iterator(); ies.hasNext();){
                    Map.Entry<String, String> entry = ies.next();

                    String key =entry.getKey();
                    String value = entry.getValue();

                    request.addHeader(key, value);
                }
            }

            if (params != null && params.size() > 0) {
                List list = new ArrayList();
                Iterator iterator = params.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry elem = (Map.Entry) iterator.next();
                    list.add(new BasicNameValuePair((String) elem.getKey(), (String) elem.getValue()));
                }
                if (list.size() > 0) {
                    UrlEncodedFormEntity entity = new UrlEncodedFormEntity(list, "utf-8");
                    request.setEntity(entity);
                }
            }

            CloseableHttpResponse response = httpClient.execute(request);
            url = URLDecoder.decode(url, "UTF-8");

            /**请求发送成功，并得到响应**/
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                try {
                    /**读取服务器返回过来的字符串数据**/
                    result = EntityUtils.toString(response.getEntity(),"UTF-8");
                } catch (Exception e) {
                    logger.error("post请求提交失败:" + url, e);
                } finally {
                    response.close();
                }
            } else {
                result = response.getStatusLine().getStatusCode()+"-"+response.getStatusLine().getReasonPhrase();
            }
        } catch (Exception e) {
            logger.error("post请求提交失败:" + url, e);
        } finally{
            httpClient.close();
        }
        return result;
    }

    public static String postAsStream(CloseableHttpClient httpClient, String url, Map<String, String> params, Map<String, String> headers) throws IOException {
        String result = null;
        try {
            HttpPost request = new HttpPost(url);

            //设置http头参数
            if(headers!=null && headers.size()>0) {
                for(Iterator<Map.Entry<String, String>> ies = headers.entrySet().iterator(); ies.hasNext();){
                    Map.Entry<String, String> entry = ies.next();

                    String key =entry.getKey();
                    String value = entry.getValue();

                    request.addHeader(key, value);
                }
            }

            if (params != null && params.size() > 0) {
                String paramUrl = ToUrlParamUtils.toUrlParam(params);
                InputStream is = new ByteArrayInputStream(paramUrl.getBytes());
                InputStreamEntity inputEntry = new InputStreamEntity(is);
                request.setEntity(inputEntry);
            }

            CloseableHttpResponse response = httpClient.execute(request);
            url = URLDecoder.decode(url, "UTF-8");

            /**请求发送成功，并得到响应**/
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                try {
                    /**读取服务器返回过来的字符串数据**/
                    result = EntityUtils.toString(response.getEntity(),"UTF-8");
                } catch (Exception e) {
                    logger.error("post请求提交失败:" + url, e);
                } finally {
                    response.close();
                }
            } else {
                result = response.getStatusLine().getStatusCode()+"-"+response.getStatusLine().getReasonPhrase();
            }
        } catch (Exception e) {
            logger.error("post请求提交失败:" + url, e);
        } finally{
            httpClient.close();
        }
        return result;
    }
}
