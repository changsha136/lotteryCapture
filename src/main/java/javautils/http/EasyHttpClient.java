// package javautils.http;
//
// import javautils.StringUtil;
// import org.apache.commons.httpclient.*;
// import org.apache.commons.httpclient.methods.GetMethod;
// import org.apache.commons.httpclient.methods.PostMethod;
// import org.apache.commons.httpclient.methods.RequestEntity;
// import org.apache.commons.httpclient.methods.StringRequestEntity;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// import java.io.InputStream;
// import java.net.SocketTimeoutException;
//
// public class EasyHttpClient {
//
// 	private static final Logger logger = LoggerFactory.getLogger(EasyHttpClient.class);
//
// 	private HttpClient httpClient;
// 	private final int TIMEOUT = 10 * 1000;
// 	private final int SO_TIMEOUT = 10 * 1000;
// 	private int REPEAT_TIMES = 3;
//
// 	public EasyHttpClient() {
// 		init();
// 	}
//
// 	/**
// 	 * 初始化
// 	 */
// 	public void init() {
// 		MultiThreadedHttpConnectionManager manager = new MultiThreadedHttpConnectionManager();
// 		manager.getParams().setMaxTotalConnections(512); // 最大连接数
// 		manager.getParams().setDefaultMaxConnectionsPerHost(512); // 每个主机默认最大连接数
// 		httpClient = new HttpClient(manager);
// 		setTimeOut(TIMEOUT, SO_TIMEOUT);
// 	}
//
// 	private void setHeaders(HttpMethod method) {
// 		method.setRequestHeader("Accept", "text/html,application/xhtml+xml,application/xml;");
// 		method.setRequestHeader("Accept-Language", "zh-cn");
// 		method.setRequestHeader("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9.0.3) Gecko/2008092417 Firefox/3.0.3");
// 		method.setRequestHeader("Accept-Charset", "utf-8");
// 		method.setRequestHeader("Keep-Alive", "300");
// 		method.setRequestHeader("Connection", "Keep-Alive");
// 		method.setRequestHeader("Cache-Control", "no-cache");
// 		}
//
//
// 	/**
// 	 * 设置超时时间
// 	 * @param TIMEOUT
// 	 * @param SO_TIMEOUT
// 	 * @param TIME_OUT_INIT
// 	 */
// 	public void setTimeOut(int TIMEOUT, int SO_TIMEOUT) {
// 		httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(TIMEOUT);
// 		httpClient.getHttpConnectionManager().getParams().setSoTimeout(SO_TIMEOUT);
// 	}
//
// 	/**
// 	 * 设置请求重复次数
// 	 * @param times
// 	 */
// 	public void setRepeatTimes(int times) {
// 		this.REPEAT_TIMES = times;
// 	}
//
// 	/**
// 	 * GET请求
// 	 * @param url
// 	 * @return
// 	 */
// 	public String get(String url) {
// 		GetMethod get = new GetMethod(url);
// 		for (int i = 0; i < REPEAT_TIMES; i++) {
// 			logger.debug("正在发送请求，当前第" + (i + 1) + "次...");
// 			try {
// 				setHeaders(get);
// 				int state = httpClient.executeMethod(get);
// 				logger.debug("请求状态" + state);
// 				if(state == HttpStatus.SC_OK) {
// 					InputStream inputStream = get.getResponseBodyAsStream();
// 					String data = StringUtil.fromInputStream(inputStream);
// 					logger.debug("成功获取到数据，长度为" + data.length());
// 					return data;
// 				}
// 			} catch(ConnectTimeoutException e) {
// 				logger.error("请求超时...Connect Timeout");
// 			} catch(SocketTimeoutException e) {
// 				logger.error("请求超时...Socket Timeout");
// 			} catch (Exception e) {
// 				logger.error("请求出错...", e);
// 			} finally {
// 				closeConnection(get);
// 			}
// 		}
// 		return null;
// 	}
//
// 	/**
// 	 * POST请求
// 	 * @param url
// 	 * @param params
// 	 * @return
// 	 */
// 	public String post(String url, NameValuePair[] params) {
// 		PostMethod post = new PostMethod(url);
// 		if(params != null) {
// 			post.setRequestBody(params);
// 		}
// 		for (int i = 0; i < REPEAT_TIMES; i++) {
// 			logger.debug("正在发送请求，当前第" + (i + 1) + "次...");
// 			try {
// 				int state = httpClient.executeMethod(post);
// 				logger.debug("请求状态" + state);
// 				if(state == HttpStatus.SC_OK) {
// 					InputStream inputStream = post.getResponseBodyAsStream();
// 					String data = StringUtil.fromInputStream(inputStream);
// 					logger.debug("成功获取到数据，长度为" + data.length());
// 					return data;
// 				}
// 			} catch(ConnectTimeoutException e) {
// 				logger.error("请求超时...Connect Timeout");
// 			} catch(SocketTimeoutException e) {
// 				logger.error("请求超时...Socket Timeout");
// 			} catch (Exception e) {
// 				logger.error("请求出错...", e);
// 			} finally {
// 				closeConnection(post);
// 			}
// 		}
// 		return null;
// 	}
//
// 	public String postString(String url, String string) {
// 		PostMethod post = new PostMethod(url);
// 		for (int i = 0; i < REPEAT_TIMES; i++) {
// 			logger.debug("正在发送请求，当前第" + (i + 1) + "次...");
// 			try {
// 				RequestEntity requestEntity = new StringRequestEntity(string, null, null);
// 				post.setRequestEntity(requestEntity);
// 				int state = httpClient.executeMethod(post);
// 				logger.debug("请求状态" + state);
// 				if(state == HttpStatus.SC_OK) {
// 					InputStream inputStream = post.getResponseBodyAsStream();
// 					String data = StringUtil.fromInputStream(inputStream);
// 					logger.debug("成功获取到数据，长度为" + data.length());
// 					return data;
// 				}
// 			} catch(ConnectTimeoutException e) {
// 				logger.error("请求超时...Connect Timeout");
// 			} catch(SocketTimeoutException e) {
// 				logger.error("请求超时...Socket Timeout");
// 			} catch (Exception e) {
// 				logger.error("请求出错...", e);
// 			} finally {
// 				closeConnection(post);
// 			}
// 		}
// 		return null;
// 	}
//
// 	/**
// 	 * 关闭连接
// 	 * @param method
// 	 */
// 	private void closeConnection(HttpMethod method) {
// 		method.releaseConnection();
// 		MultiThreadedHttpConnectionManager manager = (MultiThreadedHttpConnectionManager) httpClient.getHttpConnectionManager();
// 		manager.closeIdleConnections(0);
// 	}
//
// }