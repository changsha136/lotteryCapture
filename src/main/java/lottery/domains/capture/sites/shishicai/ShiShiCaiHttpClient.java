// package lottery.domains.capture.sites.shishicai;
//
// import javautils.StringUtil;
// import org.apache.commons.httpclient.*;
// import org.apache.commons.httpclient.methods.GetMethod;
// import org.apache.commons.httpclient.methods.PostMethod;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// import java.net.SocketTimeoutException;
//
// public class ShiShiCaiHttpClient {
//
// 	private Logger logger;
//
// 	private HttpClient httpClient;
// 	private final int TIMEOUT = 10 * 1000;
// 	private final int SO_TIMEOUT = 10 * 1000;
// 	private final int REPEAT_TIMES = 3;
//
// 	// 刷新Cookie用
// 	private final int COOKIE_TIMEOUT = 1 * 60 * 60 * 1000;
// 	private long lastUpdate;
//
// 	public ShiShiCaiHttpClient(String name) {
// 		logger = LoggerFactory.getLogger(ShiShiCaiHttpClient.class.getSimpleName() + "-" + name.toUpperCase());
// 		httpClient = new HttpClient(new MultiThreadedHttpConnectionManager());
// 		httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(TIMEOUT);
// 		httpClient.getHttpConnectionManager().getParams().setSoTimeout(SO_TIMEOUT);
// 	}
//
// 	private boolean handleCookie(String getUrl) {
// 		GetMethod get = new GetMethod(getUrl);
// 		for (int i = 0; i < REPEAT_TIMES; i++) {
// 			logger.debug("正在请求Cookie，当前第" + (i + 1) + "次...");
// 			try {
// 				int state = httpClient.executeMethod(get);
// 				logger.debug("请求状态" + state);
// 				if(state == HttpStatus.SC_OK) {
// 					Cookie[] cookies = httpClient.getState().getCookies();
// 					for (Cookie cookie : cookies) {
// 						if(cookie.getName().indexOf("Html_v") != -1) {
// 							logger.debug("请求Cookie成功了...");
// 							lastUpdate = System.currentTimeMillis();
// 							return true;
// 						}
// 					}
// 				}
// 			} catch(ConnectTimeoutException e) {
// 				logger.error("请求时时彩数据超时了...Connect Timeout");
// 			} catch(SocketTimeoutException e) {
// 				logger.error("请求时时彩数据超时了...Socket Timeout");
// 			} catch (Exception e) {
// 				logger.error("请求Cookie出错了...", e);
// 			} finally {
// 				closeConnection(get);
// 			}
// 		}
// 		return false;
// 	}
//
// 	private String getCookie(String getUrl, String date) {
// 		logger.debug("获取Cookie数据...");
// 		if(System.currentTimeMillis() > lastUpdate + COOKIE_TIMEOUT) {
// 			logger.debug("Cookie过期了，重新获取Cookie...");
// 			boolean flag = handleCookie(getUrl);
// 			if(flag) {
// 				return getCookie(getUrl, date);
// 			}
// 		}
// 		StringBuffer sb = new StringBuffer();
// 		Cookie[] cookies = httpClient.getState().getCookies();
// 		for (Cookie cookie : cookies) {
// 			if(cookie.getName().indexOf("Html_v") != -1) {
// 				sb.append(cookie.toString());
// 			}
// 		}
// 		if(!StringUtil.isNotNull(sb.toString())) {
// 			logger.debug("没有Cookie数据...");
// 			boolean flag = handleCookie(getUrl);
// 			if(flag) {
// 				return getCookie(getUrl, date);
// 			}
// 		}
// 		sb.append(";ssc_user_LandingPage=" + getUrl + date + "/");
// 		sb.append(";ssc_user_RegEnterPage=" + getUrl + date + "/");
// 		return sb.toString();
// 	}
//
// 	public String handleData(String getUrl, String postUrl, String lottery, String date) {
// 		StringBuffer sb = new StringBuffer();
// 		String cookie = getCookie(getUrl, date);
// 		if(StringUtil.isNotNull(cookie)) {
// 			PostMethod post = new PostMethod(postUrl);
// 			post.setRequestHeader("Referer", getUrl);
// 			post.setRequestHeader("Cookie", cookie);
// 			NameValuePair[] params = { new NameValuePair("lottery", lottery), new NameValuePair("date", date) };
// 			post.setRequestBody(params);
// 			for (int i = 0; i < REPEAT_TIMES; i++) {
// 				logger.debug("正在请求开奖数据，当前第" + (i + 1) + "次...");
// 				try {
// 					int state = httpClient.executeMethod(post);
// 					logger.debug("请求状态" + state);
// 					if(state == HttpStatus.SC_OK) {
// 						sb.append(post.getResponseBodyAsString());
// 						logger.debug("成功获取到开奖数据，长度为" + sb.length());
// 						return sb.toString();
// 					}
// 				} catch(ConnectTimeoutException e) {
// 					logger.error("请求时时彩数据超时了...Connect Timeout");
// 				} catch(SocketTimeoutException e) {
// 					logger.error("请求时时彩数据超时了...Socket Timeout");
// 				} catch (Exception e) {
// 					logger.error("请求开奖数据出错了...", e);
// 				} finally {
// 					closeConnection(post);
// 				}
// 			}
// 		} else {
// 			logger.debug("没有Cookie...无法获取数据...");
// 		}
// 		return sb.toString();
// 	}
//
// 	private void closeConnection(HttpMethod method) {
// 		method.releaseConnection();
// 		MultiThreadedHttpConnectionManager manager = (MultiThreadedHttpConnectionManager) httpClient.getHttpConnectionManager();
// 		manager.closeIdleConnections(0);
// 	}
//
// }