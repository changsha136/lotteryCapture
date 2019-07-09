package javautils.http;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * http 配置
 * @author Nick
 * @since 2015-10-2
 */
public class HttpConfigBuilder {
	private static final HttpConfigBuilder INSTANCE = new HttpConfigBuilder();
	private HttpConfigBuilder() {
	}
	
	public static HttpConfigBuilder getInstance() {
        return INSTANCE;
    }
	
	public ConnectionKeepAliveStrategy buildKeepAliveStrategy(final long duration) {
		return new ConnectionKeepAliveStrategy() {
			@Override
			public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
				return duration;
			}
		};
	}
	
	public HttpRequestRetryHandler buildRetryHandler(final int retryCount) {
		return new DefaultHttpRequestRetryHandler(retryCount, false);
	}
	
	public RequestConfig buildRequestConfig(final int timeout) {
		return RequestConfig.custom().setConnectionRequestTimeout(timeout).setConnectTimeout(timeout).setSocketTimeout(timeout).build();
	}

	/**
	 * 新建单个http连接
	 */
	public BasicHttpClientConnectionManager buildBasicHttpClientConnectionManager() {
		BasicHttpClientConnectionManager connectionManager = new BasicHttpClientConnectionManager();
		return connectionManager;
	}

	/**
	 * 新建单个https连接，信任所有
	 */
	public BasicHttpClientConnectionManager buildBasicHttpsClientConnectionManager() {
		try {
			TrustManager[] trustAllCerts = new TrustManager[] {
					new X509TrustManager() {
						public X509Certificate[] getAcceptedIssuers() {
							return null;
						}

						public void checkClientTrusted(X509Certificate[] certs, String authType) {  }

						public void checkServerTrusted(X509Certificate[] certs, String authType) {  }

					}
			};

			SSLContext sslContext = SSLContext.getInstance("TLS");
			sslContext.init(null, trustAllCerts, new SecureRandom());

			HostnameVerifier allHostsValid = new HostnameVerifier() {
				public boolean verify(String hostname, SSLSession session) {
					return true;
				}
			};
			SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext, allHostsValid);
			Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
					.register("http", PlainConnectionSocketFactory.getSocketFactory())
					.register("https", sslSocketFactory)
					.build();
			BasicHttpClientConnectionManager connMgr = new BasicHttpClientConnectionManager(socketFactoryRegistry);
			return connMgr;
		} catch (KeyManagementException e) {
			e.printStackTrace();
			System.out.println("初始化https连接出错");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			System.out.println("初始化https连接出错");
		}

		return null;
	}

	/**
	 * 新建单个https连接，信任所有
	 */
	public BasicHttpClientConnectionManager buildBasicHttpsClientConnectionManager(String sslVersion) {
		try {
			TrustManager[] trustAllCerts = new TrustManager[] {
					new X509TrustManager() {
						public X509Certificate[] getAcceptedIssuers() {
							return null;
						}

						public void checkClientTrusted(X509Certificate[] certs, String authType) {  }

						public void checkServerTrusted(X509Certificate[] certs, String authType) {  }

					}
			};

			SSLContext sslContext = SSLContext.getInstance("TLS");
			sslContext.init(null, trustAllCerts, new SecureRandom());

			HostnameVerifier allHostsValid = new HostnameVerifier() {
				public boolean verify(String hostname, SSLSession session) {
					return true;
				}
			};
			SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext, new String[]{sslVersion}, null, allHostsValid);
			Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
					.register("http", PlainConnectionSocketFactory.getSocketFactory())
					.register("https", sslSocketFactory)
					.build();
			BasicHttpClientConnectionManager connMgr = new BasicHttpClientConnectionManager(socketFactoryRegistry);
			return connMgr;
		} catch (KeyManagementException e) {
			e.printStackTrace();
			System.out.println("初始化https连接出错");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			System.out.println("初始化https连接出错");
		}

		return null;
	}

	/**
	 * 新建https连接池，信任所有
	 */
	public PoolingHttpClientConnectionManager buildPoolingHttpsClientConnectionManager(final int maxTotal, final int defaultMaxPerRoute) {
		try {
			TrustManager[] trustAllCerts = new TrustManager[] {
					new X509TrustManager() {
						public X509Certificate[] getAcceptedIssuers() {
							return null;
						}

						public void checkClientTrusted(X509Certificate[] certs, String authType) {  }

						public void checkServerTrusted(X509Certificate[] certs, String authType) {  }

					}
			};

			SSLContext sslContext = SSLContext.getInstance("TLS");
			sslContext.init(null, trustAllCerts, new SecureRandom());

			HostnameVerifier allHostsValid = new HostnameVerifier() {
				public boolean verify(String hostname, SSLSession session) {
					return true;
				}
			};
			SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext, allHostsValid);
			Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
					.register("http", PlainConnectionSocketFactory.getSocketFactory())
					.register("https", sslSocketFactory)
					.build();
			PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
			connectionManager.setMaxTotal(maxTotal);
			connectionManager.setDefaultMaxPerRoute(defaultMaxPerRoute);
			return connectionManager;
		} catch (KeyManagementException e) {
			e.printStackTrace();
			System.out.println("初始化https连接出错");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			System.out.println("初始化https连接出错");
		}

		return null;
	}

	/**
	 * 新建普通http连接池
	 */
	public PoolingHttpClientConnectionManager buildPoolingHttpClientConnectionManager(final int maxTotal, final int defaultMaxPerRoute) {
		PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
		connectionManager.setMaxTotal(maxTotal);
		connectionManager.setDefaultMaxPerRoute(defaultMaxPerRoute);
		return connectionManager;
	}

	/**
	 * 新建带SSL证书密码的连接池
	 */
	public PoolingHttpClientConnectionManager buildP12PoolingHttpClientConnectionManager(final int maxTotal, final int defaultMaxPerRoute, File p12File, String pwd) {
		try {
			KeyStore ks = KeyStore.getInstance("PKCS12");
			FileInputStream fis = new FileInputStream(p12File);
			ks.load(fis, pwd.toCharArray());
			KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
			kmf.init(ks, pwd.toCharArray());
			KeyManager[] kms = kmf.getKeyManagers();

			//Crete TrustManager to bypass trusted certificate check
			TrustManager[] trustAllCerts = new TrustManager[] {
					new X509TrustManager() {
						public X509Certificate[] getAcceptedIssuers() {
							return null;
						}

						public void checkClientTrusted(X509Certificate[] certs, String authType) {  }

						public void checkServerTrusted(X509Certificate[] certs, String authType) {  }

					}
			};

			SSLContext sslContext = SSLContext.getInstance("TLS");
			sslContext.init(kms, trustAllCerts, new SecureRandom());

			HostnameVerifier allHostsValid = new HostnameVerifier() {
				public boolean verify(String hostname, SSLSession session) {
					return true;
				}
			};
			SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext, allHostsValid);
			Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
					.register("http", PlainConnectionSocketFactory.getSocketFactory())
					.register("https", sslSocketFactory)
					.build();
			PoolingHttpClientConnectionManager connMgr = new PoolingHttpClientConnectionManager( socketFactoryRegistry);
			connMgr.setMaxTotal(maxTotal);
			connMgr.setDefaultMaxPerRoute(defaultMaxPerRoute);

			return connMgr;
		} catch (KeyStoreException e) {
			e.printStackTrace();
			System.out.println("初始化P12连接出错");
		} catch (CertificateException e) {
			e.printStackTrace();
			System.out.println("初始化P12连接出错");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			System.out.println("初始化P12连接出错");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("初始化P12连接出错");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("初始化P12连接出错");
		} catch (UnrecoverableKeyException e) {
			e.printStackTrace();
			System.out.println("初始化P12连接出错");
		} catch (KeyManagementException e) {
			e.printStackTrace();
			System.out.println("初始化P12连接出错");
		}

		return null;
	}
}
