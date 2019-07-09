package lottery.domains.content.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;

import static javax.persistence.GenerationType.IDENTITY;

import javax.persistence.Id;
import javax.persistence.Table;

import lottery.domains.content.global.Database;

/**
 * LotteryCrawlerNotice entity. @author MyEclipse Persistence Tools
 */
@Entity
@Table(name = "lottery_crawler_notice", catalog = Database.DBNAME)
public class LotteryCrawlerNotice implements java.io.Serializable {

	// Fields

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Integer id;
	private String name;
	private String type;
	private String host;
	private Integer port;
	private String username;
	private String password;
	private String method;
	private String url;
	private Integer status;

	// Constructors

	/** default constructor */
	public LotteryCrawlerNotice() {
	}

	/** minimal constructor */
	public LotteryCrawlerNotice(String name, String type, String host,
			Integer port, String method, Integer status) {
		this.name = name;
		this.type = type;
		this.host = host;
		this.port = port;
		this.method = method;
		this.status = status;
	}

	/** full constructor */
	public LotteryCrawlerNotice(String name, String type, String host,
			Integer port, String username, String password, String method,
			String url, Integer status) {
		this.name = name;
		this.type = type;
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.method = method;
		this.url = url;
		this.status = status;
	}

	// Property accessors
	@Id
	@GeneratedValue(strategy = IDENTITY)
	@Column(name = "id", unique = true, nullable = false)
	public Integer getId() {
		return this.id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	@Column(name = "name", nullable = false)
	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Column(name = "type", nullable = false, length = 32)
	public String getType() {
		return this.type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Column(name = "host", nullable = false, length = 256)
	public String getHost() {
		return this.host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	@Column(name = "port", nullable = false)
	public Integer getPort() {
		return this.port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	@Column(name = "username")
	public String getUsername() {
		return this.username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	@Column(name = "password")
	public String getPassword() {
		return this.password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@Column(name = "method", nullable = false, length = 32)
	public String getMethod() {
		return this.method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	@Column(name = "url")
	public String getUrl() {
		return this.url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	@Column(name = "status", nullable = false)
	public Integer getStatus() {
		return this.status;
	}

	public void setStatus(Integer status) {
		this.status = status;
	}

}