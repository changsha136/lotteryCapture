package lottery.domains.content.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;

import static javax.persistence.GenerationType.IDENTITY;

import javax.persistence.Id;
import javax.persistence.Table;

import lottery.domains.content.global.Database;

/**
 * LotteryCrawlerStatus entity. @author MyEclipse Persistence Tools
 */
@Entity
@Table(name = "lottery_crawler_status", catalog = Database.DBNAME)
public class LotteryCrawlerStatus implements java.io.Serializable {

	// Fields

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Integer id;
	private String showName;
	private String shortName;
	private Integer times;
	private String lastUpdate;
	private String lastExpect;

	// Constructors

	/** default constructor */
	public LotteryCrawlerStatus() {
	}

	/** full constructor */
	public LotteryCrawlerStatus(String showName, String shortName,
			Integer times, String lastUpdate, String lastExpect) {
		this.showName = showName;
		this.shortName = shortName;
		this.times = times;
		this.lastUpdate = lastUpdate;
		this.lastExpect = lastExpect;
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

	@Column(name = "show_name", nullable = false, length = 128)
	public String getShowName() {
		return this.showName;
	}

	public void setShowName(String showName) {
		this.showName = showName;
	}

	@Column(name = "short_name", nullable = false, length = 32)
	public String getShortName() {
		return this.shortName;
	}

	public void setShortName(String shortName) {
		this.shortName = shortName;
	}

	@Column(name = "times", nullable = false)
	public Integer getTimes() {
		return this.times;
	}

	public void setTimes(Integer times) {
		this.times = times;
	}

	@Column(name = "last_update", nullable = false, length = 19)
	public String getLastUpdate() {
		return this.lastUpdate;
	}

	public void setLastUpdate(String lastUpdate) {
		this.lastUpdate = lastUpdate;
	}

	@Column(name = "last_expect", nullable = false, length = 32)
	public String getLastExpect() {
		return this.lastExpect;
	}

	public void setLastExpect(String lastExpect) {
		this.lastExpect = lastExpect;
	}

}