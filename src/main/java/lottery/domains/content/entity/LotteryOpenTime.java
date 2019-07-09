package lottery.domains.content.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;

import static javax.persistence.GenerationType.IDENTITY;

import javax.persistence.Id;
import javax.persistence.Table;

import lottery.domains.content.global.Database;

/**
 * LotteryOpenTime entity. @author MyEclipse Persistence Tools
 */
@Entity
@Table(name = "lottery_open_time", catalog = Database.DBNAME)
public class LotteryOpenTime implements java.io.Serializable {

	// Fields

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Integer id;
	private String lottery;
	private String expect;
	private String startTime;
	private String stopTime;
	private String openTime;
	private Boolean isTodayExpect;

	// Constructors

	/** default constructor */
	public LotteryOpenTime() {
	}

	/** full constructor */
	public LotteryOpenTime(String lottery, String expect, String startTime,
			String stopTime, String openTime, Boolean isTodayExpect) {
		this.lottery = lottery;
		this.expect = expect;
		this.startTime = startTime;
		this.stopTime = stopTime;
		this.openTime = openTime;
		this.isTodayExpect = isTodayExpect;
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

	@Column(name = "lottery", nullable = false, length = 32)
	public String getLottery() {
		return this.lottery;
	}

	public void setLottery(String lottery) {
		this.lottery = lottery;
	}

	@Column(name = "expect", nullable = false, length = 32)
	public String getExpect() {
		return this.expect;
	}

	public void setExpect(String expect) {
		this.expect = expect;
	}

	@Column(name = "start_time", nullable = false, length = 19)
	public String getStartTime() {
		return this.startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	@Column(name = "stop_time", nullable = false, length = 19)
	public String getStopTime() {
		return this.stopTime;
	}

	public void setStopTime(String stopTime) {
		this.stopTime = stopTime;
	}

	@Column(name = "open_time", nullable = false, length = 19)
	public String getOpenTime() {
		return this.openTime;
	}

	public void setOpenTime(String openTime) {
		this.openTime = openTime;
	}

	@Column(name = "is_today_expect", nullable = false)
	public Boolean getIsTodayExpect() {
		return this.isTodayExpect;
	}

	public void setIsTodayExpect(Boolean isTodayExpect) {
		this.isTodayExpect = isTodayExpect;
	}

}