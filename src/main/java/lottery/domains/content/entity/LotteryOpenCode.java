package lottery.domains.content.entity;

import static javax.persistence.GenerationType.IDENTITY;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import lottery.domains.content.global.Database;

/**
 * LotteryOpenCode entity. @author MyEclipse Persistence Tools
 */
@Entity
@Table(name = "lottery_open_code", catalog = Database.DBNAME, uniqueConstraints = @UniqueConstraint(columnNames = {
		"lottery", "expect", "user_id" }))
public class LotteryOpenCode implements java.io.Serializable {

	// Fields

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Integer id;
	private Integer userId;
	private String lottery;
	private String expect;
	private String code;
	private String time;
	private String interfaceTime; // 接口中返回的时间
	private Integer openStatus;
	private String openTime;
	private String remarks;

	// Constructors

	/** default constructor */
	public LotteryOpenCode() {
	}

	/** minimal constructor */
	public LotteryOpenCode(String lottery, String expect, String code,
			String time, Integer openStatus) {
		this.lottery = lottery;
		this.expect = expect;
		this.code = code;
		this.time = time;
		this.openStatus = openStatus;
	}

	/** full constructor */
	public LotteryOpenCode(String lottery, String expect, String code,
			String time, Integer openStatus, String openTime, String remarks) {
		this.lottery = lottery;
		this.expect = expect;
		this.code = code;
		this.time = time;
		this.openStatus = openStatus;
		this.openTime = openTime;
		this.remarks = remarks;
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

	@Column(name = "user_id")
	public Integer getUserId() {
		return this.userId;
	}

	public void setUserId(Integer userId) {
		this.userId = userId;
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

	@Column(name = "code", nullable = false, length = 128)
	public String getCode() {
		return this.code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	@Column(name = "time", nullable = false, length = 19)
	public String getTime() {
		return this.time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	@Column(name = "interface_time", nullable = false, length = 19)
	public String getInterfaceTime() {
		return interfaceTime;
	}

	public void setInterfaceTime(String interfaceTime) {
		this.interfaceTime = interfaceTime;
	}

	@Column(name = "open_status", nullable = false)
	public Integer getOpenStatus() {
		return this.openStatus;
	}

	public void setOpenStatus(Integer openStatus) {
		this.openStatus = openStatus;
	}

	@Column(name = "open_time")
	public String getOpenTime() {
		return this.openTime;
	}

	public void setOpenTime(String openTime) {
		this.openTime = openTime;
	}

	@Column(name = "remarks", length = 128)
	public String getRemarks() {
		return this.remarks;
	}

	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}

}