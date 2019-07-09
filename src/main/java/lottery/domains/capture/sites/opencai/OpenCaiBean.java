package lottery.domains.capture.sites.opencai;

/**
 * 
 * @author ROOT
 *
 */
public class OpenCaiBean {

	private String code; // 菜种
	private String expect;// 期号
	private String opencode;// 开奖号码
	private String opentime;// 开奖时间
	private String opentimestamp;// 时间搓

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getExpect() {
		return expect;
	}

	public void setExpect(String expect) {
		this.expect = expect;
	}

	public String getOpencode() {
		return opencode;
	}

	public void setOpencode(String opencode) {
		this.opencode = opencode;
	}

	public String getOpentime() {
		return opentime;
	}

	public void setOpentime(String opentime) {
		this.opentime = opentime;
	}

	public String getOpentimestamp() {
		return opentimestamp;
	}

	public void setOpentimestamp(String opentimestamp) {
		this.opentimestamp = opentimestamp;
	}

}
