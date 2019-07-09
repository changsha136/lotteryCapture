package lottery.domains.capture.jobs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import javautils.date.Moment;
import javautils.http.HttpClientUtil;
import lottery.domains.capture.sites.cpk.CPKBean;
import lottery.domains.capture.sites.cpn52.CpnBean;
import lottery.domains.capture.utils.CodeValidate;
import lottery.domains.capture.utils.ExpectValidate;
import lottery.domains.content.biz.LotteryOpenCodeService;
import lottery.domains.content.dao.LotteryCrawlerStatusDao;
import lottery.domains.content.entity.LotteryOpenCode;
import net.sf.json.JSONException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * 彩票控开奖源 注意频率，同一彩种间隔必须大于3秒，否则会封IP，该接口只能用于一个环境，否则会封IP
 */
@Component
public class Cpn52Job {
	private static final String URL = "http://www.cpn52.com/api/game-lottery/query-trend?mode=times&screen=30";
	// 彩票code列表
	private static final Map<String, String> LOTTERIES = new HashMap<>();
	static {
		LOTTERIES.put("cqssc", "cqssc"); // 重庆时时彩
		LOTTERIES.put("ocqssc", "ocqssc"); // 旧重庆时时彩
		LOTTERIES.put("bjpk10", "bjpk10"); // 北京PK10
		LOTTERIES.put("objpk10", "objpk10"); // 旧北京PK10

		LOTTERIES.put("fc3d", "fc3d"); // 福彩3D
		LOTTERIES.put("pl3", "pl3"); // 排列三

	}

	private static final Logger logger = LoggerFactory
			.getLogger(Cpn52Job.class);

	@Autowired
	private LotteryOpenCodeService lotteryOpenCodeService;

	@Autowired
	private LotteryCrawlerStatusDao lotteryCrawlerStatusDao;

	private static boolean isRuning = false;

	@Scheduled(cron = "0,5,10,15,20,25,30,35,40,45,50,55 * 0-3,7-23 * * *")
	// 注意频率，同一彩种间隔必须大于3秒，否则会封IP，该接口只能用于一个环境，否则会封IP
	// @PostConstruct
	public void execute() {
		synchronized (Cpn52Job.class) {
			if (isRuning == true) {
				return;
			}
			isRuning = true;
		}

		try {
			logger.debug("开始抓取彩票控开奖数据>>>>>>>>>>>>>>>>");

			long start = System.currentTimeMillis();
			start();
			long spend = System.currentTimeMillis() - start;

			logger.debug("完成抓取彩票控开奖数据>>>>>>>>>>>>>>>>耗时{}", spend);
		} catch (Exception e) {
			logger.error("抓取彩票控开奖数据出错", e);
		} finally {
			isRuning = false;
		}
	}

	private void start() {
		for (String lottery : LOTTERIES.keySet()) {
			try {
				String result = getResult(lottery);
				handleData(lottery, result);
			} catch (Exception e) {
				logger.error("抓取彩票控" + lottery + "开奖数据出错", e);
			}
		}
	}

	public String getResult(String name) {
		String url = URL + "&code=" + name;
		String charset = "UTF-8";
		String result = get(url, charset);
		return result;
	}

	@SuppressWarnings("rawtypes")
	private void handleData(String realName, String result) {
		if (StringUtils.isEmpty(result)) {
			return;
		}

		JSONObject jsonTemp = JSON.parseObject(result);
		JSONObject jsonObject = jsonTemp.getJSONObject("data");
		JSONArray jsonArr = jsonObject.getJSONArray("result");
		System.out.println("111" + jsonArr);
		List<CpnBean> openCodes = JSON.parseArray(jsonArr.toJSONString(),
				CpnBean.class);
		if (CollectionUtils.isEmpty(openCodes)) {
			return;
		}

		// 处理数据
		for (CpnBean openCode : openCodes) {
			handleBean(realName, openCode);
		}
	}

	private boolean handleBean(String name, CpnBean bean) {
		String expect=null;
		String realName = LOTTERIES.get(name);
		switch (name) {
			case "cqssc":
			case "ocqssc":
				expect = bean.getIssue();
				expect = expect.substring(0, 8) + "-" + expect.substring(8);
				break;
			case "fc3d":
			case "pl3":
				expect = bean.getIssue();
				break;
			case "bjpk10":
			case "objpk10":
				expect = bean.getIssue();
				break;
			default:
				break;
		}
		LotteryOpenCode dbData = lotteryOpenCodeService.get(realName,
				expect);

		if (dbData != null) {
			if (!dbData.getCode().equals(bean.getCode())) {
				logger.error("抓取时遇到错误：抓取{}期开奖号码{}与数据库已有开奖号码{}不符",
						bean.getIssue(), bean.getCode(), dbData.getCode());
				return false;
			}
			return true;
		}

		// 如果本期和上奖开奖号码相同，那么把开奖号码状态改为无效撤单
		LotteryOpenCode lotteryOpenCode = new LotteryOpenCode();
		lotteryOpenCode.setInterfaceTime(bean.getGmtCreateStr());
		lotteryOpenCode.setLottery(realName);
		lotteryOpenCode.setTime(new Moment().toSimpleTime());
		lotteryOpenCode.setOpenStatus(0);
		lotteryOpenCode.setRemarks("www.cpn52.com");
		lotteryOpenCode.setCode(bean.getCode());
		lotteryOpenCode.setExpect(expect);

		if (StringUtils.isEmpty(lotteryOpenCode.getCode())
				|| StringUtils.isEmpty(lotteryOpenCode.getExpect())) {
			return false;
		}

		if (CodeValidate.validate(realName, lotteryOpenCode.getCode()) == false) {
			logger.error(  realName + "抓取号码"
					+ lotteryOpenCode.getCode() + "错误");
			return false;
		}

		if (ExpectValidate.validate(realName, lotteryOpenCode.getExpect()) == false) {
			logger.error(  realName + "抓取期数"
					+ lotteryOpenCode.getExpect() + "错误");
			return false;
		}

		boolean added = lotteryOpenCodeService.add(lotteryOpenCode, true);
		if (added) {
			logger.info("官网成功抓取腾讯分分彩{}期开奖号码{}", bean.getIssue(), bean.getCode());
			LotteryOpenCode insertOpenCode = new LotteryOpenCode(realName,
					lotteryOpenCode.getExpect(), lotteryOpenCode.getCode(),
					lotteryOpenCode.getTime(), lotteryOpenCode.getOpenStatus(),
					null, lotteryOpenCode.getRemarks());
			insertOpenCode.setInterfaceTime(insertOpenCode.getInterfaceTime());
			lotteryOpenCodeService.add(insertOpenCode, false);
		}

		return added;
	}

	public static String get(String urlAll, String charset) {
		try {
			String result = HttpClientUtil.get(urlAll, null, 30000);
			return result;
		} catch (Exception e) {
			logger.error("请求彩票控出错", e);
			return null;
		}
	}

}