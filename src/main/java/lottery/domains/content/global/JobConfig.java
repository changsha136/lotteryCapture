package lottery.domains.content.global;

import org.springframework.stereotype.Component;

@Component
public class JobConfig {

	// 重庆、江西、新疆
	public boolean isKuai168 = false;

	// 河南481
	public boolean isExeHn481 = false;

	// OpenCai
	public boolean isExeOpenCai = false;

	// 时时彩
	public boolean isExeSsc = false;

	// 11选5
	public boolean isExe11x5 = false;

	// 快三
	public boolean isExeK3 = false;

	// 北京快乐8、pk10、福彩3d、排列三
	public boolean isExeOthers = false;

}