package lottery.domains.capture.sites.pcqq;

import com.alibaba.fastjson.annotation.JSONField;
import javautils.date.Moment;

/**
 * Created by Nick on 2017-07-28.
 */
public class PCQQBean {
    @JSONField(deserialize = false)
    private String onlinetime = new Moment().toSimpleTime();
    @JSONField(name = "c")
    private int onlinenumber;
    @JSONField(deserialize = false)
    private int onlinechange;

    public String getOnlinetime() {
        return onlinetime;
    }

    public void setOnlinetime(String onlinetime) {
        this.onlinetime = onlinetime;
    }

    public int getOnlinenumber() {
        return onlinenumber;
    }

    public void setOnlinenumber(int onlinenumber) {
        this.onlinenumber = onlinenumber;
    }

    public int getOnlinechange() {
        return onlinechange;
    }

    public void setOnlinechange(int onlinechange) {
        this.onlinechange = onlinechange;
    }
}
