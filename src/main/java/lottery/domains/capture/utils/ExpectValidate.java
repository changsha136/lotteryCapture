package lottery.domains.capture.utils;

import javautils.StringUtil;

/**
 * Created by Nick on 2017/1/6.
 */
public class ExpectValidate {
    public static boolean validate(String lottery, String expect) {
        switch (lottery) {
            case "cqssc":
            case "ocqssc":
            case "xjssc":
            case "tjssc":
                return isSsc(expect);
            case "tw5fc":
                return isTw5fc(expect);
            case "sd11x5":
            case "gd11x5":
            case "jx11x5":
            case "ah11x5":
                return is11x5(expect);
            case "jsk3":
            case "ahk3":
            case "jlk3":
            case "hbk3":
            case "shk3":
                return isK3(expect);
            case "fc3d":
            case "pl3":
                return is3d(expect);
            case "bjkl8":
            case "bj5fc":
                return isBjkl8(expect);
            case "bjpk10":
            case "objpk10":
                return isBjpk10(expect);
            case "jnd3d5fc":
                return isJnd3d5fc(expect);
            case "txffc":
                return isTxffc(expect);
            default:
                return true;
        }
    }

    public static boolean isSsc(String expect) {
        // 是否为空
        if (!StringUtil.isNotNull(expect))
            return false;
        if (expect.length() != 12)
            return false;
        String[] expects = expect.split("-");
        // 长度
        if (expects.length != 2)
            return false;

        String date = expects[0];
        String exp = expects[1];

        if (!StringUtil.isInteger(date))
            return false;
        if (!StringUtil.isInteger(exp))
            return false;
        if (date.length() != 8)
            return false;
        if (exp.length() != 3)
            return false;

        return true;
    }

    public static boolean isTxffc(String expect) {
        // 是否为空
        if (!StringUtil.isNotNull(expect))
            return false;
        if (expect.length() != 13)
            return false;
        String[] expects = expect.split("-");
        // 长度
        if (expects.length != 2)
            return false;

        String date = expects[0];
        String exp = expects[1];

        if (!StringUtil.isInteger(date))
            return false;
        if (!StringUtil.isInteger(exp))
            return false;
        if (date.length() != 8)
            return false;
        if (exp.length() != 4)
            return false;

        return true;
    }

    public static boolean isTw5fc(String expect) {
        // 是否为空
        if (!StringUtil.isNotNull(expect))
            return false;
        if (!StringUtil.isInteger(expect))
            return false;
        if (expect.length() != 9)
            return false;

        return true;
    }

    public static boolean is11x5(String expect) {
        // 是否为空
        if (!StringUtil.isNotNull(expect))
            return false;
        if (expect.length() != 12)
            return false;
        String[] expects = expect.split("-");
        // 长度
        if (expects.length != 2)
            return false;

        String date = expects[0];
        String exp = expects[1];

        if (!StringUtil.isInteger(date))
            return false;
        if (!StringUtil.isInteger(exp))
            return false;
        if (date.length() != 8)
            return false;
        if (exp.length() != 3)
            return false;

        return true;
    }

    public static boolean isK3(String expect) {
        // 是否为空
        if (!StringUtil.isNotNull(expect))
            return false;
        if (expect.length() != 12)
            return false;
        String[] expects = expect.split("-");
        // 长度
        if (expects.length != 2)
            return false;

        String date = expects[0];
        String exp = expects[1];

        if (!StringUtil.isInteger(date))
            return false;
        if (!StringUtil.isInteger(exp))
            return false;
        if (date.length() != 8)
            return false;
        if (exp.length() != 3)
            return false;

        return true;
    }

    public static boolean is3d(String expect) {
        // 是否为空
        if (!StringUtil.isNotNull(expect))
            return false;
        if (!StringUtil.isInteger(expect))
            return false;
        if (expect.length() != 7)
            return false;

        return true;
    }

    public static boolean isBjkl8(String expect) {
        // 是否为空
        if (!StringUtil.isNotNull(expect))
            return false;
        if (!StringUtil.isInteger(expect))
            return false;
        if (expect.length() != 6)
            return false;

        return true;
    }

    public static boolean isBjpk10(String expect) {
        // 是否为空
        if (!StringUtil.isNotNull(expect))
            return false;
        if (!StringUtil.isInteger(expect))
            return false;
        if (expect.length() != 6)
            return false;

        return true;
    }

    public static boolean isJnd3d5fc(String expect) {
        // 是否为空
        if (!StringUtil.isNotNull(expect))
            return false;
        if (!StringUtil.isInteger(expect))
            return false;
        if (expect.length() != 7)
            return false;

        return true;
    }
}
