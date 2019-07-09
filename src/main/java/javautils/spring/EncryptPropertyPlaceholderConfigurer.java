package javautils.spring;

import javautils.StringUtil;
import javautils.encrypt.DESUtil;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

/**
 * Created by Nick on 2016/10/3.
 */
public class EncryptPropertyPlaceholderConfigurer extends PropertyPlaceholderConfigurer {
    private static final String KEY = "#$ddw134R$#G#DSFW@#?!@#!@#$CCCREW1";
    private static final DESUtil DES_UTIL = DESUtil.getInstance();

    @Override
    protected String convertProperty(String propertyName, String propertyValue) {
        if (StringUtil.isNotNull(propertyValue) && propertyValue.endsWith("|e")) {
            String tempValue = propertyValue.substring(0, propertyValue.length() - 2);
            String decryptValue = DES_UTIL.decryptStr(tempValue, KEY);
            return decryptValue;
        } else {
            return propertyValue;
        }
    }
}
