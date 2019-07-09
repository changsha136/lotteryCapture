package javautils.datasource;

import com.alibaba.druid.filter.Filter;
import com.alibaba.druid.pool.DruidDataSource;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Nick on 2016/07/06
 */
public class CustomDruidDataSource extends DruidDataSource {
    @Override
    public List<Filter> getProxyFilters() {
        List<Filter> proxyFilters = new ArrayList<>();
        proxyFilters.add(ConfigDruidDataSource.configLog4jFilter());
        proxyFilters.add(ConfigDruidDataSource.configStatFilter());
        return proxyFilters;
    }
}
