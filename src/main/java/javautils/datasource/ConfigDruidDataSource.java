package javautils.datasource;

import com.alibaba.druid.filter.logging.Log4jFilter;
import com.alibaba.druid.filter.stat.StatFilter;

/**
 * Created by Nick on 2016/07/06
 */
public class ConfigDruidDataSource {
    public static Log4jFilter configLog4jFilter() {
        Log4jFilter filter = new Log4jFilter();
        filter.setStatementExecuteQueryAfterLogEnabled(false);
        filter.setStatementExecuteUpdateAfterLogEnabled(false);
        filter.setStatementExecuteBatchAfterLogEnabled(false);
        filter.setStatementExecuteAfterLogEnabled(false);
        filter.setStatementExecutableSqlLogEnable(false);
        filter.setStatementParameterSetLogEnabled(false);
        filter.setStatementParameterClearLogEnable(false);
        filter.setStatementCreateAfterLogEnabled(false);
        filter.setStatementCloseAfterLogEnabled(false);
        filter.setStatementPrepareAfterLogEnabled(false);
        filter.setStatementPrepareCallAfterLogEnabled(false);
        filter.setStatementLogEnabled(false);
        filter.setStatementLogErrorEnabled(false);
        return filter;
    }

    public static StatFilter configStatFilter() {
        StatFilter filter = new StatFilter();
        filter.setSlowSqlMillis(5000);
        filter.setLogSlowSql(true);
        filter.setMergeSql(true);
        return filter;
    }
}