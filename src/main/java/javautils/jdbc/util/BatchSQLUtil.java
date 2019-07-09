package javautils.jdbc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BatchSQLUtil {

	private static final Logger logger = LoggerFactory.getLogger(BatchSQLUtil.class);

	private Connection conn;
	private PreparedStatement ps;

	public BatchSQLUtil(Connection conn, String sql) {
		try {
			this.conn = conn;
			this.conn.setAutoCommit(false);
			ps = this.conn.prepareStatement(sql);
		} catch (Exception e) {
			logger.error("BatchSQLUtil异常", e);
		}
	}

	public void addCount(Object[] param) {
		try {
			for (int i = 0; i < param.length; i++) {
				ps.setObject(i + 1, param[i]);
			}
			ps.addBatch();
		} catch (SQLException e) {
			logger.error("addCount异常", e);
		}
	}

	public void commit() {
		try {
			ps.executeBatch();
			conn.commit();
			ps.clearBatch();
		} catch (SQLException e) {
			logger.error("commit异常", e);
		}
	}

	public void close() {
		try {
			ps.close();
			conn.close();
		} catch (SQLException e) {
			logger.error("close异常", e);
		}
	}
}