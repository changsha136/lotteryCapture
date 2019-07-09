package javautils.jdbc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class SQLUtil {

	private static final Logger logger = LoggerFactory.getLogger(SQLUtil.class);
	
	// ------------------------------- 增删改通用 -------------------------------//
	public boolean aduSql(Connection conn, String sql) {
		boolean flag = false;
		try {
			if (conn != null) {
				PreparedStatement ps = conn.prepareStatement(sql);
				ps.executeUpdate();
				ps.close();
				flag = true;
			}
		} catch (Exception e) {
			logger.error("aduSql异常", e);
			flag = false;
		}
		return flag;
	}

	public static boolean aduSql(Connection conn, String sql, Object[] params) {
		boolean flag = false;
		try {
			if (conn != null) {
				PreparedStatement ps = conn.prepareStatement(sql);
				if (params != null) {
					for (int i = 0; i < params.length; i++) {
						ps.setObject(i + 1, params[i]);
					}
				}
				ps.executeUpdate();
				flag = true;
				ps.close();
			}
		} catch (Exception e) {
			logger.error("aduSql异常", e);
			flag = false;
		}
		return flag;
	}

	public static List<Object[]> exList(Connection conn, String sql, Object params[]) {
		ArrayList<Object[]> list = new ArrayList<Object[]>();
		try {
			if (conn != null) {
				PreparedStatement ps = conn.prepareStatement(sql);
				if (params != null) {
					for (int i = 0; i < params.length; i++) {
						ps.setObject(i + 1, params[i]);
					}
				}
				ResultSet rs = ps.executeQuery();
				while (rs.next()) {
					int columns = rs.getMetaData().getColumnCount();
					Object[] objArr = new Object[columns];
					for (int i = 0; i < columns; i++) {
						objArr[i] = rs.getObject(i + 1);
					}
					list.add(objArr);
				}
				rs.close();
				ps.close();
			}
		} catch (Exception e) {
			logger.error("exList异常", e);
		}
		return list;
	}
}
