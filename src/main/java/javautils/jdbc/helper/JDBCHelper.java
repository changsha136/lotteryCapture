package javautils.jdbc.helper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class JDBCHelper {

	public final static String MYSQL = "com.mysql.jdbc.Driver";

	public static Connection openConnection(String url, String username,
			String password, String type) {
		Connection conn = null;
		try {
			Class.forName(type);
			conn = DriverManager.getConnection(url, username, password);
		} catch (Exception e) {}
		return conn;
	}

	public static boolean executeUpdate(String sql, Connection conn) {
		boolean flag = false;
		if (conn != null) {
			PreparedStatement pst = null;
			try {
				pst = conn.prepareStatement(sql);
				int count = pst.executeUpdate();
				flag = count > 0 ? true : false;
			} catch (Exception e) {
			} finally {
				try {
					if (pst != null) pst.close();
					if (conn != null) conn.close();
				} catch (SQLException e) {}
			}
		}
		return flag;
	}

	public static JSONArray executeQuery(String sql, Connection conn) {
		JSONArray list = null;
		if (conn != null) {
			PreparedStatement pst = null;
			try {
				pst = conn.prepareStatement(sql);
				ResultSet rs = pst.executeQuery();
				list = toJsonArray(rs);
			} catch (Exception e) {} finally {
				try {
					if (pst != null) pst.close();
					if (conn != null) conn.close();
				} catch (SQLException e) {}
			}
		}
		return list;
	}

	public static JSONArray toJsonArray(ResultSet rs) throws Exception {
		JSONArray list = new JSONArray();
		if (rs != null) {
			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();
			while (rs.next()) {
				JSONObject obj = new JSONObject();
				for (int i = 0; i < columnCount; i++) {
					String columnName = metaData.getColumnLabel(i + 1);
					String value = rs.getString(columnName);
					obj.put(columnName, value);
				}
				list.add(obj);
			}
		}
		return list;
	}

	/**
	 * java:comp/env/jdbc/books
	 * @param jndi
	 * @return
	 */
	public static Connection openConnection(String jndi) {
		Connection conn = null;
		try {
			Context context = new InitialContext();
			DataSource source = (DataSource) context.lookup(jndi);
			conn = source.getConnection();
		} catch (Exception e) {}
		return conn;
	}

	public static void main(String[] args) {
		String url = "jdbc:mysql://192.168.1.111:3306/test?useUnicode=true&characterEncoding=utf-8";
		String username = "root";
		String password = "root";
		Connection conn = JDBCHelper.openConnection(url, username, password, JDBCHelper.MYSQL);
		String sql = "select * from user";
		JDBCHelper.executeQuery(sql, conn);
	}

}
