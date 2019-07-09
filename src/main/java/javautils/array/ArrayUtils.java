package javautils.array;

import java.util.List;

public class ArrayUtils {
	
	/**
	 * 针对数据库in
	 * @param ids
	 * @return
	 */
	public static String transInIds(int[] ids) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0, j = ids.length; i < j; i++) {
			sb.append(ids[i]);
			if(i < j - 1) {
				sb.append(",");
			}
		}
		return sb.toString();
	}
	
	public static String transInIds(Integer[] ids) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0, j = ids.length; i < j; i++) {
			sb.append(ids[i].intValue());
			if(i < j - 1) {
				sb.append(",");
			}
		}
		return sb.toString();
	}
	
	/**
	 * 针对数据库in
	 * @param ids
	 * @return
	 */
	public static String transInIds(List<Integer> ids) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0, j = ids.size(); i < j; i++) {
			sb.append(ids.get(i).intValue());
			if(i < j - 1) {
				sb.append(",");
			}
		}
		return sb.toString();
	}
	
	/**
	 * 存进数据库
	 * @param ids
	 * @return
	 */
	public static String transInsertIds(int[] ids) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0, j = ids.length; i < j; i++) {
			sb.append("[" + ids[i] + "]");
			if(i < j - 1) {
				sb.append(",");
			}
		}
		return sb.toString();
	}
	
	/**
	 * 获取id的时候，解析成id数组
	 * @param ids
	 * @return
	 */
	public static int[] transGetIds(String ids) {
		String[] tmp = ids.replaceAll("\\[|\\]", "").split(",");
		int[] arr = new int[tmp.length];
		for (int i = 0; i < arr.length; i++) {
			arr[i] = Integer.parseInt(tmp[i]);
		}
		return arr;
	}
	
	/**
	 * 转换成字符串
	 * @param list
	 * @return
	 */
	public static String toString(List<Integer> list) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0, j = list.size(); i < j; i++) {
			sb.append(list.get(i));
			if(i < j - 1) {
				sb.append(",");
			}
		}
		return sb.toString();
	}
}