package lottery.utils.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;

/**
 * 文件工具类
 */
public class FileUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(FileUtil.class);
	
	private static FileUtil instance;
	
	private FileUtil() {}
	
	private static synchronized void synInit() {
		if(instance == null) {
			instance = new FileUtil();
		}
	}

	public static FileUtil getInstance() {
		if(instance == null) {
			synInit();
		}
		return instance;
	}
	
	/**
	 * 读取文件
	 * @param sFileName
	 * @return
	 */
	public byte[] eReadFile(String sFileName) {
		byte[] sbStr = new byte[2];
		int rpos = 0;
		try {
			long alth = 0;
			int rsize = 102400;
			byte[] tmpbt = new byte[rsize];
			RandomAccessFile raf = new RandomAccessFile(sFileName, "r");
			alth = raf.length();
			sbStr = new byte[(int) alth];
			while (alth > rpos) {
				raf.seek(rpos);
				tmpbt = new byte[rsize];
				if (alth - rpos < rsize) {
					tmpbt = new byte[(int) (alth - rpos)];
				}
				raf.read(tmpbt);
				for (int i = 0, j = tmpbt.length; i < j; i++) {
					sbStr[rpos + i] = tmpbt[i];
				}
				rpos = rpos + rsize;
			}
			raf.close();
		} catch (Exception e) {
			logger.error("eReadFile异常", e);
		}
		return sbStr;
	}

	/**
	 * 写文件
	 * @param excpath 路径，不存在会自动创建
	 * @param excname 文件名
	 * @param tmpstr
	 * @return
	 */
	public boolean writeFile(String excpath, String excname, byte[] tmpstr) {
		boolean res = false;
		try {
			if (!(new File(excpath).isDirectory())) {
				new File(excpath).mkdirs();
			}
			String tmpPath = excpath + excname;
			FileOutputStream wf = new FileOutputStream(tmpPath, false);
			wf.write(tmpstr);
			wf.flush();
			wf.close();
			return res;
		} catch (Throwable e) {
			logger.error("writeFile异常", e);
		}
		return res;
	}
	
	/**
	 * 续写文件
	 * @param excpath 路径，不存在会自动创建
	 * @param excname 文件名，不存在会自动写成新文件
	 * @param tmpstr
	 * @return
	 */
	public boolean writeFileEX(String excpath, String excname, byte[] tmpstr) {
		boolean res = false;
		try {
			if (!(new File(excpath).isDirectory())) {
				new File(excpath).mkdirs();
			}
			String tmpPath = excpath + excname;
			RandomAccessFile raf = new RandomAccessFile(tmpPath, "rw");
			raf.seek(raf.length());
			raf.setLength(raf.length() + tmpstr.length);
			raf.write(tmpstr);
			raf.close();
			return res;
		} catch (Throwable e) {
			logger.error("writeFileEX异常", e);
		}
		return res;
	}
	
	/**
	 * 读取文件
	 * @param sFileName
	 * @return
	 */
	public byte[] readFile(String sFileName) {
		File file = new File(sFileName);
		if(!file.exists()) {
			return "".getBytes();
		}
		byte[] sbStr = new byte[2];
		int rpos = 0;
		try {
			long alth = 0;
			int rsize = 102400;
			byte[] tmpbt = new byte[rsize];
			RandomAccessFile raf = new RandomAccessFile(sFileName, "r");
			alth = raf.length();
			sbStr = new byte[(int) alth];
			while (alth > rpos) {
				raf.seek(rpos);
				tmpbt = new byte[rsize];
				if (alth - rpos < rsize) {
					tmpbt = new byte[(int) (alth - rpos)];
				}
				raf.read(tmpbt);
				for (int i = 0, j = tmpbt.length; i < j; i++) {
					sbStr[rpos + i] = tmpbt[i];
				}
				rpos = rpos + rsize;
			}
			raf.close();
		} catch (Throwable e) {
			logger.error("readFile异常", e);
			sbStr = null;
		}
		return sbStr;
	}
	
	/**
	 * 获取文件长度
	 * @param sFileName
	 * @return
	 */
	public long getFileLength(String sFileName) {
		long alth = 0;
		try {
			RandomAccessFile raf = new RandomAccessFile(sFileName, "r");
			alth = raf.length();
			raf.close();
		} catch (Throwable e) {
			logger.error("getFileLength异常", e);
		}
		return alth;
	}
	
	/**
	 * 分段读取文件
	 * @param sFileName
	 * @param lpos
	 * @param rlth
	 * @return
	 */
	public byte[] readFile(String sFileName, int lpos, int rlth) {
		byte[] sbStr = new byte[2];
		int rpos = lpos;
		try {
			long alth = 0;
			int rsize = 102400;
			byte[] tmpbt = new byte[rsize];
			RandomAccessFile raf = new RandomAccessFile(sFileName, "r");
			alth = raf.length();
			if ((lpos + rlth) < alth) {
				alth = lpos + rlth;
			}
			sbStr = new byte[(int) (alth - lpos)];
			while (alth > rpos) {
				raf.seek(rpos);
				tmpbt = new byte[rsize];
				if (alth - rpos < rsize) {
					tmpbt = new byte[(int) (alth - rpos)];
				}
				raf.read(tmpbt);
				for (int i = 0, j = tmpbt.length; i < j; i++) {
					sbStr[rpos-lpos + i] = tmpbt[i];
				}
				rpos = rpos + rsize;
			}
			raf.close();
			tmpbt=new byte[1];
		} catch (Throwable e) {
			logger.error("readFile异常", e);
			sbStr = null;
		}
		return sbStr;
	}
	
	/**
	 * 批量读取文件，并组合成新的字符串
	 * @param pathList
	 * @return
	 */
	public String readFile(List<String> pathList) {
		StringBuffer buffer = new StringBuffer();
		for (String file : pathList) {
			try {
				byte[] bt = readFile(file);
				if(bt.length > 0){
					buffer.append(new String(bt));
				}
			} catch (Exception e) {
				logger.info("文件不存在!");
			}
		}
		return buffer.toString();
	}

	/**
	 * 复制源文件到目标路径文件
	 * @param srcFile
	 * @param newFile
	 * @return
	 */
	public boolean copyFile(File srcFile, File newFile) {
		boolean flag = false;
		try {
			if (srcFile.exists()) {
				BufferedInputStream fis = new BufferedInputStream(
						new FileInputStream(srcFile));
				BufferedOutputStream fos = new BufferedOutputStream(
						new FileOutputStream(newFile));
				int byteSize = 0;
				while ((byteSize = fis.read()) != -1) {
					fos.write(byteSize);
				}
				fos.close();
				fis.close();
				flag = true;
			} else {
				return false;
			}
		} catch (Throwable e) {
			logger.error("copyFile异常", e);
			return false;
		}
		return flag;
	}

	public void eMKDir(String eFilePath) {
		try {
			int ipos = eFilePath.lastIndexOf(File.separator);
			String excpath = eFilePath.substring(0, ipos);
			if (!(new File(excpath).isDirectory())) {
				new File(excpath).mkdirs();
			}
		} catch (Throwable e) {
		}
	}

	/**
	 * 删除文件
	 */
	public boolean delFile(File deleteFile) {
		try {
			deleteFile.delete();
		} catch (Throwable e) {
			logger.error("delFile异常", e);
			return false;
		}
		return true;
	}

	/**
	 * 移动文件到指定路径
	 */
	public boolean moveFile(File oldFile, File newFile) {
		if (copyFile(oldFile, newFile) && delFile(oldFile)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 复制文件夹下所有文件到指定路径
	 */
	public boolean copyDir(String srcDirPath, String distDirPath) {
		try {
			(new File(distDirPath)).mkdirs();
			File srcDir = new File(srcDirPath);
			String[] files = srcDir.list();// 取源路径下所有文件
			File temp = null;
			for (int i = 0; i < files.length; i++) {//
				if (srcDirPath.endsWith(File.separator)) {// 源路径以/结尾.
					temp = new File(srcDirPath + files[i]);
				} else {// 源路径不以/结尾.
					temp = new File(srcDirPath + File.separator + files[i]);
				}
				if (temp.isFile()) {// 是文件
					FileInputStream input = new FileInputStream(temp);
					FileOutputStream output = new FileOutputStream(distDirPath
							+ File.separator + (temp.getName()).toString());
					byte[] bytes = new byte[1024];// 1k缓冲
					int len;
					while ((len = input.read(bytes)) != -1) {
						output.write(bytes, 0, len);
					}
					output.flush();
					output.close();
					input.close();
				}
				if (temp.isDirectory()) {// 是文件夹
					copyDir(srcDirPath + File.separator + files[i], distDirPath
							+ File.separator + files[i]);
				}
			}
		} catch (Throwable e) {
			logger.error("copyDir异常", e);
			return false;
		}
		return true;
	}

	public void deleteFolder(File dir) {
		File filelist[] = dir.listFiles();
		int listlen = filelist.length;
		for (int i = listlen-1; i >=0; i--) {
			try {
				if (filelist[i].isDirectory()) {
					deleteFolder(filelist[i]);
				}
			} catch (Throwable e) {
                 e.printStackTrace();
			}
		}
		dir.delete();// 删除当前目录
	}

	public byte[] getFileBytes(String filePath) {
		File file = new File(filePath);
		byte[] bytes = getFileBytes(file);
		return bytes;
	}

	public byte[] getFileBytes(File file) {
		byte[] bt = null;
		BufferedInputStream bis = null;
		ByteArrayOutputStream baos = null;
		try {
			bis = new BufferedInputStream(new FileInputStream(file));
			baos = new ByteArrayOutputStream();
			byte[] bytes = new byte[1024];// 1k缓冲
			int len;
			while ((len = bis.read(bytes)) != -1) {
				baos.write(bytes, 0, len);
			}
			baos.flush();
			bt = baos.toByteArray();
			baos.close();
			bis.close();
		} catch (Throwable e) {
			logger.error("getFileBytes异常", e);
		} finally {
			try {
				if (baos != null)
					baos.close();
				if (bis != null)
					bis.close();
			} catch (Throwable e) {
				logger.error("getFileBytes异常", e);
			}
		}
		return bt;
	}
}
