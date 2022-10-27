package com.eurlanda.datashire.engine.util;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FileUtil {
	private FileUtil() {
	};
	
	/***
	 * 属性文件对象 因为有一些值是要经常改的.所以要配置在配置文件
	 */
	private static Properties props = null;

	/**
	 * 将文本文件文件打碎成行
	 * @author akache
	 * @param file
	 * @return
	 */
	public static String[] smashRead(File file){
		try {
			BufferedReader bw = new BufferedReader(new FileReader(file));
			List<String> list = new ArrayList<String>();
			String[] nums = null;
			String line = bw.readLine();
			while (line != null) {
				list.add(line);
				line = bw.readLine();
			}
			nums=new String[list.size()];
			for (int i =0;i<list.size();i++) {
				nums[i]=list.get(i);
			}
			return nums;
		} catch (FileNotFoundException e2) {
			e2.printStackTrace();
			// TODO: handle exception
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * 将文本文件文件打碎成行
	 * @author akache
	 * @param fileDir
	 * @return
	 */
	public static String[] smashRead(String fileDir){
		try {
			InputStream in = FileUtil.class.getResourceAsStream(fileDir);
			InputStreamReader inr= new InputStreamReader(in,"UTF-8");
			BufferedReader bin= new BufferedReader(inr);
			List<String> list = new ArrayList<String>();
			String[] nums = null;
			String line = bin.readLine();
			while (line != null) {
				list.add(line);
				line = bin.readLine();
			}
			nums=new String[list.size()];
			for (int i =0;i<list.size();i++) {
				nums[i]=list.get(i);
			}
			return nums;
		} catch (FileNotFoundException e2) {
			e2.printStackTrace();
			// TODO: handle exception
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	
	/**
	 * @author akache
	 * 获得目录下所有文件名
	 * @param dir
	 * @return
	 */
	public static String[] getDirFileName(String dir){
		File filePath = new File(dir);
		if(filePath.isDirectory()){
			return filePath.list();
		}
		return null;
	}
	
	/**
	 * 保存文件
	 * 
	 * @param file
	 *            文件对象
	 * @param filename
	 *            文件名
	 */
	public static String saveFile(File file, String savepath, String filename) {
		FileOutputStream fos = null;
		FileInputStream fis = null;
		try {
			createDir(savepath);
			rebirthFile(savepath + "/" + filename);
			// 以服务器的文件保存路路径和随机数+原文件名构建文件输出流
			fos = new FileOutputStream(savepath + "/" + filename);
			// 以上传文件构建一个上传流
			fis = new FileInputStream(file);

			// 如果大于2M则提示错误信息
			/*
			 * if(fis.available() > 2048500) { return "errorfilesize"; }
			 */
			// 将上传文件的内容写入服务器
			byte[] buffer = new byte[2048500];
			int len = 0;
			while ((len = fis.read(buffer)) > 0) {
				fos.write(buffer, 0, len);
			}
			return "success";
		} catch (Exception e) {
			try {
				fis.close();
				fos.close();
			} catch (IOException e1) {
			}
			return "erroruploadfailed";
		} finally {
			try {
				fis.close();
				fos.close();
			} catch (IOException e1) {
			}
		}
	}
	/**
	 * 保存文件流
	 * @author akache
	 * @param fis 文件流
	 * @param savepath 保存路径
	 * @param filename 文件名
	 * @return
	 */
	public static String saveFis(InputStream fis, String savepath,
			String filename) {
		FileOutputStream fos = null;
		try {
			createDir(savepath);
			rebirthFile(savepath + "/" + filename);
			// 以服务器的文件保存路路径和随机数+原文件名构建文件输出流
			fos = new FileOutputStream(savepath + "/" + filename);
			// 以上传文件构建一个上传流

			// 如果大于2M则提示错误信息
			/*
			 * if(fis.available() > 2048500) { return "errorfilesize"; }
			 */
			// 将上传文件的内容写入服务器
			byte[] buffer = new byte[2048500];
			int len = 0;
			while ((len = fis.read(buffer)) > 0) {
				fos.write(buffer, 0, len);
			}
			return "success";
		} catch (Exception e) {
			try {
				fis.close();
				fos.close();
			} catch (IOException e1) {
			}
			return "erroruploadfailed";
		} finally {
			try {
				fis.close();
				fos.close();
			} catch (IOException e1) {
			}
		}
	}

	/**
	 * 保存多个文件
	 * 
	 * @param file
	 *            文件对象
	 * @param filename
	 *            文件名
	 */
	public static String saveFiles(List<File> file, String savepath,
			List<String> filename) {
		FileOutputStream fos = null;
		FileInputStream fis = null;
		try {
			createDir(savepath);
			for (int i = 0; i < file.size(); i++) {
				// 如果文件存在将其删除
				rebirthFile(savepath + "/" + filename.get(i));
				// 以服务器的文件保存路路径和随机数+原文件名构建文件输出流
				fos = new FileOutputStream(savepath + "/" + filename.get(i));
				// 以上传文件构建一个上传流
				fis = new FileInputStream(file.get(i));

				// 如果大于2M则提示错误信息
				/*
				if (fis.available() > 2048500) {
					return "errorfilesize";
				}
				*/
				// 将上传文件的内容写入服务器
				byte[] buffer = new byte[2048500];
				int len = 0;
				while ((len = fis.read(buffer)) > 0) {
					fos.write(buffer, 0, len);
				}
			}
			return "success";
		} catch (Exception e) {
			try {
				fis.close();
				fos.close();
			} catch (IOException e1) {
			}
			return "erroruploadfailed";
		} finally {
			try {
				fis.close();
				fos.close();
			} catch (IOException e1) {
			}
		}
	}

	/**
	 * 保存多个文件流
	 * 
	 * @param fiss
	 *            文件对象
	 * @param filename
	 *            文件名
	 */
	public static String saveFiss(List<InputStream> fiss, String savepath,
			List<String> filename) {
		FileOutputStream fos = null;
		InputStream fis = null;
		try {
			createDir(savepath);
			for (int i = 0; i < fiss.size(); i++) {

				rebirthFile(savepath + "/" + filename.get(i));
				// 以服务器的文件保存路路径和随机数+原文件名构建文件输出流
				fos = new FileOutputStream(savepath + "/" + filename.get(i));
				// 以上传文件构建一个上传流
				fis = fiss.get(i);

				// 如果大于2M则提示错误信息
				/**\
				if (fis.available() > 2048500) {
					return "errorfilesize";
				}*/
				// 将上传文件的内容写入服务器
				byte[] buffer = new byte[2048500];
				int len = 0;
				while ((len = fis.read(buffer)) > 0) {
					fos.write(buffer, 0, len);
				}
			}
			return "success";
		} catch (Exception e) {
			try {
				fis.close();
				fos.close();
			} catch (IOException e1) {
			}
			return "erroruploadfailed";
		} finally {
			try {
				fis.close();
				fos.close();
			} catch (IOException e1) {
			}
		}
	}

	/**
	 * 如果文件存在就删除文件 2012-3-13
	 * 
	 * @author akache
	 * @param destDirName
	 * @return
	 */
	public static boolean rebirthFile(String destDirName) {
		File dir = new File(destDirName);
		if (dir.exists()) {
			FileUtil.deleteFile(destDirName);
			return true;
		}
		return false;
	}

	/**
	 * 如果目录不存在就创建目录 2012-3-13
	 * 
	 * @author akache
	 * @param destDirName
	 * @return
	 */
	public static boolean createDir(String destDirName) {
		File dir = new File(destDirName);
		if (dir.exists()) {
			return false;
		}
		if (!destDirName.endsWith(File.separator))
			destDirName = destDirName + File.separator;
		// 创建单个目录
		if (dir.mkdirs()) {
			System.out.println("create path" + destDirName + "succeed");
			return true;
		} else {
			System.out.println("create path" + destDirName + "succeed");
			return false;
		}
	}

	/**
	 * 删除文件
	 * 
	 * @param filePath
	 *            文件名
	 */
	public static void deleteFile(String filePath) {
		try {
			File f = new File(filePath);
			f.delete();
		} catch (Exception e) {
		}
	}

	/**
	 * @author akache
	 * @date/time Apr 23, 2009 5:17:09 PM
	 * @description 加载属性文件
	 */
	private synchronized static void loadProperties() {
		props = new Properties();
		try {
			InputStream input = FileUtil.class
					.getResourceAsStream("/system.properties");
			props.load(input);
		} catch (Exception e) {
			Logger.getLogger(FileUtil.class).error("加载message.properties文件失败!");
		}
	}
	/**
	 * @author akache
	 * @date/time Apr 23, 2009 5:17:09 PM
	 * @description 加载属性文件
	 * @param configName 读取的配置文件
	 */
	private synchronized static void loadProperties(String configName) {
		props = new Properties();
		try {
			InputStream input = FileUtil.class
					.getResourceAsStream(configName);
			props.load(input);
		} catch (Exception e) {
			Logger.getLogger(FileUtil.class).error("加载message.properties文件失败!");
		}
	}

	/**
	 * @author akache
	 * @date/time Apr 23, 2009 5:26:22 PM
	 * @description 没有相关说明
	 * @param propName
	 *            属性文件的KEY
	 * @return 属性文件KEY对应的VALUE
	 */
	public static String getProperty(String propName) {
		try {
			loadProperties();
			return props.getProperty(propName);
		} catch (Exception e) {
			Logger.getLogger(FileUtil.class).error("获取propName对应的值失败!");
			e.printStackTrace();
			return null;
		}
	}
	/**
	 * @author akache
	 * @date/time Apr 23, 2009 5:26:22 PM
	 * @description 没有相关说明
	 * @param propName
	 *            属性文件的KEY
	 * @param configName 读取的配置文件
	 * @return 属性文件KEY对应的VALUE
	 */
	public static String getProperty(String propName,String configName) {
		try {
			loadProperties(configName);
			return props.getProperty(propName);
		} catch (Exception e) {
			Logger.getLogger(FileUtil.class).error("获取propName对应的值失败!");
			e.printStackTrace();
			return null;
		}
	}

	/****** smw ******/
	public static File getFile(String dir) {
		File f = new File(dir);
		try {
			f.mkdirs();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return f;
	}
	/**
	 * 是否存在此文件
	 * @param path
	 * @return true 存在 false 不存在
	 */
	public static Boolean isFile(String path) {
		File f = new File(path);
		if (f.exists()){
			return true;
		}
		return false;
	}
}
