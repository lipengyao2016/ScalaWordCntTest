package com.bigdata.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * 
 * @ClassName: SerializableUtil 
 * @Description: 对象序列化/反序列化工具
 * @author tianpengw 
 * @date 2017年4月26日 上午10:11:07 
 *
 */
public class SerializableUtil {
	private static Logger logger = LogManager.getLogger(SerializableUtil.class);
	/**
	 * 
	 * @Description: 序列化
	 * @author tianpengw 
	 * @return byte[]
	 */
	public static byte[] serialize(Object obj) {
		ObjectOutputStream objout = null;
		ByteArrayOutputStream basout = null;
		try {
			basout = new ByteArrayOutputStream();
			objout = new ObjectOutputStream(basout);
			objout.writeObject(obj);
			byte[] bt = basout.toByteArray();
			return bt;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e);
		}
		return null;
	}

	/**
	 * 
	 * @Description: 反序列化
	 * @author tianpengw 
	 * @return Object
	 */
	public static Object unSerialize(byte[] bt) {
		ObjectInputStream objinput = null;
		ByteArrayInputStream basinput = null;
		try {
			basinput = new ByteArrayInputStream(bt);
			objinput = new ObjectInputStream(basinput);
			return objinput.readObject();
		} catch (Exception e) {
			e.printStackTrace();
			
		}
		return null;
	}
}
