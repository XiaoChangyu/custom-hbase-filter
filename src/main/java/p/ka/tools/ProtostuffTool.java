package p.ka.tools;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * 功能描述: Protostuff 序列化工具
 * @createTime: 2016年11月23日 下午2:41:14
 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
 * @version: 0.1
 * @lastVersion: 0.1
 * @updateTime: 2016年11月23日 下午2:41:14
 * @updateAuthor: <a href="mailto:676096658@qq.com">xiaochangyu</a>
 * @changesSum:
 */
public class ProtostuffTool {

	private static Map<Class<?>, Schema<?>> cacheSchema = new ConcurrentHashMap<Class<?>, Schema<?>>();

	/**
	 * 功能描述: 获取类的 {@link Schema<T>}
	 * @createTime: 2016年11月23日 下午2:40:51
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param clazz
	 * @return {@link Schema<T>}
	 */
	private static <T> Schema<T> getSchema(Class<T> clazz) {
		@SuppressWarnings("unchecked")
		Schema<T> schema = (Schema<T>) cacheSchema.get(clazz);
		if (schema == null) {
			schema = RuntimeSchema.getSchema(clazz);
			if (schema != null) {
				cacheSchema.put(clazz, schema);
			}
		}
		return schema;
	}

	/**
	 * 功能描述: 序列化
	 * @createTime: 2016年11月23日 下午2:38:41
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param obj
	 * @return {@link byte[]}
	 */
	public static <T> byte[] serializer(T obj) {
		@SuppressWarnings("unchecked")
		Class<T> clazz = (Class<T>) obj.getClass();
		LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
		try {
			Schema<T> schema = getSchema(clazz);
			return ProtostuffIOUtil.toByteArray(obj, schema, buffer);
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		} finally {
			buffer.clear();
		}
	}

	/**
	 * 功能描述: 反序列化
	 * @createTime: 2016年11月23日 下午2:40:39
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param data
	 * @param clazz
	 * @return {@link T}
	 */
	public static <T> T deserializer(byte[] data, Class<T> clazz) {
		try {
			T obj = clazz.newInstance();
			Schema<T> schema = getSchema(clazz);
			ProtostuffIOUtil.mergeFrom(data, obj, schema);
			return obj;
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

}