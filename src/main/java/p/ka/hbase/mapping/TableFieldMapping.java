package p.ka.hbase.mapping;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 功能描述: HBase 表和数据类型 字段的映射
 * @createTime: 2016年3月14日 下午6:07:23
 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
 * @version: 0.1
 * @lastVersion: 0.1
 * @updateTime: 2016年3月14日 下午6:07:23
 * @updateAuthor: <a href="mailto:676096658@qq.com">xiaochangyu</a>
 * @changesSum:
 */
public final class TableFieldMapping {

	/**
	 * 功能描述: 测试样例
	 * @createTime: 2016年8月26日 下午5:20:39
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param args
	 * @throws Throwable
	 */
	public static void main(String[] args) throws Throwable {
		FieldTypeTable normal = new Normal("test_normal_table");
		System.out.println(normal.getTableName());
		System.out.println(new String(normal.getQualifier(771)));
		System.out.println(new String(normal.getQualifierMark(771)));
		System.out.println(new String(normal.getQualifier(-45057)));
		System.out.println(new String(normal.getQualifierMark(-45057)));
		System.out.println(normal.parseType(normal.getQualifier(-55)));
		System.out.println(normal.parseType(normal.getQualifierMark(-55)));
		FieldTypeTable oneType = new OneTypeTable("test_one_type_table");
		System.out.println(oneType.getTableName());
		System.out.println(new String(oneType.getQualifier(771)));
		System.out.println(new String(oneType.getQualifierMark(771)));
		System.out.println(new String(oneType.getQualifier(-45057)));
		System.out.println(new String(oneType.getQualifierMark(-45057)));
		System.out.println(oneType.parseType(oneType.getQualifier(-55)));
		System.out.println(oneType.parseType(oneType.getQualifierMark(-55)));
	}

	protected final static byte BYTE_A = 'A';
	protected final static byte BYTE_B = 'B';
	protected final static byte BYTE_C = 'C';
	protected final static byte BYTE_D = 'D';
	protected final static byte BYTE_E = 'E';
	protected final static byte BYTE_F = 'F';
	protected final static byte BYTE_G = 'G';
	protected final static byte BYTE_H = 'H';
	protected final static byte BYTE_I = 'I';
	protected final static byte BYTE_J = 'J';
	protected final static byte BYTE_K = 'K';
	protected final static byte BYTE_L = 'L';
	protected final static byte BYTE_M = 'M';
	protected final static byte BYTE_N = 'N';
	protected final static byte BYTE_O = 'O';
	protected final static byte BYTE_P = 'P';
	protected final static byte BYTE_Q = 'Q';
	protected final static byte BYTE_R = 'R';
	protected final static byte BYTE_S = 'S';
	protected final static byte BYTE_T = 'T';
	protected final static byte BYTE_U = 'U';
	protected final static byte BYTE_V = 'V';
	protected final static byte BYTE_W = 'W';
	protected final static byte BYTE_X = 'X';
	protected final static byte BYTE_Y = 'Y';
	protected final static byte BYTE_Z = 'Z';

	protected final static byte BYTE_a = BYTE_A + 32;
	protected final static byte BYTE_b = BYTE_B + 32;
	protected final static byte BYTE_c = BYTE_C + 32;
	protected final static byte BYTE_d = BYTE_D + 32;
	protected final static byte BYTE_e = BYTE_E + 32;
	protected final static byte BYTE_f = BYTE_F + 32;
	protected final static byte BYTE_g = BYTE_G + 32;
	protected final static byte BYTE_h = BYTE_H + 32;
	protected final static byte BYTE_i = BYTE_I + 32;
	protected final static byte BYTE_j = BYTE_J + 32;
	protected final static byte BYTE_k = BYTE_K + 32;
	protected final static byte BYTE_l = BYTE_L + 32;
	protected final static byte BYTE_m = BYTE_M + 32;
	protected final static byte BYTE_n = BYTE_N + 32;
	protected final static byte BYTE_o = BYTE_O + 32;
	protected final static byte BYTE_p = BYTE_P + 32;
	protected final static byte BYTE_q = BYTE_Q + 32;
	protected final static byte BYTE_r = BYTE_R + 32;
	protected final static byte BYTE_s = BYTE_S + 32;
	protected final static byte BYTE_t = BYTE_T + 32;
	protected final static byte BYTE_u = BYTE_U + 32;
	protected final static byte BYTE_v = BYTE_V + 32;
	protected final static byte BYTE_w = BYTE_W + 32;
	protected final static byte BYTE_x = BYTE_X + 32;
	protected final static byte BYTE_y = BYTE_Y + 32;
	protected final static byte BYTE_z = BYTE_Z + 32;

	protected final static byte BYTE_0 = '0';
	protected final static byte BYTE_1 = '1';
	protected final static byte BYTE_2 = '2';
	protected final static byte BYTE_3 = '3';
	protected final static byte BYTE_4 = '4';
	protected final static byte BYTE_5 = '5';
	protected final static byte BYTE_6 = '6';
	protected final static byte BYTE_7 = '7';
	protected final static byte BYTE_8 = '8';
	protected final static byte BYTE_9 = '9';



	/** 表名的索引标记 */
	public final static String Index_Tag = "_idx_";


	/** 所有表的第一个 family */
	public final static byte[] FAMILY00 = new byte[]{BYTE_0};


	/** "M" */
	public final static String QUALIFIER_MARK_PREFIX = "M";
	/** "-" */
	public final static String QUALIFIER_NEGATIVE_TAG = "-";
	/** 'M' */
	public final static byte QUALIFIER_MARK_PREFIX_Byte = 'M';
	/** '-' */
	public final static byte QUALIFIER_NEGATIVE_TAG_Byte = '-';

	/** default type: 0 */
	public final static int QUALIFIER_DEFAULT_TYPE = 0;

	/** qualifier default */
	private static byte[] QUALIFIER_DEFAULT = null;

	/** qualifier mark default */
	private static byte[] QUALIFIER_MARK_DEFAULT = null;

	/**
	 * 功能描述: 获取默认的列标识符, 根据 {@link #QUALIFIER_DEFAULT_TYPE} 而定
	 * @createTime: 2016年11月21日 上午9:50:35
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @return {@link byte[]}
	 */
	public final static byte[] getDefaultQualifier() {
		if (QUALIFIER_DEFAULT == null) {
			synchronized (FAMILY00) {
				if (QUALIFIER_DEFAULT == null) {
					FieldTypeTable fieldTypeTable = new FieldTypeTable(null) {};
					QUALIFIER_DEFAULT = fieldTypeTable.getQualifier(QUALIFIER_DEFAULT_TYPE);
					if (QUALIFIER_MARK_DEFAULT == null)
						QUALIFIER_MARK_DEFAULT = fieldTypeTable.getQualifierMark(QUALIFIER_DEFAULT_TYPE);
				}
			}
		}
		return QUALIFIER_DEFAULT;
	}

	/**
	 * 功能描述: 获取默认的 Mark 列的列标识符, 根据 {@link #QUALIFIER_DEFAULT_TYPE} 而定
	 * @createTime: 2016年11月21日 上午9:50:49
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @return {@link byte[]}
	 */
	public final static byte[] getDefaultQualifierMark() {
		if (QUALIFIER_MARK_DEFAULT == null) {
			synchronized (FAMILY00) {
				if (QUALIFIER_MARK_DEFAULT == null) {
					FieldTypeTable fieldTypeTable = new FieldTypeTable(null) {};
					QUALIFIER_MARK_DEFAULT = fieldTypeTable.getQualifierMark(QUALIFIER_DEFAULT_TYPE);
					if (QUALIFIER_DEFAULT == null)
						QUALIFIER_DEFAULT = fieldTypeTable.getQualifier(QUALIFIER_DEFAULT_TYPE);
				}
			}
		}
		return QUALIFIER_MARK_DEFAULT;
	}

	/** 索引 列名 [{@link #BYTE_9}] */
	public final static byte[] QUALIFIER_INDEX = new byte[]{BYTE_9};








	/** 空值 */
	public final static byte[] EMPTY_VALUE = new byte[]{};

	/** 字节标记是已删除 */
	public final static byte MARK_DELETED = 0x01;
	/** 字节标记是已隐藏 */
	public final static byte MARK_HIDDEN = 0x02;
	/** 字节标记是已删除的正则字符串 */
	public final static String MARK_DELETED_REGEX_STRING = "\\x01|\\x03";
	/** 字节标记是已隐藏的正则字符串 */
	public final static String MARK_HIDDEN_REGEX_STRING = "\\x02|\\x03";


	/**
	 * 功能描述: 组合标记
	 * @createTime: 2016年3月16日 下午3:06:33
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param bs
	 * @return byte[]
	 */
	public static byte[] groupMark(byte... bs) {
		if (bs == null || bs.length == 0) return EMPTY_VALUE;
		byte b = 0;
		for (int i = 0; i < bs.length; i++) {
			if (bs[i] == MARK_DELETED) b += MARK_DELETED;
			if (bs[i] == MARK_HIDDEN) b += MARK_HIDDEN;
		}
		return new byte[]{b};
	}

	/**
	 * 功能描述: 是否已删除
	 * @createTime: 2016年3月16日 下午3:14:27
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param markValueBytes
	 * @return boolean
	 */
	public static boolean isDeleted(byte[] markValueBytes) {
		if (markValueBytes == null || markValueBytes.length == 0) return false;
		if ((markValueBytes[0] | 0xFE) == 0xFF) return true;
		else return false;
	}

	/**
	 * 功能描述: 是否已隐藏
	 * @createTime: 2016年3月16日 下午3:15:21
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param markValueBytes
	 * @return boolean
	 */
	public static boolean isHidden(byte[] markValueBytes) {
		if (markValueBytes == null || markValueBytes.length == 0) return false;
		if ((markValueBytes[0] | 0xFD) == 0xFF) return true;
		else return false;
	}

	/**
	 * 功能描述: 判断两个 qualifier 是否相等, 基于字节数组比较
	 * @createTime: 2016年10月28日 下午3:42:21
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param left
	 * @param right
	 * @return {@link boolean}
	 */
	public static boolean isEquals(byte[] left, byte[] right) {
		return Arrays.equals(left, right);
	}








	/**
	 * 功能描述: 抽象 Field Type Table 类
	 * @createTime: 2016年3月16日 下午10:26:02
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @version: 0.1
	 * @lastVersion: 0.1
	 * @updateTime: 2016年3月16日 下午10:26:02
	 * @updateAuthor: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @changesSum:
	 */
	public static abstract class FieldTypeTable {

		private final String TABLE_NAME;

		private final ConcurrentHashMap<Integer, byte[]> TYPE_MAP = new ConcurrentHashMap<>();
		private final ConcurrentHashMap<Integer, byte[]> TYPE_MARK_MAP = new ConcurrentHashMap<>();

		private FieldTypeTable(String table_name) {
			this.TABLE_NAME = table_name;
		}

		/**
		 * 功能描述: 获取表名
		 * @createTime: 2016年3月19日 上午11:10:10
		 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
		 * @return String
		 */
		public String getTableName() {
			return TABLE_NAME;
		}

		/**
		 * 功能描述: 获取 qualifier
		 * @createTime: 2016年3月14日 下午6:15:25
		 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
		 * @param type
		 * @return byte[]
		 */
		public byte[] getQualifier(int type) {
			byte[] bytes = TYPE_MAP.get(type);
			if (bytes == null) {
				bytes = (type >= 0 ? Integer.toHexString(type) : (QUALIFIER_NEGATIVE_TAG + Integer.toHexString(-type))).getBytes();
				TYPE_MAP.put(type, bytes);
			}
			return bytes;
		}

		/**
		 * 功能描述: 获取 Mark qualifier
		 * @createTime: 2016年3月14日 下午6:23:37
		 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
		 * @param type
		 * @return byte[]
		 */
		public byte[] getQualifierMark(int type) {
			byte[] bytes = TYPE_MARK_MAP.get(type);
			if (bytes == null) {
				bytes = (QUALIFIER_MARK_PREFIX + (type >= 0 ? Integer.toHexString(type) : (QUALIFIER_NEGATIVE_TAG + Integer.toHexString(-type)))).getBytes();
				TYPE_MARK_MAP.put(type, bytes);
			}
			return bytes;
		}

		/**
		 * 功能描述: 根据 qualifier 解析出相应的 type 类型, 解析错误时会抛出异常
		 * @createTime: 2016年3月14日 下午6:09:13
		 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
		 * @param qualifier
		 * @return int
		 */
		public int parseType(byte[] qualifier) {
			for (Entry<Integer, byte[]> entry: TYPE_MAP.entrySet()) {
				if (isEquals(qualifier, entry.getValue())) return entry.getKey();
			}
			for (Entry<Integer, byte[]> entry: TYPE_MARK_MAP.entrySet()) {
				if (isEquals(qualifier, entry.getValue())) return entry.getKey();
			}
			boolean isMark = qualifier[0] == QUALIFIER_MARK_PREFIX_Byte;
			boolean isNegative = qualifier[isMark ? 1 : 0] == QUALIFIER_NEGATIVE_TAG_Byte;
			int numStart = (isMark ? 1 : 0) + (isNegative ? 1 : 0);
			int type = Integer.parseInt(new String(qualifier, numStart, qualifier.length - numStart), 16);
			return isNegative ? -type : type;
		}

		/**
		 * 功能描述: 判断是否是 type 列的 qualifier, 可能同时也不是 {@link #isTypeMark(byte[])}, 所以只对 true 做保证
		 * @createTime: 2016年8月26日 下午6:25:03
		 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
		 * @param qualifier
		 * @return boolean
		 */
		public boolean isType(byte[] qualifier) {
			try {
				parseType(qualifier);
			} catch (Throwable e) {
				return false;
			}
			return qualifier[0] != QUALIFIER_MARK_PREFIX_Byte;
		}

		/**
		 * 功能描述: 判断是否是 type 的 mark 列的 qualifier, 可能同时也不是 {@link #isType(byte[])}, 所以只对 true 做保证
		 * @createTime: 2016年8月26日 下午6:25:18
		 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
		 * @param qualifierMark
		 * @return boolean
		 */
		public boolean isTypeMark(byte[] qualifierMark) {
			try {
				parseType(qualifierMark);
			} catch (Throwable e) {
				return false;
			}
			return qualifierMark[0] == QUALIFIER_MARK_PREFIX_Byte;
		}
	}


	/**
	 * 功能描述: {@link TableFieldMapping.Normal} 数据结构
	 * @createTime: 2016年3月14日 下午6:35:46
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @version: 0.1
	 * @lastVersion: 0.1
	 * @updateTime: 2016年3月14日 下午6:35:46
	 * @updateAuthor: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @changesSum:
	 */
	public static class Normal extends FieldTypeTable {

		public Normal(String tableName) {
			super(tableName);
		}
	}




	/**
	 * 功能描述: {@link TableFieldMapping.OneTypeTable} 数据结构
	 * @createTime: 2016年3月14日 下午6:35:46
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @version: 0.1
	 * @lastVersion: 0.1
	 * @updateTime: 2016年3月14日 下午6:35:46
	 * @updateAuthor: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @changesSum:
	 */
	public static class OneTypeTable extends FieldTypeTable {

		public OneTypeTable(String tableName) {
			super(tableName);
		}

		@Override
		public byte[] getQualifier(int type) {
			return getDefaultQualifier();
		}

		@Override
		public byte[] getQualifierMark(int type) {
			return getDefaultQualifierMark();
		}
	}

}