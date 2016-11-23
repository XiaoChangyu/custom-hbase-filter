package p.ka.test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;

import p.ka.hbase.filter.QualifierMarkValueFilter;
import p.ka.hbase.filter.QualifierNoMarkFilter;
import p.ka.hbase.mapping.TableFieldMapping;
import p.ka.hbase.mapping.TableFieldMapping.Normal;

/**
 * 功能描述: 自定义 Filter 测试
 * @createTime: 2016年7月21日 下午3:06:30
 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
 * @version: 0.1
 * @lastVersion: 0.1
 * @updateTime: 2016年7月21日 下午3:06:30
 * @updateAuthor: <a href="mailto:676096658@qq.com">xiaochangyu</a>
 * @changesSum:
 */
public class CustomFilterTestMain {

	public static void main(String[] args) throws Throwable {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "hadoopmy");
		final Connection connection = ConnectionFactory.createConnection(conf);
		String tableName = "test_filter";
		boolean init = false;
		boolean scanAll = true;
		if (init) {
			createTestTable(connection, tableName);
			putTestData(connection, tableName);
		}

		scan(connection, tableName, scanAll);
		scanByQualifierFilter(connection, tableName, scanAll);
		/* 以下三个使用的是自定义 Filter, 需要在 HBase RegionServer 的 lib/ 目录放入自定义 Filter 相关依赖 */
		scanByQualifierNoMarkFilter(connection, tableName, scanAll);
		scanByQualifierMarkValueFilter(connection, tableName, scanAll);
		scanByQualifierMarkValueFilter(connection, tableName, scanAll, CompareOp.NOT_EQUAL, new RegexStringComparator("m"));

		System.out.println("Done!");
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					connection.close();
					System.out.println("Connection Closed");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
	}

	public final static byte[] FAMILY0 = "0".getBytes();
	public final static byte[] Mark_m = "m".getBytes();
	public final static byte[] Mark_d = "d".getBytes();
	public final static Normal Normal = new TableFieldMapping.Normal(null);

	/**
	 * 功能描述: 使用 {@link QualifierMarkValueFilter} 进行扫描, 同时指定 Mark 值过滤条件
	 * @createTime: 2016年11月23日 下午3:35:39
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param connection
	 * @param tableName
	 * @param scanAll
	 * @param valueCompareOp
	 * @param valueComparator
	 * @throws Throwable
	 */
	public static void scanByQualifierMarkValueFilter(Connection connection, String tableName, boolean scanAll, CompareOp valueCompareOp, ByteArrayComparable valueComparator) throws Throwable {
		System.out.println("scanByQualifierMarkValueFilter2 ====> start");
		Scan scan = new Scan();
		if (!scanAll) {
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(-21891));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(-21891));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(3));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(3));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(-51));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(-51));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(18));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(18));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(-40));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(-40));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(67));
		}
		scan.setFilter(new QualifierMarkValueFilter(valueCompareOp, valueComparator));
		_printScanResult(connection, tableName, scan);
		System.out.println("scanByQualifierMarkValueFilter2 ====> stop");
	}

	/**
	 * 功能描述: 使用 {@link QualifierMarkValueFilter} 进行扫描
	 * @createTime: 2016年11月23日 下午3:29:24
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param connection
	 * @param tableName
	 * @param scanAll
	 * @throws Throwable
	 */
	public static void scanByQualifierMarkValueFilter(Connection connection, String tableName, boolean scanAll) throws Throwable {
		System.out.println("scanByQualifierMarkValueFilter ====> start");
		Scan scan = new Scan();
		if (!scanAll) {
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(-21891));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(-21891));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(3));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(3));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(-51));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(-51));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(18));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(18));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(-40));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(-40));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(67));
		}
		scan.setFilter(new QualifierMarkValueFilter());
		_printScanResult(connection, tableName, scan);
		System.out.println("scanByQualifierMarkValueFilter ====> stop");
	}

	/**
	 * 功能描述: 使用 {@link QualifierNoMarkFilter} 进行扫描
	 * @createTime: 2016年11月23日 下午3:29:24
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param connection
	 * @param tableName
	 * @param scanAll
	 * @throws Throwable
	 */
	public static void scanByQualifierNoMarkFilter(Connection connection, String tableName, boolean scanAll) throws Throwable {
		System.out.println("scanByQualifierNoMarkFilter ====> start");
		Scan scan = new Scan();
		if (!scanAll) {
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(-21891));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(-21891));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(3));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(3));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(-51));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(-51));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(18));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(18));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(-40));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(-40));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(67));
		}
		scan.setFilter(new QualifierNoMarkFilter());
		_printScanResult(connection, tableName, scan);
		System.out.println("scanByQualifierNoMarkFilter ====> stop");
	}

	/**
	 * 功能描述: 使用 {@link QualifierFilter} 进行扫描
	 * @createTime: 2016年11月23日 下午3:29:24
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param connection
	 * @param tableName
	 * @param scanAll
	 * @throws Throwable
	 */
	public static void scanByQualifierFilter(Connection connection, String tableName, boolean scanAll) throws Throwable {
		System.out.println("scanByQualifierFilter ====> start");
		Scan scan = new Scan();
		if (!scanAll) {
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(-21891));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(-21891));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(3));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(3));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(-51));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(-51));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(18));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(18));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(-40));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(-40));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(67));
		}
		scan.setFilter(new QualifierFilter(CompareOp.NOT_EQUAL, new RegexStringComparator("^M")));
		_printScanResult(connection, tableName, scan);
		System.out.println("scanByQualifierFilter ====> stop");
	}

	/**
	 * 功能描述: 普通 Scan 操作.
	 * @createTime: 2016年11月23日 下午3:40:10
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param connection
	 * @param tableName
	 * @param scanAll
	 * @throws Throwable
	 */
	public static void scan(Connection connection, String tableName, boolean scanAll) throws Throwable {
		System.out.println("scan ====> start");
		Scan scan = new Scan();
		if (!scanAll) {
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(-21891));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(-21891));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(3));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(3));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(-51));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(-51));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(18));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(18));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifier(-40));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(-40));
			scan.addColumn(TableFieldMapping.FAMILY00, Normal.getQualifierMark(67));
		}
		_printScanResult(connection, tableName, scan);
		System.out.println("scan ====> stop");
	}

	/**
	 * 功能描述: Scan 并简单打印 Result 信息
	 * @createTime: 2016年11月23日 下午3:31:20
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param connection
	 * @param tableName
	 * @param scan
	 * @throws Throwable
	 */
	public static void _printScanResult(Connection connection, String tableName, Scan scan) throws Throwable {
		Table table = connection.getTable(TableName.valueOf(tableName));
		ResultScanner results = table.getScanner(scan);
		Result result = null;
		while ((result = results.next()) != null) {
			List<Cell> listCells = result.listCells();
			for (Cell cell: listCells) {
				String rowkey = new String(CellUtil.cloneRow(cell));
				String family = new String(CellUtil.cloneFamily(cell));
				String qualifier = new String(CellUtil.cloneQualifier(cell));
				String value = new String(CellUtil.cloneValue(cell));
				System.out.printf("%s\t%s:%-10s %s\n", rowkey, family, qualifier, value);
			}
			System.out.println();
		}
		table.close();
	}

	/**
	 * 功能描述: 写入测试数据
	 * @createTime: 2016年11月23日 下午3:18:12
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param connection
	 * @param tableName
	 * @throws Throwable
	 */
	public static void putTestData(Connection connection, String tableName) throws Throwable {
		System.out.println("putTestData ====> start");
		Table table = connection.getTable(TableName.valueOf(tableName));
		List<Put> puts = new ArrayList<>();
		puts.add(new Put("A01".getBytes()).addColumn(FAMILY0, "-33".getBytes(), "a33".getBytes()));
		puts.add(new Put("A01".getBytes()).addColumn(FAMILY0, "12".getBytes(), "a12".getBytes()));
		puts.add(new Put("A01".getBytes()).addColumn(FAMILY0, "M-33".getBytes(), Mark_m));
		puts.add(new Put("A01".getBytes()).addColumn(FAMILY0, "M12".getBytes(), Mark_m));
		puts.add(new Put("B01".getBytes()).addColumn(FAMILY0, "-5583".getBytes(), "b55".getBytes()));
		puts.add(new Put("B01".getBytes()).addColumn(FAMILY0, "-3".getBytes(), "b3".getBytes()));
		puts.add(new Put("B01".getBytes()).addColumn(FAMILY0, "M-5583".getBytes(), Mark_m));
		puts.add(new Put("C01".getBytes()).addColumn(FAMILY0, "-810".getBytes(), "c81".getBytes()));
		puts.add(new Put("C01".getBytes()).addColumn(FAMILY0, "998".getBytes(), "c98".getBytes()));
		puts.add(new Put("D01".getBytes()).addColumn(FAMILY0, "-28".getBytes(), "d28".getBytes()));
		puts.add(new Put("D01".getBytes()).addColumn(FAMILY0, "43".getBytes(), "d43".getBytes()));
		puts.add(new Put("D01".getBytes()).addColumn(FAMILY0, "M43".getBytes(), Mark_d));
		table.put(puts);
		table.close();
		System.out.println("putTestData ====> stop");
	}

	/**
	 * 功能描述: 创建测试表
	 * @createTime: 2016年11月23日 下午3:17:36
	 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
	 * @param connection
	 * @param tableName
	 * @throws Throwable
	 */
	public static void createTestTable(Connection connection, String tableName) throws Throwable {
		System.out.println("createTestTable ====> start");
		HBaseAdmin hbaseAdmin = null;
		try {
			hbaseAdmin = (HBaseAdmin) connection.getAdmin();
			TableName _tableName = TableName.valueOf(tableName);

			HTableDescriptor desc = new HTableDescriptor(_tableName);

			HColumnDescriptor columnDescriptor = new HColumnDescriptor("0");
			desc.addFamily(columnDescriptor);

			System.out.println(tableName + " 开始创建");
			hbaseAdmin.createTable(desc);

			Thread.sleep(1000);

			while (hbaseAdmin.isTableDisabled(_tableName)) {
				hbaseAdmin.enableTable(_tableName);
				System.out.println("enable " + tableName + " ...");
				Thread.sleep(1000);
			}
			System.out.println(tableName + " 创建完成");
		} finally {
			if (hbaseAdmin != null) hbaseAdmin.close();
		}
		System.out.println("createTestTable ====> stop");
	}
}
