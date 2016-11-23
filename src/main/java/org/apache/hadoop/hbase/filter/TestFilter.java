package org.apache.hadoop.hbase.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**

• {@link #filterRowKey(byte[], int, int)}: true means drop this row; false means include.
• {@link #filterAllRemaining()}: true means row scan is over; false means keep going. 
• {@link #filterKeyValue(Cell)}: decides whether to include or exclude this KeyValue. See ReturnCode. 
• {@link #transformCell(Cell)}: if the KeyValue is included, let the filter transform the KeyValue. 
• {@link #filterRowCells(List)}: allows direct modification of the final list to be submitted 
• {@link #filterRow()}: last chance to drop entire row based on the sequence of filter calls. Eg: filter a row if it doesn't contain a specified column. 
• {@link #reset()} : reset the filter state before filtering a new row. 

经验证的调用顺序如下:
查询级别(一次性)
	row 级别(可重复)
		Cell 级别(可重复)
{@link #parseFrom(byte[])}
{@link #isFamilyEssential(byte[])}
	{@link #hasFilterRow()}
	{@link #filterRowKey(byte[], int, int)}
		{@link #filterAllRemaining()}
		{@link #filterKeyValue(Cell)}
		{@link #transformCell(Cell)}
		.
		.
		{@link #filterAllRemaining()}
	{@link #filterRowCells(List)}
	{@link #filterRow()}
	{@link #hasFilterRow()}
	{@link #reset()}
	{@link #filterAllRemaining()}
	.
	.
	.
	{@link #filterAllRemaining()}

 * 功能描述: Filter 方法调用顺序测试
 * @createTime: 2016年11月21日 下午4:37:13
 * @author: <a href="mailto:676096658@qq.com">xiaochangyu</a>
 * @version: 0.1
 * @lastVersion: 0.1
 * @updateTime: 2016年11月21日 下午4:37:13
 * @updateAuthor: <a href="mailto:676096658@qq.com">xiaochangyu</a>
 * @changesSum:
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TestFilter extends Filter {

	private static Logger logger = LoggerFactory.getLogger(TestFilter.class);

	public static void println_all(String msg) {
		try {
			System.out.println(msg);
			logger.info(msg);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	private static AtomicLong runIndex = new AtomicLong();


	/**
	 * 每次查询都是不同的实例
	 * 创建一个新的实例 {@link TestFilter}.
	 */
	public TestFilter() {
	}



	/**
	 * Concrete implementers can signal a failure condition in their code by throwing an {@link IOException}.
	 * @param pbBytes A pb serialized {@link Filter} instance
	 * @return An instance of {@link Filter} made from <code>bytes</code>
	 * @throws DeserializationException
	 * @throws IOException in case an I/O or an filter specific failure needs to be signaled.
	 * @see #toByteArray
	 */
	public static Filter parseFrom(final byte[] pbBytes) throws DeserializationException {
		/*
		 * 查询级别(一次性)
		 * 反序列化为该 filter, 使用的是 pb 序列化后的字节数组		0
		 */
		println_all("TestFilter =======> parseFrom(final byte[] pbBytes), " + Arrays.toString(pbBytes) + ", run: " + runIndex.getAndIncrement());
		return new TestFilter();
	}

	@Override
	public boolean isFamilyEssential(byte[] name) throws IOException {
		/*
		 * 查询级别(一次性)
		 * 复杂逻辑可能会返回 false, 通常都返回 true. 判断 family 是否是必须的.		1
		 */
		println_all("TestFilter =======> isFamilyEssential(byte[] name), run: " + runIndex.getAndIncrement());
		return true;
	}

	@Override
	public boolean hasFilterRow() {
		/*
		 * row 级别(可重复)
		 * 是否会进入: filterRowCells(List<Cell> kvs) 和 filterRow() 过滤环节, true: 会, false: 不会		2		13		16...3
		 */
		println_all("TestFilter =======> hasFilterRow(), run: " + runIndex.getAndIncrement());
		return true;
	}

	@Override
	public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
		/*
		 * row 级别(可重复), 开始
		 * rowkey 过滤, true: 被过滤掉, false: 进入下一步过滤: filterKeyValue(Cell v)		3
		 */
		println_all("TestFilter =======> filterRowKey(byte[] buffer, int offset, int length), run: " + runIndex.getAndIncrement());
		return false;
	}

	@Override
	public boolean filterAllRemaining() throws IOException {
		/*
		 * row 级别(可重复), Cell 级别(可重复)
		 * 判断是否要进行接下来的过滤, true: 结束, false: 继续		4		7		10		15		end
		 */
		println_all("TestFilter =======> filterAllRemaining(), run: " + runIndex.getAndIncrement());
		return false;
	}

	@Override
	public ReturnCode filterKeyValue(Cell v) throws IOException {
		/*
		 * Cell 级别(可重复)
		 * family, qualifier, value 过滤. 下一步过滤: transformCell(Cell v)		5		8
		 */
		println_all("TestFilter =======> filterKeyValue(Cell v), run: " + runIndex.getAndIncrement());
		return ReturnCode.INCLUDE;
	}

	@Override
	public Cell transformCell(Cell v) throws IOException {
		/*
		 * Cell 级别(可重复)
		 * 可以对包含到结果中的 Cell 进行转换. 下一步过滤: filterRowCells(List<Cell> kvs)		6		9
		 */
		println_all("TestFilter =======> transformCell(Cell v), run: " + runIndex.getAndIncrement());
		return v;
	}

	@Override
	public void filterRowCells(List<Cell> kvs) throws IOException {
		/*
		 * row 级别(可重复), 结尾
		 * 允许对最终的结果进行修改. 下一步过滤: filterRow()		11
		 */
		println_all("TestFilter =======> filterRowCells(List<Cell> kvs), run: " + runIndex.getAndIncrement());
	}

	@Override
	public boolean filterRow() throws IOException {
		/*
		 * row 级别(可重复), 结尾
		 * 最后去掉该 row 的机会, true: 被过滤掉, false: 保留.		12
		 */
		println_all("TestFilter =======> filterRow(), run: " + runIndex.getAndIncrement());
		return false;
	}

	@Override
	public void reset() throws IOException {
		/*
		 * row 级别(可重复), 结尾
		 * 行之间的重置过滤器调用		14
		 */
		println_all("TestFilter =======> reset(), run: " + runIndex.getAndIncrement());
	}

	@Override
	public KeyValue transform(KeyValue currentKV) throws IOException {
		/* 已过期, 由: transformCell(Cell v) 代替 */
		println_all("TestFilter =======> transform(KeyValue currentKV)已过期, run: " + runIndex.getAndIncrement());
		return currentKV;
	}

	@Override
	public Cell getNextCellHint(Cell currentKV) throws IOException {
		/*
		 * 指定下一步必须搜索的 key, 需要: filterKeyValue(Cell v) 返回 SEEK_NEXT_USING_HINT. 返回 null 这表示不确定
		 */
		println_all("TestFilter =======> getNextCellHint(Cell currentKV), run: " + runIndex.getAndIncrement());
		return null;
	}

	@Override
	public KeyValue getNextKeyHint(KeyValue currentKV) throws IOException {
		/* 已过期, 由: getNextCellHint(Cell currentKV) 代替 */
		println_all("TestFilter =======> getNextKeyHint(KeyValue currentKV)已过期, run: " + runIndex.getAndIncrement());
		return null;
	}

	@Override
	public byte[] toByteArray() throws IOException {
		/*
		 * 客户端使用, 发送 filter 具体信息
		 * 这个 filter 使用 pb 来序列化成字节数组
		 */
		println_all("TestFilter =======> toByteArray(), run: " + runIndex.getAndIncrement());
		return new byte[0];
	}

	/**
	 * Given the filter's arguments it constructs the filter <p>
	 * @param filterArguments the filter's arguments
	 * @return constructed filter object
	 */
	public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
		/*
		 * 根据参数来实例化 filter
		 */
		println_all("TestFilter =======> parseFrom(final byte[] pbBytes), " + Arrays.deepToString(filterArguments.toArray()) + ", run: " + runIndex.getAndIncrement());
		return new TestFilter();
	}

	@Override
	boolean areSerializedFieldsEqual(Filter other) {
		/*
		 * 判断这个 filter 的字段是否是可序列化, 并且和目标 filter (客户端对服务端) 一一对应.
		 */
		println_all("TestFilter =======> areSerializedFieldsEqual(Filter other), run: " + runIndex.getAndIncrement());
		return true;
	}

}
