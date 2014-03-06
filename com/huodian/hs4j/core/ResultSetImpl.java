/**
 *Copyright [2010-2011] [dennis zhuang(killme2008@gmail.com)]
 *Licensed under the Apache License, Version 2.0 (the "License");
 *you may not use this file except in compliance with the License. 
 *You may obtain a copy of the License at 
 *             http://www.apache.org/licenses/LICENSE-2.0 
 *Unless required by applicable law or agreed to in writing, 
 *software distributed under the License is distributed on an "AS IS" BASIS, 
 *WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 *either express or implied. See the License for the specific language governing permissions and limitations under the License
 */
package com.huodian.hs4j.core;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * A java.sql.ResultSet implementation,most methods are not supported,use
 * getString as much as possible.
 * 
 * @author dennis
 * @date 2010-11-28
 */
public class ResultSetImpl implements ResultSet {
	private final List<List<byte[]>> rows;
	private final String[] fieldList;
    private Map<String, Integer> fieldMap = null;
	private int rowNo = this.BEFORE_FIRST;
	private final Charset charset;

	private final int BEFORE_FIRST = Integer.MIN_VALUE;

	private final int AFTER_LAST = Integer.MAX_VALUE;

	private boolean lastWasNull = false;

	public ResultSetImpl(List<List<byte[]>> rows, String[] fieldList, Charset charset) {
		this.rows = rows;
		this.fieldList = fieldList;
		this.charset = charset;
	}

    @Override
	public boolean absolute(int row) throws SQLException {
		if (this.rows.isEmpty()) {
			return false;
		}
		
		if (row == 0) {
			throw new SQLException("row must not be zero");
		}
		
		if (row > this.rows.size()) {
			this.rowNo = this.AFTER_LAST;
			return false;
		}
		
		if (row < 0 && -row > this.rows.size()) {
			this.rowNo = this.BEFORE_FIRST;
			return false;
		}
		
		if (row < 0) {
			int newPos = this.rows.size() + row + 1;
			return this.absolute(newPos);
		}
		
		this.rowNo = row - 1;
		
		return true;
	}

    @Override
	public void afterLast() throws SQLException {
		if (this.rows.isEmpty()) {
			return;
		}
		this.rowNo = this.AFTER_LAST;
	}

    @Override
	public void beforeFirst() throws SQLException {
		if (this.rows.isEmpty()) {
			return;
		}
		this.rowNo = this.BEFORE_FIRST;
	}

    @Override
	public void cancelRowUpdates() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void clearWarnings() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void close() throws SQLException {
	}

    @Override
	public void deleteRow() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public int findColumn(String columnName) throws SQLException {
	    if(this.fieldMap == null) {
	        this.fieldMap = new HashMap<String, Integer>();
	        int index = 1;
	        for (String name : this.fieldList) {
	            this.fieldMap.put(name.toLowerCase(), index++);
	        }
	    }
	    
		Integer idx = this.fieldMap.get(columnName.toLowerCase());
		if(idx == null) {
			throw new SQLException("column " + columnName + " is not in result set");
		}
		return idx.intValue();
	}

    @Override
	public boolean first() throws SQLException {
		if (this.rows.isEmpty()) {
			return false;
		}
		this.rowNo = 0;
		return true;
	}

    @Override
	public Array getArray(int i) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public Array getArray(String colName) throws SQLException {
        throw new UnsupportedOperationException();
	}

	private void checkRowCol(int columnIndex) throws SQLException {
		if (this.rowNo < 0 || this.rowNo + 1 > this.rows.size()) {
			throw new SQLException("Invalid row:" + this.rowNo);
		}
		if (columnIndex <= 0 || columnIndex > this.fieldList.length) {
			throw new SQLException("Invalid col:" + columnIndex);
		}
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		this.checkRowCol(columnIndex);
		byte[] data = this.getColumnData(columnIndex);
		if (data == null) {
			return null;
		}
		return new ByteArrayInputStream(data);
	}

	public InputStream getAsciiStream(String columnName) throws SQLException {
		return this.getAsciiStream(this.findColumn(columnName));
	}

    @Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		String stringVal = this.getString(columnIndex, true);
        if (stringVal == null) {
            return null;
        }

        if (stringVal.length() == 0) {
            stringVal = "0";
        }

        try {
            return new BigDecimal(stringVal).setScale(scale);
        } catch (ArithmeticException ex) {
            try {
                return new BigDecimal(stringVal).setScale(scale, BigDecimal.ROUND_HALF_UP);
            } catch (ArithmeticException arEx) {
                throw new SQLException("ResultSet.Bad_format_for_BigDecimal: value=" + stringVal + ",scale=" + scale);
            }
        } catch (NumberFormatException ex) {
            throw new SQLException("ResultSet.Bad_format_for_BigDecimal: value=" + stringVal + ",scale=" + scale);
        }
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return getBigDecimal(columnIndex, BigDecimal.ROUND_UNNECESSARY);
	}

    @Override
	public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
		return this.getBigDecimal(this.findColumn(columnName), scale);
	}

	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		return this.getBigDecimal(this.findColumn(columnName), BigDecimal.ROUND_UNNECESSARY);
	}

    @Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		return this.getAsciiStream(columnIndex);
	}

    @Override
	public InputStream getBinaryStream(String columnName) throws SQLException {
		return this.getBinaryStream(this.findColumn(columnName));
	}

    @Override
	public Blob getBlob(int i) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public Blob getBlob(String colName) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		String stringVal = this.getString(columnIndex, true);
        if (!isNull(stringVal)) {
            int c = Character.toLowerCase(stringVal.charAt(0));
            return c == 't' || c == 'y' || c == '1' || stringVal.equals("-1");
        }

        return false;
	}

    @Override
	public boolean getBoolean(String columnName) throws SQLException {
		return this.getBoolean(this.findColumn(columnName));
	}

    @Override
	public byte getByte(int columnIndex) throws SQLException {
		String stringVal = this.getString(columnIndex, true);
        if (isNull(stringVal)) {
            return (byte)0;
        }
        try {
            return Byte.parseByte(stringVal);
        } catch (NumberFormatException NFE) {
            throw new SQLException("Parse byte value error:" + stringVal);
        }
	}

    @Override
	public byte getByte(String columnName) throws SQLException {
		return this.getByte(this.findColumn(columnName));
	}

    @Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		this.checkRowCol(columnIndex);
		return this.getColumnData(columnIndex);
	}

    @Override
	public byte[] getBytes(String columnName) throws SQLException {
		return this.getBytes(this.findColumn(columnName));
	}

    @Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		String stringVal = this.getString(columnIndex);
		if (stringVal == null) {
			return null;
		}
		return new StringReader(stringVal);
	}

    @Override
	public Reader getCharacterStream(String columnName) throws SQLException {
		return this.getCharacterStream(this.findColumn(columnName));
	}

    @Override
	public Clob getClob(int i) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public Clob getClob(String colName) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public int getConcurrency() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public String getCursorName() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		String stringVal = this.getString(columnIndex);
		return new Date(getDateFromString(stringVal, cal));
	}

    @Override
	public Date getDate(int columnIndex) throws SQLException {
		return this.getDate(columnIndex, null);
	}

    @Override
	public Date getDate(String columnName, Calendar cal) throws SQLException {
		return this.getDate(this.findColumn(columnName), cal);
	}

    @Override
	public Date getDate(String columnName) throws SQLException {
		return this.getDate(this.findColumn(columnName), null);
	}

	private static final long getDateFromString(String val, Calendar cal) throws SQLException {
		//0000-00-00 | 0000-00-00 00:00:00 | 00000000000000 | 0
		if (isNull(val) || val.indexOf("0000") == 0 || val.equals("0")) {
		    return 0L;
		}

		DateFormat dateFormat = DateFormat.getDateTimeInstance();
		if (cal != null) {
			TimeZone timeZone = cal.getTimeZone();
			dateFormat.setTimeZone(timeZone);
		}
		
		try {
			return dateFormat.parse(val).getTime();
		} catch (ParseException e) {
			throw new SQLException("Parse date error:" + val);
		}
	}

    @Override
	public double getDouble(int columnIndex) throws SQLException {
        String stringVal = this.getString(columnIndex, true);
        if (isNull(stringVal)) {
            return 0d;
        }
        
        try {
            return Double.parseDouble(stringVal);
        } catch (NumberFormatException e) {
            throw new SQLException("Parse double error:" + stringVal);
        }
	}

    @Override
	public double getDouble(String columnName) throws SQLException {
		return this.getDouble(this.findColumn(columnName));
	}

    @Override
	public int getFetchDirection() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public int getFetchSize() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public float getFloat(int columnIndex) throws SQLException {
        String stringVal = this.getString(columnIndex, true);
        if (isNull(stringVal)) {
            return 0f;
        }
        
        try {
            return Float.parseFloat(stringVal);
        } catch (NumberFormatException e) {
            throw new SQLException("Parse float error:" + stringVal);
        }
	}

    @Override
	public float getFloat(String columnName) throws SQLException {
		return this.getFloat(this.findColumn(columnName));
	}

    @Override
	public int getInt(int columnIndex) throws SQLException {
	    String stringVal = this.getString(columnIndex, true);
        if (isNull(stringVal)) {
            return 0;
        }
        
        try {
            return Integer.parseInt(stringVal);
        } catch (NumberFormatException e) {
            throw new SQLException("Parse integer error:" + stringVal);
        }
	}

    @Override
	public int getInt(String columnName) throws SQLException {
		return this.getInt(this.findColumn(columnName));
	}

    @Override
	public long getLong(int columnIndex) throws SQLException {
        String stringVal = this.getString(columnIndex, true);
        if (isNull(stringVal)) {
            return 0L;
        }
        
        try {
            return Long.parseLong(stringVal);
        } catch (NumberFormatException e) {
            throw new SQLException("Parse long error:" + stringVal);
        }
	}

    @Override
	public long getLong(String columnName) throws SQLException {
		return this.getLong(this.findColumn(columnName));
	}

    @Override
	public ResultSetMetaData getMetaData() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public Object getObject(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public Object getObject(String colName, Map<String, Class<?>> map) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public Object getObject(String columnName) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public Ref getRef(int i) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public Ref getRef(String colName) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public int getRow() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public short getShort(int columnIndex) throws SQLException {
        String stringVal = this.getString(columnIndex, true);
        if (isNull(stringVal)) {
            return 0;
        }
        
        try {
            return Short.parseShort(stringVal);
        } catch (NumberFormatException e) {
            throw new SQLException("Parse short error:" + stringVal);
        }
	}

    @Override
	public short getShort(String columnName) throws SQLException {
		return this.getShort(this.findColumn(columnName));
	}

    @Override
	public Statement getStatement() throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
	    return this.getString(columnIndex, false);
	}

    protected String getString(int columnIndex, boolean trim) throws SQLException {
        this.checkRowCol(columnIndex);
        byte[] data = this.getColumnData(columnIndex);
        if (data == null) {
            return null;
        }
        
        String s = new String(data, charset);
        
        return trim ? s.trim() : s;
    }

	private byte[] getColumnData(int columnIndex) {
		List<byte[]> row = this.rows.get(this.rowNo);
		if (row == null) {
			this.lastWasNull = true;
			return null;
		}
		byte[] data = row.get(columnIndex - 1);
		if (data == null) {
			this.lastWasNull = true;
			return null;
		}
		this.lastWasNull = false;
		return data;
	}

    @Override
	public String getString(String columnName) throws SQLException {
		return this.getString(this.findColumn(columnName), false);
	}

    @Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public Time getTime(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public Time getTime(String columnName, Calendar cal) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public Time getTime(String columnName) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		String stringVal = this.getString(columnIndex, true);
        return new Timestamp(getDateFromString(stringVal, cal));
	}

    @Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return this.getTimestamp(columnIndex, null);
	}

    @Override
	public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
		return this.getTimestamp(this.findColumn(columnName), cal);
	}

    @Override
	public Timestamp getTimestamp(String columnName) throws SQLException {
		return this.getTimestamp(columnName, null);
	}

    @Override
	public int getType() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public InputStream getUnicodeStream(String columnName) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public URL getURL(int columnIndex) throws SQLException {
	    String url = this.getString(columnIndex, true);
	    if(isNull(url)) {
	        return null;
	    }
	    
		try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new SQLException("Parse url error:" + url);
        }
	}

    @Override
	public URL getURL(String columnName) throws SQLException {
        return this.getURL(this.findColumn(columnName));
	}

    @Override
	public SQLWarning getWarnings() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void insertRow() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public boolean isAfterLast() throws SQLException {
		return this.rowNo == this.AFTER_LAST;
	}

    @Override
	public boolean isBeforeFirst() throws SQLException {
		return this.rowNo == this.BEFORE_FIRST;
	}

    @Override
	public boolean isFirst() throws SQLException {
		if (this.rows.isEmpty()) {
			return false;
		}
		return this.rowNo == 0;
	}

    @Override
	public boolean isLast() throws SQLException {
		if (this.rows.isEmpty()) {
			return false;
		}
		return this.rowNo + 1 == this.rows.size();
	}

    @Override
	public boolean last() throws SQLException {
		if (this.rows.isEmpty()) {
			return false;
		}
		this.rowNo = this.rows.size() - 1;
		return true;
	}

    @Override
	public void moveToCurrentRow() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void moveToInsertRow() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public boolean next() throws SQLException {
		if (this.rows.isEmpty()) {
			return false;
		}
		this.normalRowNO();
		if (this.rowNo + 1 >= this.rows.size()) {
			return false;
		}
		this.rowNo++;
		return true;
	}

	private void normalRowNO() {
		if (this.rowNo == this.BEFORE_FIRST) {
			this.rowNo = -1;
		} else if (this.rowNo == this.AFTER_LAST) {
			this.rowNo = this.rows.size();
		}
	}

    @Override
	public boolean previous() throws SQLException {
		if (this.rows.isEmpty()) {
			return false;
		}
		this.normalRowNO();
		if (this.rowNo == 0) {
			this.rowNo = this.BEFORE_FIRST;
			return false;
		}
		this.rowNo--;
		return true;
	}

    @Override
	public void refreshRow() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public boolean relative(int rows) throws SQLException {
		this.normalRowNO();
		return this.absolute(this.rowNo + rows + 1);
	}

    @Override
	public boolean rowDeleted() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public boolean rowInserted() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public boolean rowUpdated() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void setFetchDirection(int direction) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void setFetchSize(int rows) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateArray(String columnName, Array x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBlob(String columnName, Blob x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBoolean(String columnName, boolean x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateByte(String columnName, byte x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBytes(String columnName, byte[] x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateCharacterStream(String columnName, Reader reader, int length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateClob(String columnName, Clob x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateDate(String columnName, Date x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateDouble(String columnName, double x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateFloat(String columnName, float x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateInt(String columnName, int x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateLong(String columnName, long x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateNull(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateNull(String columnName) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateObject(String columnName, Object x, int scale) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateObject(String columnName, Object x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateRef(String columnName, Ref x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateRow() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateShort(String columnName, short x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateString(String columnName, String x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateTime(String columnName, Time x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public boolean wasNull() throws SQLException {
		return this.lastWasNull;
	}

    @Override
	public int getHoldability() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public String getNString(int columnIndex) throws SQLException {
		return this.getString(columnIndex);
	}

    @Override
	public String getNString(String columnName) throws SQLException {
        return this.getString(this.findColumn(columnName));
	}

    @Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public boolean isClosed() throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException();

	}

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateNString(int columnIndex, String nString) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateNString(String columnLabel, String nString) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException();
	}

    @Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException();
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException();
	}

	private static final boolean isNull(String stringVal) {
	    return stringVal == null || stringVal.isEmpty();
	}
}
