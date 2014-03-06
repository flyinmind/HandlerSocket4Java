/**
 * Copyright 2012 The HuoDian HandlerSocket Client For Java
 *
 * https://github.com/komelgman/Java-HandlerSocket-Connection/
 *
 * The Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.huodian.hs4j.core;

import java.util.concurrent.atomic.AtomicLong;

/**
 * From HS protocol description
 *
 * ----------------------------------------------------------------------------
 * Opening index
 *
 * The 'open_index' request has the following syntax.
 *
 *    P <indexid> <dbname> <tablename> <indexname> <columns> [<fcolumns>]
 *
 * - <indexid> is a number in decimal.
 * - <dbname>, <tablename>, and <indexname> are strings. To open the primary
 *   key, use PRIMARY as <indexname>.
 * - <columns> is a comma-separated list of column names.
 * - <fcolumns> is a comma-separated list of column names. This parameter is
 *  optional.
 *
 * Once an 'open_index' request is issued, the HandlerSocket plugin opens the
 * specified index and keep it open until the client connection is closed. Each
 * open index is identified by <indexid>. If <indexid> is already open, the old
 * open index is closed. You can open the same combination of <dbname>
 * <tablename> <indexname> multple times, possibly with different <columns>.
 * For efficiency, keep <indexid> small as far as possible.
 * ----------------------------------------------------------------------------
 *
 * HSIndexDescriptor class contains information about index according to the
 * HS protocol description
 */

public class HSIndexDescriptor {
	/**
	 * The following fields are equivalent to the corresponding fields of the
	 * protocol description
	 */
	private final String indexId;
	private final String dbName;
	private final String tableName;
	private final String indexName;
	private final String[] columns;
	private final String[] filterColumns;
	private final String[] indexColumns;

	private static AtomicLong generator = new AtomicLong(0L);

	public HSIndexDescriptor(String dbName, String tableName, String indexName, String[] columns) {
		this(generator.incrementAndGet(), dbName, tableName, indexName, columns, null, null);
	}

	public HSIndexDescriptor(String dbName, String tableName, String indexName, String[] columns,
			String[] filterColumns) {

		this(generator.incrementAndGet(), dbName, tableName, indexName, columns, filterColumns, null);
	}

	public HSIndexDescriptor(String dbName, String tableName, String indexName, String[] columns,
			String[] filterColumns, String[] indexColumns) {

		this(generator.incrementAndGet(), dbName, tableName, indexName, columns, filterColumns, indexColumns);
	}

	public HSIndexDescriptor(long indexId, String dbName, String tableName, String indexName, String[] columns) {
		this(indexId, dbName, tableName, indexName, columns, null, null);
	}

	public HSIndexDescriptor(long indexId, String dbName, String tableName, String indexName, String[] columns,
		String[] filterColumns) {
		this(indexId, dbName, tableName, indexName, columns, filterColumns, null);
	}

	/**
	 * @param indexId
	 * @param dbName
	 * @param tableName
	 * @param indexName
	 * @param columns
	 *     columns in result-set 
	 * @param filterColumns
	 *     filter columns
	 * @param indexColumns
	 *     columns in index
	 */
	public HSIndexDescriptor(long indexId, String dbName, String tableName, String indexName, String[] columns,
			String[] filterColumns, String[] indexColumns) {
		
		if (columns == null || tableName == null || indexName == null || dbName == null) {
			throw new IllegalArgumentException();
		}

		this.indexId = Long.toString(indexId);

		this.dbName = dbName;
		this.tableName = tableName;
		this.indexName = indexName;

		this.columns = columns == null ? new String[0] : columns;
		this.filterColumns = filterColumns == null ? new String[0] : filterColumns;
		this.indexColumns = indexColumns == null ? new String[0] : indexColumns;
	}

	public String getIndexId() {
		return indexId;
	}

	public String getDbName() {
		return dbName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getIndexName() {
		return indexName;
	}

	public String[] getColumns() {
		return columns;
	}
	
	public String[] getFilterColumns() {
		return filterColumns;
	}
	
    public String[] getIndexColumns() {
        return indexColumns;
    }
    
	public int getFilterColumnPos(String name) {
	    int n = 0;
	    for(String s : filterColumns) {
	        if(s.equals(name)) {
	            return n;
	        }
	        n++;
	    } 
	    return -1;
	}
	
	private static final int findStr(String name, String[] strs) {
	    if(strs == null) {
	        return -1;
	    }
	    
        int n = 0;
        for(String s : strs) {
            if(s.equals(name)) {
                return n;
            }
            n++;
        } 
        return -1;
	    
	}

	public boolean hasFilterColumn(String name) {
		return findStr(name, filterColumns) != -1;
	}

	public boolean hasFilterColumns() {
		return filterColumns.length > 0;
	}
    
	@Override
    public String toString() {
	    StringBuilder sb = new StringBuilder();
	    
	    sb.append("indexId:").append(this.indexId)
	      .append(",db:").append(this.dbName)
	      .append(",table:").append(this.tableName)
	      .append(",index:").append(this.indexName);
	    
	    mergeStrs(",value columns", columns, sb);
        mergeStrs(",index columns", indexColumns, sb);
        mergeStrs(",filter columns", filterColumns, sb);
        
        return sb.toString();
	}
	
	private static final void mergeStrs(String name, String[] strs, StringBuilder sb) {
	    int len;
        if(strs == null || (len = strs.length) <= 0) {
            return;
        }
        
        sb.append(name).append(":[").append(strs[0]);
        for(int i = 1; i < len; i++) {
            sb.append(',').append(strs[i]);
        }
        sb.append(']');
	}
}