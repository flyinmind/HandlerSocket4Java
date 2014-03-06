/*
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

package com.huodian.hs4j.command;

import java.security.InvalidParameterException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.huodian.hs4j.core.FilterType;
import com.huodian.hs4j.core.HSIndexDescriptor;
import com.huodian.hs4j.core.HSProto;
import com.huodian.hs4j.core.ResultSetImpl;
import com.huodian.hs4j.core.SafeByteStream;

public class HSFind extends HSCommand {
	protected CompareOperator operator = null;
	protected String[] conditions = null;

	// extra params
	// LIM
	protected int limit = 1;
	protected int offset = 0;
	
	private ResultSet result = null;

	// [IN]
	protected int inIndexNumber = 0;
	protected String[] inIndexValues = null;

	// [FILTER]
	protected List<Filter> filters = new ArrayList<Filter>();

	public HSFind() {
		this(CompareOperator.GE, new String[]{""});
	}

	public HSFind(CompareOperator operator, String[] conditions) {
		if (conditions == null) {
		    throw new InvalidParameterException("conditions can't be null");
		}
		this.operator = operator;
		this.conditions = conditions;
	}

	@Override
	protected void encode(final SafeByteStream output) {
		output.writeString(indexDescr.getIndexId(), false);
		output.writeByte(HSProto.TOKEN_DELIMITER);

		output.writeBytes(operator.getValue(), false);
		output.writeByte(HSProto.TOKEN_DELIMITER);

		output.writeString(String.valueOf(conditions.length), false);
		output.writeByte(HSProto.TOKEN_DELIMITER);

		output.writeStrings(conditions, HSProto.TOKEN_DELIMITER, true);
		output.writeByte(HSProto.TOKEN_DELIMITER);

		output.writeString(String.valueOf(limit), false);
		output.writeByte(HSProto.TOKEN_DELIMITER);

		output.writeString(String.valueOf(offset), false);

		if (inIndexValues != null && inIndexValues.length > 0) {
			output.writeByte(HSProto.TOKEN_DELIMITER);
			output.writeByte(HSProto.OPERATOR_IN);
			output.writeByte(HSProto.TOKEN_DELIMITER);

			output.writeString(String.valueOf(inIndexNumber), false);
			output.writeByte(HSProto.TOKEN_DELIMITER);

			output.writeString(String.valueOf(inIndexValues.length), false);
			output.writeByte(HSProto.TOKEN_DELIMITER);

			output.writeStrings(inIndexValues, HSProto.TOKEN_DELIMITER, true);
		}

		if (filters.size() > 0) {
			for (Filter filter : filters) {
				filter.encode(output);
			}
		}

		// extend it in update/delete
		modify(output);

		output.writeByte(HSProto.PACKET_DELIMITER, false);
	}

	protected void modify(SafeByteStream output) {
		// nothing, override in modify operations
	}
	
	public HSFind where(CompareOperator operator, String[] conditions) {
		if (conditions == null) {
			throw new IllegalArgumentException("conditions can't be NULL");
		}

		if (operator == null) {
			throw new IllegalArgumentException("operator can't be NULL");
		}

		if (conditions == null || conditions.length == 0) {
			throw new IllegalArgumentException("conditions can't be EMPTY");
		}

		this.operator = operator;
		this.conditions = conditions;

		return this;
	}

	public HSFind limit(int limit) {
		if (limit < 1) {
			throw new IllegalArgumentException("limit must be ONE or greater");
		}

		this.limit = limit;
		return this;
	}

	public HSFind offset(int offset) {
		if (limit < 0) {
			throw new IllegalArgumentException("limit must be ZERO or greater");
		}

		this.offset = offset;
		return this;
	}

	public HSFind in(int indexFieldNumber, String[] conditions) {
		inIndexNumber = indexFieldNumber;
		inIndexValues = conditions;
		return this;
	}

	public HSFind filter(String column, CompareOperator operator, String value) {
		filters.add(new Filter(FilterType.FILTER, operator, column, value));
		return this;
	}

	public HSFind till(String column, CompareOperator operator, String value) {
		filters.add(new Filter(FilterType.WHILE, operator, column, value));
		return this;
	}

	public void reset() {
		filters.clear();

		inIndexNumber = 0;
		inIndexValues = null;

		limit = 1;
		offset = 0;

		conditions = null;
		operator = null;
	}

	class Filter {
		public final FilterType type;
		public final CompareOperator operator;
		public final String column;
		public final String value;

		public Filter(FilterType type, CompareOperator operator, String column, String value) {
			this.value = value;
			this.operator = operator;
			this.type = type;
			this.column = column;
		}

		public void encode(SafeByteStream output) {
			validate();

			output.writeByte(HSProto.TOKEN_DELIMITER);
			output.writeByte(type.getValue());
			output.writeByte(HSProto.TOKEN_DELIMITER);
			output.writeBytes(operator.getValue(), false);
			output.writeByte(HSProto.TOKEN_DELIMITER);
			output.writeString(String.valueOf(indexDescr.getFilterColumnPos(column)), false);
			output.writeByte(HSProto.TOKEN_DELIMITER);
			output.writeString(value, true);
		}

		private void validate() {
			if (value == null || operator == null || type == null || !indexDescr.hasFilterColumn(column)) {
				throw new IllegalArgumentException("invalid filter");
			}
		}
	}

    @Override
    protected boolean decodeBody(SafeByteStream input, int start, int end) {
        if(getStatus() != HSProto.STATUS_OK || getColumnNum() <= 0) {
            return true;
        }
        
        HSIndexDescriptor idxDesc = getIndexDescriptor(); 
        String[] cols = idxDesc.getColumns();
        int colNum = cols.length;
        List<List<byte[]>> rows = new ArrayList<List<byte[]>>(getColumnNum() / colNum);
        List<byte[]> oneRow = new ArrayList<byte[]>(colNum);
        
        int currentCols = 0;
        int colBytesSize = 0;
        int pos = start;
        int preIdx = start;
        byte one;
        
        while(pos < end) {
            one = input.getByte(pos++);
            
            if (one == HSProto.TOKEN_DELIMITER || one == HSProto.PACKET_DELIMITER) {
                /**
                 * Patched by sam.tingleff
                 * "A character in the range [0x00 - 0x0f] is prefixed by 0x01 and shifted by 0x40"
                 */
                byte[] colData = new byte[colBytesSize];
                boolean shift = false;
                int colDataIndex = 0;
                int segEnd = pos - 1;
                byte b;
                
                for (int i = preIdx; i < segEnd; i++) {
                    b = input.getByte(i);
                    if (b == HSProto.UNSAFE_BYTE_MARKER) {
                        shift = true;
                        continue;
                    }
                    colData[colDataIndex++] = (shift) ? (byte) (b & ~HSProto.UNSAFE_BYTE_MASK) : b;
                    shift = false;
                }
                
                oneRow.add(colData);
                colBytesSize = 0; // colBytesSize must reset to zero
                currentCols++;
                if (currentCols == colNum) {
                    currentCols = 0;
                    rows.add(oneRow);
                    oneRow = new ArrayList<byte[]>(colNum);
                }
                preIdx = pos;
            } else if (one != HSProto.UNSAFE_BYTE_MARKER) {
                colBytesSize++;
            }
        }
        
        if (!oneRow.isEmpty()) {
            rows.add(oneRow);
        }
        this.result = new ResultSetImpl(rows, cols, input.getCharset());
        
        return true;
    }

    public ResultSet getResult() {
        return this.result;
    }
}