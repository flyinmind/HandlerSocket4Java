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
import com.huodian.hs4j.core.HSProto;
import com.huodian.hs4j.core.SafeByteStream;

public class HSModify extends HSFind {
	protected String[] values;
	protected final byte modOperator;
	private int affectedRows = 0;

    public HSModify(CompareOperator operator, String[] conditions, String[] values) {
        this(HSProto.OPERATOR_UPDATE, operator, conditions, values);
    }
    
	public HSModify(byte modOperator, CompareOperator operator, String[] conditions, String[] values) {
		super(operator, conditions);

		this.values = values;
		this.modOperator = modOperator;
	}

	@Override
	protected void modify(SafeByteStream output) {
		output.writeByte(HSProto.TOKEN_DELIMITER);

		output.writeByte(modOperator);
		output.writeByte(HSProto.TOKEN_DELIMITER);
		output.writeStrings(values, HSProto.TOKEN_DELIMITER, true);
	}

    /* don't support modify_and_find
     * @see com.huodian.hs4j.command.HSFind#decodeBody(com.huodian.hs4j.core.SafeByteStream, int, int)
     */
    @Override
    protected boolean decodeBody(SafeByteStream input, int start, int end) {
        int pos = start;
        int v;
        byte b;
        
        while(pos < end) {
            b = input.getByte(pos++);
            
            if (b == HSProto.TOKEN_DELIMITER || b == HSProto.PACKET_DELIMITER) {
                break;
            }
            v = this.affectedRows;
            this.affectedRows = (v << 3) + (v << 1) /* *= 10 */ + ((((int)b) & 0xff) - 0x30); 
        }
        
        return true;
    }
	
	public HSFind values(String[] values) {
		if (null == values) {
			throw new InvalidParameterException("values can't be NULL");
		}

		this.values = values;
		
		return this;
	}
	
	public int getAffectedRows() {
	    return affectedRows;
	}
	
	@Override
	public String toString() {
	    return super.toString() + ",affectRow:" + this.affectedRows;
	}
}