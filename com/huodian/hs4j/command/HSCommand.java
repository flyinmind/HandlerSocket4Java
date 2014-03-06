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

import com.huodian.hs4j.core.HSIndexDescriptor;
import com.huodian.hs4j.core.HSProto;
import com.huodian.hs4j.core.SafeByteStream;

abstract public class HSCommand {
	protected HSIndexDescriptor indexDescr;

	public void encode(final HSIndexDescriptor indexDescr, final SafeByteStream output) {
		if (null == indexDescr) {
			throw new InvalidParameterException("indexDescr can't be null");
		}

		this.indexDescr = indexDescr;

		encode(output);
	}

    private static enum ParseState {
        STATUS, NUMCOLUMNS, BODY, REASON
    }
    private ParseState currentState = ParseState.STATUS;
    private int responseStatus = HSProto.STATUS_TIMEOUT;
    private int columnNum;
    private String reason = "";
    
    public int getStatus() {
        return responseStatus;
    }
    
    protected int getColumnNum() {
        return columnNum;
    }
    
	abstract protected void encode(final SafeByteStream output);
    
	/**
     * decode response body
     * @param input
     * @param start
     * @param end
     * @return
     */
    protected boolean decodeBody(final SafeByteStream input, int start, int end) {
        return true;
    }
    
    public boolean decode(final SafeByteStream input) {
        int v;
        byte b;
        int start = 0;
        int end = input.getLength();
        
        LABEL: while(start < end) {
            switch (this.currentState) {
            case STATUS:
                if (end - start < 2) {
                    return false;
                }
                this.responseStatus = input.getByte(start++) - 0x30;
                start++; //skip Seperator
                this.currentState = ParseState.NUMCOLUMNS;
                continue LABEL;
            case NUMCOLUMNS:
                while(start < end) {
                    b = input.getByte(start++);
                    if (b == HSProto.PACKET_DELIMITER) {
                        return true;
                    } else if(b == HSProto.TOKEN_DELIMITER) {
                        if(this.responseStatus == HSProto.STATUS_OK) {
                            this.currentState = ParseState.BODY;
                        } else {
                            this.currentState = ParseState.REASON;
                        }
                        continue LABEL;
                    } else {
                        v = this.columnNum;
                        this.columnNum = (v << 3) + (v << 1) /* *= 10 */ + (((int)b) & 0xff) - 0x30;
                    }
                }
                return false;
            case BODY:
                if (start >= end) {
                    return false;
                }
                return decodeBody(input, start, end);
            case REASON:
                while(start < end) {
                    b = input.getByte(start++);
                    if (b == HSProto.PACKET_DELIMITER || b == HSProto.TOKEN_DELIMITER) {
                        break;
                    }
                    this.reason += (char)b;
                }
                return true;
            }
        }

        return false;
    }

	public HSIndexDescriptor getIndexDescriptor() {
		return indexDescr;
	}
	
	public String getReason() {
	    switch(this.responseStatus) {
	    case HSProto.STATUS_OK:
	        return "OK";
        case HSProto.STATUS_TIMEOUT:
            return "TIMEOUT";
        default:
            return this.reason;
	    }
	}
	
	@Override
	public String toString() {
	    return "class:" + this.getClass().getName()
	           + ",response status:" + getReason()
	           + ",columnNum:" + this.columnNum;
	}
}