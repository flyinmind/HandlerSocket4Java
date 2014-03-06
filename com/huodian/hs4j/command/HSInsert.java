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

public class HSInsert extends HSCommand {

	protected String[] values;

	public HSInsert() {
		this(null);
	}

	public HSInsert(String[] values) {
        if (null == values) {
            throw new InvalidParameterException("values can't be null");
        }

		this.values = values;
	}

	@Override
	public void encode(SafeByteStream output) {
		output.writeString(indexDescr.getIndexId(), false);
		output.writeByte(HSProto.TOKEN_DELIMITER);
		output.writeByte(HSProto.OPERATOR_INSERT);
		output.writeByte(HSProto.TOKEN_DELIMITER);
		output.writeString(String.valueOf(values.length), false);
		output.writeByte(HSProto.TOKEN_DELIMITER);
		output.writeStrings(values, HSProto.TOKEN_DELIMITER, true);
		output.writeByte(HSProto.PACKET_DELIMITER);
	}

	public HSInsert values(String[] values) {
		if (null == values) {
			throw new InvalidParameterException("values can't be null");
		}

		this.values = values;

		return this;
	}
}