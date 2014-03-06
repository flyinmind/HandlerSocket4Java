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

import com.huodian.hs4j.core.HSProto;
import com.huodian.hs4j.core.SafeByteStream;

public class HSDelete extends HSModify {
	public HSDelete() {
		this(CompareOperator.GE, new String[] {""});
	}

	public HSDelete(CompareOperator operator, String[] conditions) {
		super(HSProto.OPERATOR_DELETE, operator, conditions, null);
	}

    @Override
    protected void modify(SafeByteStream output) {
        output.writeByte(HSProto.TOKEN_DELIMITER);
        output.writeByte(modOperator);
        //no values
    }
	
	@Override
	public HSFind values(String[] values) {
		throw new UnsupportedOperationException("can't perform this operation for DELETE query");
	}
}