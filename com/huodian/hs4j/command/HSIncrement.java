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

public class HSIncrement extends HSModify {
	public HSIncrement() {
		this(CompareOperator.GE, new String[]{""}, null);
	}

	public HSIncrement(CompareOperator operator, String[] conditions, String[] values) {
		super(HSProto.OPERATOR_INCREMENT, operator, conditions, values);
	}
}