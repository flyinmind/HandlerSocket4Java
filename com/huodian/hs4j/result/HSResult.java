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

package com.huodian.hs4j.result;

import java.nio.charset.Charset;

import com.huodian.hs4j.command.HSCommand;
import com.huodian.hs4j.core.HSIndexDescriptor;
import com.huodian.hs4j.core.SafeByteStream;

public class HSResult {
	private Exception cause;

	private final HSIndexDescriptor indexDescr;
	private final HSCommand command;
    private final SafeByteStream buff;
    private boolean needDecod = true;

	public HSResult(HSIndexDescriptor indexDescr, HSCommand command, Charset charset) {
		this.indexDescr = indexDescr;
		this.command = command;
        this.buff = new SafeByteStream(1024, 1024, charset);
	}

	public void setCause(Exception cause) {
		this.cause = cause;
	}
	
	public Exception getCause() {
	    return cause;
	}
	
	public HSCommand getCommand() {
	    if(needDecod) {
	        needDecod = false;
	        command.decode(this.buff);
	    }
	    return command;
	}

	public HSIndexDescriptor getIndexDescriptor() {
	    return indexDescr;
	}
	
    public void saveByte(byte b) {
        this.buff.writeByte(b);
    }
	
	@Override
	public String toString() {
	    return "Command(" + this.command.toString()
	           + "),IndexDescriptor(" + this.indexDescr.toString() + ')';
	}
}