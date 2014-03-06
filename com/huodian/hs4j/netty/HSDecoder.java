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

package com.huodian.hs4j.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import com.huodian.hs4j.core.HSProto;
import com.huodian.hs4j.result.HSResult;
import io.netty.handler.codec.ByteToMessageDecoder;

public class HSDecoder extends ByteToMessageDecoder {
    private final HSConnection connection;
    private HSResult curResult = null;
    
    public HSDecoder(HSConnection connection) {
        if(connection == null) {
            throw new NullPointerException("connection is null");
        }
        
        this.connection = connection;
    }
    
	@Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
        byte b;

        int len = buffer.readableBytes();
        if(curResult == null) {
            curResult = connection.getNextResult();
        }

        for(int i = 0; i < len; i++) {
            b = buffer.readByte();
            if (b == HSProto.PACKET_DELIMITER) {
                curResult = connection.getNextResult();
            } else if(curResult != null) {
                //buffer it, and decode it in user-thread when call getCommand()
                curResult.saveByte(b); 
            }
        }
		
		//needn't call buffer.release
	}
}