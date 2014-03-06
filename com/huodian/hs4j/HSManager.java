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

package com.huodian.hs4j;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.ChannelGroupFutureListener;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.nio.charset.Charset;
import java.security.InvalidParameterException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huodian.hs4j.command.HSAuth;
import com.huodian.hs4j.command.HSCommand;
import com.huodian.hs4j.command.HSOpenIndex;
import com.huodian.hs4j.core.HSIndexDescriptor;
import com.huodian.hs4j.core.HSProto;
import com.huodian.hs4j.netty.HSConnection;
import com.huodian.hs4j.netty.HSConnection.HSConnectionMode;
import com.huodian.hs4j.result.HSResult;
import com.huodian.hs4j.result.HSResultFuture;

public class HSManager {
    public static final int READWRITE_PORT = 9999;
    public static final int READONLY_PORT = 9998;
    public static final long DEFAULT_CONNECT_TIMEOUT = 10000; //ms
    public static final long DEFAULT_REQUEST_TIMEOUT = 5000; //ms
    
    private static final Logger LOG = LoggerFactory.getLogger(HSManager.class.getName());
    
    protected Charset charset;
    private AtomicInteger curConnection = new AtomicInteger(0);
    
    protected final HSConnection[] connections;

    protected final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    protected final EventLoopGroup workerGroup;
    
    private long connectionTimeout = DEFAULT_CONNECT_TIMEOUT;
    

    public HSManager(String host, String secret) throws TimeoutException, HSException {
        this(
            host, secret,
            4,
            Runtime.getRuntime().availableProcessors(),
            DEFAULT_CONNECT_TIMEOUT,
            DEFAULT_REQUEST_TIMEOUT
        );
    }
    
    public HSManager(String host, String secret, int poolSize, int threadNum,
            long connectionTimeout, long requestTimeout) throws TimeoutException, HSException {
        this(
            host, secret,
            HSConnectionMode.READ_WRITE,
            READWRITE_PORT, 
            READONLY_PORT,
            poolSize,
            threadNum,
            Charset.forName("UTF-8"),
            connectionTimeout,
            requestTimeout
        );
    }
    
    public HSManager(String host, String secret, int poolSize, int threadNum)
            throws TimeoutException, HSException {
        this(
            host, secret,
            poolSize,
            threadNum,
            DEFAULT_CONNECT_TIMEOUT,
            DEFAULT_REQUEST_TIMEOUT
        );
    }
    
	public HSManager(String host, String secret, HSConnectionMode mode, int readWritePort,
	        int readOnlyPort, int poolSize, int threadNum,
	        Charset charset, long connectionTimeout, long requestTimeout) throws TimeoutException, HSException {
        if(host == null || host.equals("")) {
            throw new InvalidParameterException("host is invalid");
        }
        
	    if(poolSize <= 0 || threadNum <= 0) {
	        throw new InvalidParameterException("poolSize and threadNum must be big than 0");
	    }
	    
        if(readWritePort <= 1024 || readOnlyPort <= 1024) {
            throw new InvalidParameterException("readWritePort and readOnlyPort must be big than 1024");
        }
        
        HSResultFuture.setDefaultTimeout(requestTimeout);
        
        this.charset = charset;
        this.connectionTimeout = connectionTimeout;
        
        this.connections = new HSConnection[poolSize];
        
        this.workerGroup = new NioEventLoopGroup(threadNum);
        
        for(int i = 0; i < poolSize; i++) {
            HSConnection cp = new HSConnection(host, this, mode, readOnlyPort, readWritePort, charset);
            this.connections[i] = cp;
            
            HSResultFuture resultSet = cp.execute(new HSAuth(secret));
            HSResult[] results = resultSet.get();
            HSCommand cmd = results[0].getCommand(); 
            if(cmd.getStatus() != HSProto.STATUS_OK) {
                throw new HSException("Fail to connect " + host + ",for fail to auth, reason:" + cmd.getReason());
            }
        }
	}

	public EventLoopGroup getWorkGroup() {
	    return workerGroup;
	}
	
	public ChannelGroup getChannelGroup() {
	    return this.channelGroup;
	}
	
	/**
	 * close all channels
	 * @return
	 */
	public synchronized ChannelGroupFuture close() {
		ChannelGroupFuture result = channelGroup.close();

		result.addListener(new ChannelGroupFutureListener() {
			public void operationComplete(ChannelGroupFuture future) throws Exception {
				LOG.info("channel group closed, all channels in it closed");
			}
		});

		return result;
	}

	public Charset getCharset() {
		return charset;
	}
	
	public long getConnectionTimeout() {
	    return connectionTimeout;
	}
	
    public HSResultFuture execute(HSIndexDescriptor indexDescr, HSCommand command) {
        return execute(indexDescr, new HSCommand[] {command});
    }

    /**
     * execute a command in all connections
     * @param indexDescr
     * @param command
     * @param timeout
     * @return
     *  > 0, failed
     *  == 0, all succeeded 
     */
    public int openIndex(HSIndexDescriptor indexDescr) {
        int sn = 0;
        
        for(HSConnection conn : connections) {
            sn++;
            HSResultFuture resultSet = conn.execute(indexDescr, new HSOpenIndex());
            HSResult[] results = resultSet.get();
            HSCommand cmd = results[0].getCommand(); 
            if(cmd.getStatus() != HSProto.STATUS_OK) {
                LOG.info("Fail to open index:{}, reason:{}", indexDescr.toString(), cmd.getReason());
                return sn;
            }
        }
        
        return HSProto.STATUS_OK;
    }

    /**
     * select one connection to execute commands
     * @param indexDescr
     *  index opened with command HSOpenIndex
     * @param commands
     *  command list, if HSFind, can't be more than one
     * @return
     *  result future
     */
    public HSResultFuture execute(HSIndexDescriptor indexDescr, HSCommand[] commands) {
        int cur = curConnection.incrementAndGet() % connections.length;
        return connections[cur].execute(indexDescr, commands);
    }
}