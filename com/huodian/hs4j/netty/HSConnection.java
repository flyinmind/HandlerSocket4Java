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

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.security.InvalidParameterException;
import java.util.LinkedList;
import java.util.concurrent.TimeoutException;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import com.huodian.hs4j.HSException;
import com.huodian.hs4j.HSManager;
import com.huodian.hs4j.command.HSAuth;
import com.huodian.hs4j.command.HSCommand;
import com.huodian.hs4j.core.HSIndexDescriptor;
import com.huodian.hs4j.core.SafeByteStream;
import com.huodian.hs4j.result.HSResult;
import com.huodian.hs4j.result.HSResultFuture;

public class HSConnection {
	private final String host;
	private final HSConnectionMode connectMode;
	private final int readOnlyPort;
	private final int readWritePort;
	
    protected final Bootstrap bootstrap;
    private Channel channel;
    private Charset charset;
    private final LinkedList<HSResultFuture> pendingResults = new LinkedList<HSResultFuture>();
	
    public static enum HSConnectionMode {
        READ_ONLY,
        READ_WRITE
    }
    
	public HSConnection(String host, HSManager manager, Charset charset) throws TimeoutException, HSException {
		this(host, manager, HSConnectionMode.READ_WRITE, 9998, 9999, charset);
	}

	public HSConnection(String host, HSManager manager,
	        HSConnectionMode connectMode, Charset charset) throws TimeoutException, HSException {
		this(host, manager, connectMode, 9998, 9999, charset);
	}

	public HSConnection(String host, HSManager manager,
	        HSConnectionMode connectMode, int readOnlyPort,
	        int readWritePort, Charset charset) throws TimeoutException, HSException {
		this.host = host;
		this.connectMode = connectMode;
		this.readOnlyPort = readOnlyPort;
		this.readWritePort = readWritePort;
		this.charset = charset;
		
        bootstrap = new Bootstrap();
        bootstrap.group(manager.getWorkGroup());
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();

                pipeline.addLast("decoder", new HSDecoder(HSConnection.this));
            }
        });
        this.channel = connect(connectMode, manager);
	}

    public Channel connect(HSConnectionMode mode, HSManager manager)
            throws TimeoutException, HSException {
        InetSocketAddress addr = new InetSocketAddress(getHost(), getPort(mode)); 
        ChannelFuture channelFuture = bootstrap.connect(addr);

        if (!channelFuture.awaitUninterruptibly(manager.getConnectionTimeout())) {
            throw new TimeoutException("Timeout to  connect " + addr.toString());
        }

        if (!channelFuture.isSuccess()) {
            throw new HSException("Fail to connection " + addr.toString());
        }

        Channel channel = channelFuture.channel();
        manager.getChannelGroup().add(channel);

        return channel;
    }
	
	public String getHost() {
		return host;
	}

	public HSConnectionMode getConnectMode() {
		return connectMode;
	}

	public int getReadOnlyPort() {
		return readOnlyPort;
	}

	public int getReadWritePort() {
		return readWritePort;
	}

	public int getPort(HSConnectionMode mode) {
		if (HSConnectionMode.READ_ONLY == mode) {
			return readOnlyPort;
		} else {
			return readWritePort;
		}
	}
	
    /**
     * execute without index, only HSAuth can use it
     * @param command
     * @return
     */
    public HSResultFuture execute(HSCommand command) {
        if(!(command instanceof HSAuth)) {
            throw new InvalidParameterException("Only HSAuth can be executed with it");
        }
        return execute(null, new HSCommand[] {command});
    }
    
    public HSResultFuture execute(HSIndexDescriptor indexDescr, HSCommand command) {
        return execute(indexDescr, new HSCommand[] {command});
    }

    public HSResultFuture execute(HSIndexDescriptor indexDescr, HSCommand[] commands) {
        int size = commands.length;
        SafeByteStream packet = new SafeByteStream(size << 9, size << 8, charset);
        HSResult[] resultSet = new HSResult[size];
        int i = 0;
        
        for (HSCommand cmd : commands) {
            resultSet[i++] = new HSResult(indexDescr, cmd, charset);
            cmd.encode(indexDescr, packet);
        }

        HSResultFuture resultFuture = new HSResultFuture(resultSet);
        synchronized(pendingResults) {
            pendingResults.addFirst(resultFuture);
            channel.writeAndFlush(packet.copy()); //must flush
        }

        return resultFuture;
    }

    public synchronized HSResult getNextResult() {
        if(pendingResults.size() <= 0) {
            return null;
        }
        
        HSResultFuture resultSet = pendingResults.getLast();
        if(resultSet.isReady()) {
            if(pendingResults.size() <= 0) {
                return null;
            }
            pendingResults.removeLast();
            
            resultSet.finish();

            if(pendingResults.size() <= 0) {
                return null;
            }
            
            resultSet = pendingResults.peek();
        }
        
        return resultSet.getResult();
    }

    public ChannelFuture close() {
        return channel.close();
    }
}