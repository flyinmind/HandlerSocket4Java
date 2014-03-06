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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HSResultFuture {
    private static final Logger LOG = LoggerFactory.getLogger(HSResultFuture.class.getName());
    private static long DEFAULT_TIMEOUT = 5000;
    
	private final HSResult[] resultSet;
	private final int taskNum;
	private int okNum = 0;
	private final CountDownLatch counter;

	public HSResultFuture(HSResult[] resultSet) {
		this.resultSet = resultSet;
		this.taskNum = resultSet.length;
		this.counter = new CountDownLatch(1);
	}

	public HSResult[] get() {
		return get(DEFAULT_TIMEOUT);
	}

	public HSResult[] get(long timeout /*milli-seconds*/) {
	    boolean waitResult = true;
		try {
		    waitResult = counter.await(timeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			// ignore
		}
		
		if(!waitResult) {
		    LOG.debug("Result feture timeout({})", timeout);
		}
		
		if(!isReady()) {
		    Exception cause = new TimeoutException("request reset by timeout");
		    for(int i = okNum; i < taskNum; i++) {
		        resultSet[i].setCause(cause);
		    }
		}

		return resultSet;
	}

	public synchronized HSResult getResult() {
	    if(isReady()) {
            return null;
	    }
	    
	    return resultSet[okNum++];
	}

	public boolean isReady() {
		return okNum >= taskNum;
	}
	
	public void finish() {
	    counter.countDown();
	}
	
	/**
	 * default request timeout,
	 * when get(), use default value
	 * @param timeout
	 */
	public static final void setDefaultTimeout(long timeout) {
	    DEFAULT_TIMEOUT = timeout;
	}
}