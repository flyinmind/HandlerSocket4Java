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

package com.huodian.hs4j.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;
import java.security.InvalidParameterException;
import java.util.List;

public class SafeByteStream {
	private final int buffIncStepSize;
	private final int initialBufferSize;
	private final Charset charset;

	private int writePos = 0;
    private int readPos = 0;

	private byte[] buffer;
	private final byte[] unsafeBuffer = new byte[2];

	/**
	 * @param bufferSize
	 *  initial buffer size
	 * @param buffIncStepSize
	 *  increase size when buffer is not enough
	 * @param charset
	 */
	public SafeByteStream(final int bufferSize, final int buffIncStepSize, Charset charset) {
		this.buffIncStepSize = buffIncStepSize;
		this.initialBufferSize = bufferSize;
		this.charset = charset;

		this.buffer = new byte[bufferSize];
		this.unsafeBuffer[0] = HSProto.UNSAFE_BYTE_MARKER;
	}

	public synchronized void writeByte(byte b, boolean safe) {
		if (safe && /*unsigned*/ (0xFF & b) < 0x10) {
			unsafeBuffer[1] = (byte) (b ^ HSProto.UNSAFE_BYTE_MASK);
			_write(unsafeBuffer, 0, 2, false);
		} else {
			unsafeBuffer[1] = b;
			_write(unsafeBuffer, 1, 1, false);
		}
	}

    public synchronized void writeByte(byte b) {
        if (writePos + 1 > this.buffer.length) {
            ensureSize(writePos + 1);
        }

        this.buffer[writePos++] = b;
    }

	public synchronized void writeBytes(final byte[] b, boolean safe) {
		_write(b, 0, b.length, safe);
	}

	public synchronized void writeBytes(final byte[] b, final int offset, final int length, boolean safe) {
		_write(b, offset, length, safe);
	}

	public synchronized void writeString(String str, boolean safe) {
		final byte[] b = str.getBytes(charset);
		_write(b, 0, b.length, safe);
	}

	public synchronized void writeStrings(List<String> strings, byte[] delimiter, boolean safe) {
		int count = strings.size();
		byte[] b;

		for (String str : strings) {
			b = str.getBytes(charset);
			_write(b, 0, b.length, safe);
			if (--count > 0) {
				_write(delimiter, 0, delimiter.length, false);
			}
		}
	}

    public synchronized void writeStrings(String[] strings, byte[] delimiter, boolean safe) {
        int count = strings.length;
        byte[] b;

        for (String str : strings) {
            b = str.getBytes(charset);
            _write(b, 0, b.length, safe);
            if (--count > 0) {
                _write(delimiter, 0, delimiter.length, false);
            }
        }
    }

    public synchronized void writeStrings(String[] strings, byte delimiter, boolean safe) {
        int count = strings.length;
        byte[] b;

        for (String str : strings) {
            b = str.getBytes(charset);
            _write(b, 0, b.length, safe);
            if (--count > 0) {
                writeByte(delimiter);
            }
        }
    }

	private void _write(final byte[] bytes, final int offset, final int length, boolean safe) {
		if (length < 0) {
			throw new IllegalArgumentException();
		}
		if (offset < 0) {
			throw new IndexOutOfBoundsException();
		}
		if (bytes == null) {
			throw new NullPointerException();
		}
		if ((length + offset) > bytes.length) {
			throw new IndexOutOfBoundsException();
		}

		if (safe) {
			if (writePos + 2 * length > this.buffer.length) {
				ensureSize(writePos + 2 * length);
			}

			for (int i = offset; i < offset + length; ++i) {
				byte b = bytes[i];

				if (/*unsigned*/ (b & 0xFF) < 0x10) {
					buffer[writePos++] = HSProto.UNSAFE_BYTE_MARKER;
					buffer[writePos++] = (byte) (b ^ HSProto.UNSAFE_BYTE_MASK);
				} else {
					buffer[writePos++] = b;
				}
			}
		} else {
			if (writePos + length > this.buffer.length) {
				ensureSize(writePos + length);
			}

			System.arraycopy(bytes, offset, this.buffer, writePos, length);
			writePos += length;
		}
	}

	private void ensureSize(final int size) {
		final int computedSize = (int) Math.min((this.buffer.length + 1) * 1.5, this.buffer.length
				+ buffIncStepSize);
		final int newSize = Math.max(size, computedSize);
		final byte[] newBuffer = new byte[newSize];

		System.arraycopy(this.buffer, 0, newBuffer, 0, writePos);

		this.buffer = newBuffer;
	}

	public synchronized byte[] toByteArray() {
		final byte[] result = new byte[writePos];
		System.arraycopy(buffer, 0, result, 0, writePos);
		return result;
	}

	public int getLength() {
		return writePos;
	}
	
    public int getReadPos() {
        return readPos;
    }
	
	public byte read() {
        if(this.readPos >= this.writePos) {
            throw new IllegalStateException("Wrong read/write position");
        }
        
	    return this.buffer[this.readPos++];
	}
	
	public byte getByte(int pos) {
        if(pos >= this.writePos) {
            throw new IllegalStateException("Wrong read/write position");
        }
        return this.buffer[pos];
	}
	
	/**
	 * get a part from buffer
	 * @param start
	 * @param end
	 * @return
	 */
	public SafeByteStream copy(int start, int end) {
	    if(end < start || end >= this.writePos) {
	        throw new InvalidParameterException("Wrong end, start parameter");
	    }
	    
	    int size = end - start;
	    SafeByteStream sbs = new SafeByteStream(size, size, this.charset);
	    sbs.writeBytes(this.buffer, start, size, false);
	    return sbs;
	}
	
    public void copy(final ByteBuf buf) {
        int len;
        
        if(buf == null || (len = buf.readableBytes()) <= 0) {
            throw new InvalidParameterException("Wrong buf parameter");
        }
        
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        writeBytes(bytes, 0, len, false);
    }
    
	public ByteBuf copy() {
	    return Unpooled.wrappedBuffer(this.buffer, 0, this.writePos);
	}
	
    public void reset(int pos) {
        System.arraycopy(this.buffer, pos, this.buffer, 0, this.writePos - pos);
        this.writePos = pos;
        this.readPos = 0;
    }

    public synchronized void reset() {
        this.writePos = 0;
        this.readPos = 0;
    }
    
    public synchronized void reBuild() {
        this.buffer = new byte[this.initialBufferSize];
        this.writePos = 0;
        this.readPos = 0;
    }
    
    public Charset getCharset() {
        return charset;
    }
	/**
	 * It is danger to call this function,
	 * for the buffer may be changed
	 * @return
	 */
	public byte[] getRaw() {
		return buffer;
	}
}