package com.danikula.videocache;

import java.io.ByteArrayInputStream;

/**
 * Simple memory based {@link Source} implementation.
 *
 * @author Alexey Danilov (danikula@gmail.com).
 * @author zhangfeng
 * @license: Apache License 2.0
 */
public class ByteArraySource implements Source {

    private final byte[] data;
    private ByteArrayInputStream arrayInputStream;

    public ByteArraySource(byte[] data) {
        this.data = data;
    }

    @Override
    public int read(byte[] buffer) throws ProxyCacheException {
        return arrayInputStream.read(buffer, 0, buffer.length);
    }

    @Override
    public long length() throws ProxyCacheException {
        return data.length;
    }

    @Override
    public void open(long offset) throws ProxyCacheException {
        arrayInputStream = new ByteArrayInputStream(data);
        arrayInputStream.skip(offset);
    }

    @Override
    public void openPartial(long offset, long size) throws ProxyCacheException {
        //todo:
        arrayInputStream = new ByteArrayInputStream(data);
        arrayInputStream.skip(offset);
    }

    @Override
    public void close() throws ProxyCacheException {
    }
}

