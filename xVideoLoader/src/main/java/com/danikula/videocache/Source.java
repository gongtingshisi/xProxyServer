package com.danikula.videocache;

/**
 * Source for proxy.
 *
 * @author Alexey Danilov (danikula@gmail.com).
 */
public interface Source {

    /**
     * Opens source. Source should be open before using {@link #read(byte[])}
     *
     * @param offset offset in bytes for source.
     * @throws ProxyCacheException if error occur while opening source.
     */
    void open(long offset) throws ProxyCacheException;

    /**
     * Opens source. Source should be open before using {@link #read(byte[])}.
     * 和{@link #open(long)}不同，这个适用于读取前offset字节，而非range offset
     *
     * @param offset offset in socket
     * @param size   size in bytes from offset.
     * @throws ProxyCacheException if error occur while opening source.
     */
    void openPartial(long offset, long size) throws ProxyCacheException;

    /**
     * Returns length bytes or <b>negative value</b> if length is unknown.
     *
     * @return bytes length
     * @throws ProxyCacheException if error occur while fetching source data.
     */
    long length() throws ProxyCacheException;

    /**
     * Read data to byte buffer from source with current offset.
     *
     * @param buffer a buffer to be used for reading data.
     * @return a count of read bytes
     * @throws ProxyCacheException if error occur while reading source.
     */
    int read(byte[] buffer) throws ProxyCacheException;

    /**
     * Closes source and release resources. Every opened source should be closed.
     *
     * @throws ProxyCacheException if error occur while closing source.
     */
    void close() throws ProxyCacheException;
}
