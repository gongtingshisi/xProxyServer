package com.danikula.videocache;

import android.text.TextUtils;
import android.util.Log;

import com.danikula.videocache.file.FileCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.util.Locale;

import static com.danikula.videocache.HttpProxyCacheServer.NONE;
import static com.danikula.videocache.ProxyCacheUtils.DEFAULT_BUFFER_SIZE;

/**
 * {@link ProxyCache} that read http url and writes data to {@link Socket}
 *
 * @author Alexey Danilov (danikula@gmail.com).
 * @author zhangfeng
 * @license: Apache License 2.0
 */
class HttpProxyCache extends ProxyCache {
    private static final Logger LOG = LoggerFactory.getLogger("HttpProxyCache");
    private final HttpUrlSource source;
    private final FileCache cache;
    private CacheListener listener;
    private HttpUrlSource httpUrlSource;
    private boolean DEBUG = Log.isLoggable(getClass().getSimpleName(), Log.DEBUG);
    private boolean allFromCache = false;

    public long getSpeed() {
        return speed;
    }

    public HttpProxyCache(HttpUrlSource source, FileCache cache) {
        super(source, cache);
        this.cache = cache;
        this.source = source;
    }

    public void registerCacheListener(CacheListener cacheListener) {
        this.listener = cacheListener;
    }

    public boolean processRequest(GetRequest request, Socket socket) {
        long time = System.currentTimeMillis();
        boolean ret = false;
        try {
            OutputStream out = new BufferedOutputStream(socket.getOutputStream());
            String responseHeaders = newResponseHeaders(request);
            if (DEBUG) {
                LOG.warn(" \n\n" + request.uri + "\n" + responseHeaders);
            }
            out.write(responseHeaders.getBytes("UTF-8"));

            return ret = response(out, request);

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            LOG.warn("###### cache result:" + ret + ",time:" + (System.currentTimeMillis() - time) + "ms, " + (DEBUG ? request.uri : ""));
        }
        return ret;
    }

    private String newResponseHeaders(GetRequest request) throws IOException, ProxyCacheException {
        LOG.warn("\n" + source);

        String mime = source.getMime();
        boolean mimeKnown = !TextUtils.isEmpty(mime);
        long sourceLength = source.length();
        long contentLength;
        boolean lengthKnown = sourceLength >= 0;
        if (lengthKnown && request.rangeTo >= sourceLength) {
            throw new IllegalArgumentException("Request range to:" + request.rangeTo + " is greater than source length:" + sourceLength);
        }
        if (request.partial && request.rangeTo == NONE) {
            request.rangeTo = sourceLength - 1;
        }
        contentLength = request.rangeTo - request.rangeOffset + 1;
        return new StringBuilder()
                .append(request.partial ? "HTTP/1.1 206 PARTIAL CONTENT\n" : "HTTP/1.1 200 OK\n")
                .append("Accept-Ranges: bytes\n")
                .append(lengthKnown ? format("Content-Length: %d\n", contentLength) : "")
                .append(lengthKnown ? format("Content-Range: bytes %d-%d/%d\n", request.rangeOffset, request.rangeTo, sourceLength) : "")
                .append(mimeKnown ? format("Content-Type: %s\n", mime) : "")
                .append("\n") // headers end
                .toString();
    }

    private boolean response(OutputStream out, GetRequest request) {
        try {
            if (httpUrlSource != null) {
                httpUrlSource.close();
            }

            boolean echo = request.keyUA;
            long cacheSize = cache.available();
            long last = 0;
            long clientRead = 0;
            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            int readBytes;
            long from = request.rangeOffset;
            long to = request.rangeTo;
            httpUrlSource = new HttpUrlSource(this.source);

            if (DEBUG)
                LOG.warn("request:[" + from + "," + to + "],cache size:" + cacheSize + ",echo:" + echo + "," + request);
            if (from >= cacheSize) {
                //we still request (cacheSize,sourceLength)
                long offset = cacheSize;

                if (DEBUG)
                    LOG.warn("request from " + cacheSize + " " + request);

                httpUrlSource.openPartial(cacheSize, to - cacheSize + 1);
                long start = System.currentTimeMillis();

                while ((readBytes = httpUrlSource.read(buffer)) > 0) {
                    offset += readBytes;
                    //fill cache file with header part:(cacheSize+1,sourceLength).
                    if (offset > from) {
                        if (offset - readBytes < from) {
                            if (echo)
                                out.write(buffer, (int) (readBytes - (offset - from)), (int) (offset - from));
                            clientRead += (offset - from);
                        } else {
                            if (echo)
                                out.write(buffer, 0, readBytes);
                            clientRead += readBytes;
                        }
                    }
                    cache.append(buffer, readBytes);

                    if (System.currentTimeMillis() - start > 1 * 1000) {
                        speed = (clientRead - last);//instant single task speed
                        last = clientRead;
                        start = System.currentTimeMillis();
                    }
                }
            } else if (cacheSize < to && cacheSize > from) {
                if (DEBUG)
                    LOG.warn("request from " + cacheSize + " " + request);

                //read [from,cacheSize] from cached file
                long offset = from;
                while ((readBytes = cache.read(buffer, offset, buffer.length)) > 0) {
                    if (echo) {
                        out.write(buffer, 0, readBytes);
                    }
                    offset += readBytes;
                }
                if (offset != cacheSize) {
                    throw new IllegalStateException("offset != cacheSize");
                }
                //then continue to request from offset,here offset should be equals to cache size.
                httpUrlSource.openPartial(offset, to - offset + 1);
                long start = System.currentTimeMillis();

                while ((readBytes = httpUrlSource.read(buffer)) > 0) {
                    clientRead += readBytes;
                    if (echo) {
                        out.write(buffer, 0, readBytes);
                    }
                    offset += readBytes;
                    cache.append(buffer, readBytes);
                    if (System.currentTimeMillis() - start > 1 * 1000) {
                        speed = (clientRead - last);//instant single task speed
                        last = clientRead;
                        start = System.currentTimeMillis();
                    }
                }
            } else if (echo) {
                if (DEBUG)
                    LOG.warn("from cache " + request);
                //read from local cached file
                allFromCache = true;
                long offset = from;
                while ((readBytes = cache.read(buffer, offset, buffer.length)) > 0) {
                    if (offset < to) {
                        out.write(buffer, 0, readBytes);
                    } else {
                        out.write(buffer, 0, (int) (offset - to));
                        break;
                    }
                    offset += readBytes;
                }
            }
            out.flush();
            onCachePercentsAvailableChanged(100);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                httpUrlSource.close();
            } catch (ProxyCacheException e1) {
                e1.printStackTrace();
            }
        }
        return false;
    }

    private String format(String pattern, Object... args) {
        return String.format(Locale.US, pattern, args);
    }

    @Override
    protected void onCacheAvailable(long cacheAvailable, long sourceLength) {
        super.onCacheAvailable(cacheAvailable, sourceLength);
    }

    @Override
    protected void onCachePercentsAvailableChanged(int percents) {
        if (listener != null) {
            listener.onCacheAvailable(source.getTitle(), cache.file, source.getUrl(), percents, allFromCache);
        }
    }
}
