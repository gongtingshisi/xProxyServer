package com.danikula.videocache;

import android.text.TextUtils;

import com.danikula.videocache.file.FileCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.util.Locale;

import static com.danikula.videocache.ProxyCacheUtils.DEFAULT_BUFFER_SIZE;

/**
 * {@link ProxyCache} that read http url and writes data to {@link Socket}
 *
 * @author Alexey Danilov (danikula@gmail.com).
 */
class HttpProxyCache extends ProxyCache {
    private static final Logger LOG = LoggerFactory.getLogger("HttpProxyCache");

    private static final float NO_CACHE_BARRIER = .2f;

    private final HttpUrlSource source;
    private final FileCache cache;
    private CacheListener listener;

    public HttpProxyCache(HttpUrlSource source, FileCache cache) {
        super(source, cache);
        this.cache = cache;
        this.source = source;
    }

    public void registerCacheListener(CacheListener cacheListener) {
        this.listener = cacheListener;
    }

    public boolean processRequest(GetRequest request, Socket socket, long requestSize, boolean continuePartial) {
        try {
            OutputStream out = new BufferedOutputStream(socket.getOutputStream());
            String responseHeaders = newResponseHeaders(request, requestSize, request.keyFrameRequest, continuePartial);
            LOG.warn("continuePartial:" + continuePartial + ", response:" + responseHeaders + ", " + source);
            out.write(responseHeaders.getBytes("UTF-8"));

            long offset = request.rangeOffset;
            //todo:if we play the in-preloading state task,cancel it,pick up downloaded part.but,we will resolve file stubs related.
            if (continuePartial && cache.getFile().exists() && cache.isCompleted()) {
                return responseInsertCache(out, offset, cache.available() == source.length());
            }
            if (isUseCache(request)) {
                if (requestSize == Integer.MIN_VALUE) {
                    return responseWithCache(out, offset);
                } else {
                    return responseWithCache(out, offset, requestSize);
                }
            } else {
                return responseWithoutCache(out, offset);
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ProxyCacheException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean isUseCache(GetRequest request) throws ProxyCacheException {
        long sourceLength = source.length();
        boolean sourceLengthKnown = sourceLength > 0;
        long cacheAvailable = cache.available();
        // do not use cache for partial requests which too far from available cache. It seems user seek video.
        return !sourceLengthKnown || !request.partial || (!request.keyFrameRequest && request.rangeOffset <= cacheAvailable + sourceLength * NO_CACHE_BARRIER);
    }

    private String newResponseHeaders(GetRequest request, long requestSize, boolean requestKeyFrame, boolean continuePartial) throws IOException, ProxyCacheException {
        LOG.warn("request:" + request + " ,requestSize:" + requestSize + " ,cache.available:" + cache.available() + " ,source.length:" + source.length() + " ,continuePartial:" + continuePartial);
        LOG.warn("\n" + source);
        if (requestSize != Integer.MIN_VALUE) {
            String mime = source.getMime(requestSize);
            boolean mimeKnown = !TextUtils.isEmpty(mime);
            long length = cache.isCompleted() ? cache.available() : source.length();
            boolean lengthKnown = length >= 0;
            long contentLength = request.partial ? requestSize : length;
            boolean addRange = lengthKnown && request.partial;
            return new StringBuilder()
                    .append(request.partial ? "HTTP/1.1 206 PARTIAL CONTENT\n" : "HTTP/1.1 200 OK\n")
                    .append("Accept-Ranges: bytes\n")
                    .append(lengthKnown ? format("Content-Length: %d\n", contentLength) : "")
                    .append(addRange ? format("Content-Range: bytes %d-%d/%d\n", request.rangeOffset, request.rangeOffset + requestSize - 1, length) : "")
                    .append(mimeKnown ? format("Content-Type: %s\n", mime) : "")
                    .append("\n") // headers end
                    .toString();
        } else if (continuePartial) {
            String mime = source.getMime();
            boolean mimeKnown = !TextUtils.isEmpty(mime);
            long length = source.length();
            boolean lengthKnown = length >= 0;
            boolean addRange = lengthKnown && request.partial;
            return new StringBuilder()
                    .append(request.partial ? "HTTP/1.1 206 PARTIAL CONTENT\n" : "HTTP/1.1 200 OK\n")
                    .append("Accept-Ranges: bytes\n")
                    .append(lengthKnown ? format("Content-Length: %d\n", length /*contentLength*/) : "")
                    .append(addRange ? format("Content-Range: bytes %d-%d/%d\n", 0/*request.rangeOffset*/, length/* request.rangeOffset + contentLength*/, length) : "")
                    .append(mimeKnown ? format("Content-Type: %s\n", mime) : "")
                    .append("\n") // headers end
                    .toString();
        } else if (requestKeyFrame) {
            String mime = source.getMime();
            boolean mimeKnown = !TextUtils.isEmpty(mime);
            long length = source.length();
            boolean lengthKnown = length >= 0;
            long contentLength = source.length() - request.rangeOffset;
            boolean addRange = lengthKnown && request.partial;
            return new StringBuilder()
                    .append(request.partial ? "HTTP/1.1 206 PARTIAL CONTENT\n" : "HTTP/1.1 200 OK\n")
                    .append("Accept-Ranges: bytes\n")
                    .append(lengthKnown ? format("Content-Length: %d\n", contentLength) : "")
                    .append(addRange ? format("Content-Range: bytes %d-%d/%d\n", request.rangeOffset, source.length(), source.length()) : "")
                    .append(mimeKnown ? format("Content-Type: %s\n", mime) : "")
                    .append("\n") // headers end
                    .toString();
        } else {
            String mime = source.getMime();
            boolean mimeKnown = !TextUtils.isEmpty(mime);
            long length = cache.isCompleted() ? cache.available() : source.length();
            boolean lengthKnown = length >= 0;
            long contentLength = request.partial ? source.length() : length;
            boolean addRange = lengthKnown && request.partial;
            return new StringBuilder()
                    .append(request.partial ? "HTTP/1.1 206 PARTIAL CONTENT\n" : "HTTP/1.1 200 OK\n")
                    .append("Accept-Ranges: bytes\n")
                    .append(lengthKnown ? format("Content-Length: %d\n", contentLength) : "")
                    .append(addRange ? format("Content-Range: bytes %d-%d/%d\n", request.rangeOffset, contentLength, length) : "")
                    .append(mimeKnown ? format("Content-Type: %s\n", mime) : "")
                    .append("\n") // headers end
                    .toString();
        }
    }

    private boolean responseWithCache(OutputStream out, long offset) {
        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        int readBytes;
        try {
            while ((readBytes = read(buffer, offset, buffer.length)) != -1) {
                out.write(buffer, 0, readBytes);
                offset += readBytes;
            }
            out.flush();
            return true;
        } catch (ProxyCacheException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean responseWithCache(OutputStream out, long offset, long requestSize) {
        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        int readBytes;
        try {
            while ((readBytes = read(buffer, offset, buffer.length, requestSize)) != -1) {
                out.write(buffer, 0, readBytes);
                offset += readBytes;
            }
            out.flush();
            return true;
        } catch (Exception e) {
            HandyUtil.handle("responseWithCache offset:" + offset + " with requestSize: " + requestSize, e);
            return false;
        }
    }

    private boolean responseWithoutCache(OutputStream out, long offset) {
        HttpUrlSource newSourceNoCache = new HttpUrlSource(this.source);
        try {
            newSourceNoCache.open((int) offset);
            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            int readBytes;
            while ((readBytes = newSourceNoCache.read(buffer)) != -1) {
                out.write(buffer, 0, readBytes);
                offset += readBytes;
            }
            out.flush();
            return true;
        } catch (ProxyCacheException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                newSourceNoCache.close();
            } catch (ProxyCacheException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    private boolean responseInsertCache(OutputStream out, long offset, boolean allInLocal) {
        HttpUrlSource newSourceNoCache = new HttpUrlSource(this.source);
        try {
            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            int readBytes;
            long write = 0;
            long start = System.currentTimeMillis();
            //if we had downloaded whole file,just use it.
            while ((readBytes = cache.read(buffer, write, buffer.length)) > 0 && write < offset) {
                out.write(buffer, 0, readBytes);
                write += readBytes;
            }
            out.flush();
            LOG.warn("\n\n#####loaded from file success size:" + write + ",time:" + (System.currentTimeMillis() - start) + " ##### " + source);
            start = System.currentTimeMillis();
            if (allInLocal && write != offset) {
                throw new IllegalStateException();
            }

            if (!allInLocal) {
                newSourceNoCache.open((int) offset);
                LOG.warn("\n\n#####load from server :" + write + " ##### " + source);
                while ((readBytes = newSourceNoCache.read(buffer)) > 0) {
                    out.write(buffer, 0, readBytes);
                    write += readBytes;
                    cache.append(buffer, readBytes);
                }
                out.flush();
                cache.complete();
            }
            LOG.warn("\n\n#####loaded all success,in total:" + write + ",time:" + (System.currentTimeMillis() - start) + " ##### " + source);
            return true;
        } catch (ProxyCacheException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                newSourceNoCache.close();
            } catch (ProxyCacheException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    private String format(String pattern, Object... args) {
        return String.format(Locale.US, pattern, args);
    }

    @Override
    protected void onCachePercentsAvailableChanged(int percents) {
        if (listener != null) {
            listener.onCacheAvailable(cache.file, source.getUrl(), percents);
        }
    }
}
