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

//import com.coremedia.iso.IsoFile;
//import com.coremedia.iso.boxes.Box;

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
    private FileCache cache;
    private CacheListener listener;
    private boolean DEBUG = Log.isLoggable(getClass().getSimpleName(), Log.DEBUG);
    private boolean allFromCache = false;
    private static final float NO_CACHE_BARRIER = .2f;
    private static final boolean ALLOW_FAST_FORWARD_SKIP = true;
    //try my best to reuse fragment,either continue download or fast-forward.
    private static final long MAX_ALLOW_IGNORE_FAST_FORWARD_LENGTH = 1 << 10;

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
            if (!request.keyUA)
                LOG.warn("###### Cache result:" + ret + ",time:" + (System.currentTimeMillis() - time) + "ms, " + (DEBUG ? request.uri : "") + " " + request);
            else if (DEBUG && request.keyFrameRequest) {
                LOG.warn("###### Cache play result:" + ret + ",time:" + (System.currentTimeMillis() - time) + "ms, " + (DEBUG ? request.uri : "") + " " + request);
            }
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
        if (lengthKnown && request.partial && request.rangeTo == NONE) {
            request.rangeTo = sourceLength - 1;
        }
        contentLength = request.rangeTo - request.rangeOffset + 1;
        return new StringBuilder()
                .append(request.partial ? "HTTP/1.1 206 PARTIAL CONTENT\n" : "HTTP/1.1 200 OK\n")
                .append("Accept-Ranges: bytes\n")
                .append(lengthKnown ? format("Content-Length: %d\n", contentLength) : "")
                .append((lengthKnown && request.partial) ? format("Content-Range: bytes %d-%d/%d\n", request.rangeOffset, request.rangeTo, sourceLength) : "")
                .append(mimeKnown ? format("Content-Type: %s\n", mime) : "")
                .append("\n") // headers end
                .toString();
    }

    /*
    private long getMp4HeaderSize(String path) throws FileNotFoundException {
        IsoFile isoFile = null;
        File file = new File(path);
        if (!file.exists()) {
            throw new FileNotFoundException("Not find:" + path);
        }
        if (!file.canRead()) {
            throw new IllegalStateException("No read permission:" + path);
        }
        try {
            isoFile = new IsoFile(path);
            for (Box box : isoFile.getBoxes()) {
                if ("mdat".equals(box.getType())) {
                    return box.getOffset();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (isoFile != null)
                    isoFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        throw new IllegalStateException("Parse mp4 mdat fail...");
    }
    */

    private boolean response(OutputStream out, GetRequest request) {
        HttpUrlSource httpUrlSource = new HttpUrlSource(this.source);

        try {
            boolean echo = request.keyUA;
            /**
             * Robustness: it doesn't affect delivery even though file operation fails :P
             * */
            boolean error = false;
            long cacheSize = cache.available();
            long last = 0;
            long clientRead = 0;
            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            int readBytes;
            long from = request.rangeOffset;
            long to = request.rangeTo;
            long length = source.length();
            boolean ignoreSkip = from - cacheSize <= MAX_ALLOW_IGNORE_FAST_FORWARD_LENGTH || !ALLOW_FAST_FORWARD_SKIP;

            if (from == 0 && request.keyUA && to == length - 1) {
//                to = getMp4HeaderSize(cache.getFile().getAbsolutePath());
            }

            if (DEBUG)
                LOG.warn("request:[" + from + "," + to + "],cache size:" + cacheSize + ",echo:" + echo + "," + request);

            if (from >= cacheSize) {
                long offset = ignoreSkip ? cacheSize : from;

                if (DEBUG)
                    LOG.warn("request extra from " + offset + ", ignoreSkip option:" + ignoreSkip + " " + request);

                httpUrlSource.openPartial(offset, to - offset + 1);
                long start = System.currentTimeMillis();

                while ((readBytes = httpUrlSource.read(buffer)) > 0) {
                    offset += readBytes;
                    //fill cache file with header part:(offset+1,sourceLength).
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

                    if (ignoreSkip && !error) {
                        try {
                            cache.append(buffer, readBytes);
                        } catch (ProxyCacheException e) {
                            error = true;
                        }
                    }

                    if (System.currentTimeMillis() - start > 1 * 1000) {
                        speed = (clientRead - last);//instant single task speed
                        last = clientRead;
                        start = System.currentTimeMillis();
                    }
                }
            } else if (cacheSize < to && cacheSize > from) {
                //interrupt other threads write cache on a meanwhile violently.
                cache.close();
                cache = new FileCache(cache.getFile(), cache.getDiskUsage());

                if (DEBUG)
                    LOG.warn("request less from " + cacheSize + " " + request);

                //read [from,cacheSize] from cached file
                long offset = from;
                while ((readBytes = cache.read(buffer, offset, buffer.length)) > 0) {
                    offset += readBytes;
                    if (echo) {
                        out.write(buffer, 0, readBytes);
                    }
                }
                if (offset != cacheSize) {
                    LOG.warn("offset:" + offset + " != cacheSize:" + cacheSize + " " + request);
                }
                //then continue to request from offset,here offset should be equals to cache size.
                httpUrlSource.openPartial(offset, to - offset + 1);
                long start = System.currentTimeMillis();

                while ((readBytes = httpUrlSource.read(buffer)) > 0) {
                    clientRead += readBytes;
                    offset += readBytes;
                    if (echo) {
                        out.write(buffer, 0, readBytes);
                    }
                    if (!error) {
                        try {
                            cache.append(buffer, readBytes);
                        } catch (ProxyCacheException e) {
                            error = true;
                        }
                    }
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

            long size = cache.available();
            if (DEBUG) {
                LOG.warn("Current cached size:" + size + " " + request);
            }

            if (ignoreSkip && size != to + 1) {
                LOG.warn("Cache error,cache size: " + size + " !=to+1: " + (to + 1) + " " + request);
                cache.delete();
                return false;
            }

            if (!request.keyUA && size == to + 1) {
                onCachePercentsAvailableChanged(100);
            }

            if (size == length) {
                cache.complete();
            }
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

    private boolean isUseCache(GetRequest request) throws ProxyCacheException {
        long sourceLength = source.length();
        boolean sourceLengthKnown = sourceLength > 0;
        long cacheAvailable = cache.available();
        // do not use cache for partial requests which too far from available cache. It seems user seek video.
        return !sourceLengthKnown || !request.partial || request.rangeOffset <= cacheAvailable + sourceLength * NO_CACHE_BARRIER;
    }

    private void responseWithCache(OutputStream out, long offset) throws ProxyCacheException, IOException {
        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        int readBytes;
        while ((readBytes = read(buffer, offset, buffer.length)) != -1) {
            out.write(buffer, 0, readBytes);
            offset += readBytes;
        }
        out.flush();
    }

    private void responseWithoutCache(OutputStream out, long offset) throws ProxyCacheException, IOException {
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
        } finally {
            newSourceNoCache.close();
        }
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
