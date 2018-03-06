package com.danikula.videocache;

import android.text.TextUtils;
import android.util.Log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.danikula.videocache.Preconditions.checkNotNull;

/**
 * Model for Http GET request.
 *
 * @author Alexey Danilov (danikula@gmail.com).
 * @author zhangfeng
 * @license: Apache License 2.0
 */
class GetRequest {
    private static final Logger LOG = LoggerFactory.getLogger("GetRequest");

    private static final Pattern RANGE_HEADER_PATTERN = Pattern.compile("[R,r]ange:[ ]?bytes=(\\d*)-(\\d*)");
    private static final Pattern URL_PATTERN = Pattern.compile("GET /(.*) HTTP");
    private static final Pattern UA_PATTERN = Pattern.compile("[U,u]ser-[A,a]gent:[ ]?Lavf*");

    /**
     * request url
     */
    public String uri;
    /**
     * range offset in http header
     */
    public long rangeOffset;
    /**
     * range to in http header,-1 mean no this segment
     */
    public long rangeTo;
    /**
     * whether the range of request continues from offset >= 0
     */
    public final boolean partial;
    /**
     * whether this is a key frame request(range>0) from player
     */
    public boolean keyFrameRequest;
    /**
     * whether this is a request from player
     */
    public boolean keyUA;
    private static boolean DEBUG = Log.isLoggable("GetRequest", Log.DEBUG);

    private GetRequest(String request) {
        checkNotNull(request);
        this.rangeOffset = findRangeOffset(request);
        this.rangeTo = findRangeTo(request);
        this.partial = rangeOffset >= 0;
        this.uri = findUri(request);
        this.keyUA = findUA(request);
        this.keyFrameRequest = (keyUA && rangeOffset > 0);
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public static GetRequest read(InputStream inputStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
        StringBuilder stringRequest = new StringBuilder();
        String line;
        while (!TextUtils.isEmpty(line = reader.readLine())) { // until new line (headers ending)
            stringRequest.append(line).append('\n');
        }
        if (DEBUG)
            LOG.warn("GetRequest:" + stringRequest.toString());
        return new GetRequest(stringRequest.toString());
    }

    private long findRangeOffset(String request) {
        Matcher matcher = RANGE_HEADER_PATTERN.matcher(request);
        if (matcher.find()) {
            String rangeValue = matcher.group(1);
            return Long.parseLong(rangeValue);
        }
        return 0;
    }

    private long findRangeTo(String request) {
        try {
            Matcher matcher = RANGE_HEADER_PATTERN.matcher(request);
            if (matcher.find()) {
                String rangeValue = matcher.group(2);
                return Long.parseLong(rangeValue);
            }
        } catch (NumberFormatException e) {

        }
        return -1;
    }

    private String findUri(String request) {
        Matcher matcher = URL_PATTERN.matcher(request);
        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new IllegalArgumentException("Invalid request `" + request + "`: url not found!");
    }

    private boolean findUA(String request) {
        return UA_PATTERN.matcher(request).find();
    }

    @Override
    public String toString() {
        return "GetRequest{" +
                "uri='" + uri + '\'' +
                ", rangeOffset=" + rangeOffset +
                ", rangeTo=" + rangeTo +
                ", partial=" + partial +
                ", keyFrameRequest=" + keyFrameRequest +
                ", keyUA=" + keyUA +
                '}';
    }
}
