package com.danikula.videocache;

import android.text.TextUtils;

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
 */
class GetRequest {
    private static final Logger LOG = LoggerFactory.getLogger("GetRequest");

    private static final Pattern RANGE_HEADER_PATTERN = Pattern.compile("[R,r]ange:[ ]?bytes=(\\d*)-");
    private static final Pattern URL_PATTERN = Pattern.compile("GET /(.*) HTTP");
    private static final Pattern UA_PATTERN = Pattern.compile("[U,u]ser-[A,a]gent:[ ]?Lavf*");
    public String uri;
    public long rangeOffset;
    public final boolean partial;
    public boolean keyFrameRequest;

    private GetRequest(String request) {
        checkNotNull(request);
        long offset = findRangeOffset(request);
        this.rangeOffset = Math.max(0, offset);
        this.partial = offset >= 0;
        this.uri = findUri(request);
        boolean keyUA = findUA(request);
        this.keyFrameRequest = (keyUA && offset > 0);
    }

    public void setRangeOffset(long offset) {
        this.rangeOffset = offset;
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
        LOG.warn("GetRequest:" + stringRequest.toString());
        return new GetRequest(stringRequest.toString());
    }

    private long findRangeOffset(String request) {
        Matcher matcher = RANGE_HEADER_PATTERN.matcher(request);
        if (matcher.find()) {
            String rangeValue = matcher.group(1);
            return Long.parseLong(rangeValue);
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
                "rangeOffset=" + rangeOffset +
                ", partial=" + partial +
                ", keyFrameRequest=" + keyFrameRequest +
                ", uri='" + uri + '\'' +
                '}';
    }
}
