package com.danikula.videocache;

/**
 * Sometimes,we have known the response content type in advance,so there is no need to fetch it again.
 *
 * @author zhangfeng
 * @license: Apache License 2.0
 */
public interface IContentTypeProvider {
    String getMIME();
}