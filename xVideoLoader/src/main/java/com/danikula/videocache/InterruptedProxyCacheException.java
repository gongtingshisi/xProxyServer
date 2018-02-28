package com.danikula.videocache;

/**
 * Indicates interruption error in work of {@link ProxyCache} fired by user.
 *
 * @author Alexey Danilov
 * @author zhangfeng
 * @license: Apache License 2.0
 */
public class InterruptedProxyCacheException extends ProxyCacheException {

    public InterruptedProxyCacheException(String message) {
        super(message);
    }

    public InterruptedProxyCacheException(String message, Throwable cause) {
        super(message, cause);
    }

    public InterruptedProxyCacheException(Throwable cause) {
        super(cause);
    }
}
