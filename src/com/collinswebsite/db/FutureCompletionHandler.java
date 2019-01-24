package com.collinswebsite.db;

import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;

/**
 * Shim for creating futures from nio async CompletionHandlers.
 */
public class FutureCompletionHandler<T> implements CompletionHandler<T, CompletableFuture<T>> {
    // specializations can be singletons, since the important part is the attachment
    private static FutureCompletionHandler<Integer> integerHandler = new FutureCompletionHandler<>();

    @Override
    public void completed(T result, CompletableFuture<T> attachment) {
        attachment.complete(result);
    }

    @Override
    public void failed(Throwable exc, CompletableFuture<T> attachment) {
        attachment.completeExceptionally(exc);
    }

    public static FutureCompletionHandler<Integer> integer() {
        return integerHandler;
    }
}
