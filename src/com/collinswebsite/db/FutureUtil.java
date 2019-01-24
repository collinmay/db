package com.collinswebsite.db;

import java.util.concurrent.CompletableFuture;

public class FutureUtil {
    public static <T> CompletableFuture<T> exceptionalFuture(Throwable t) {
        CompletableFuture<T> f = new CompletableFuture<T>();
        f.completeExceptionally(t);
        return f;
    }
}
