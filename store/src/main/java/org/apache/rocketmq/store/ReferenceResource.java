/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {
    protected final AtomicLong refCount = new AtomicLong(1);
    protected volatile boolean available = true;
    protected volatile boolean cleanupOver = false;
    private volatile long firstShutdownTimestamp = 0;

    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    public void shutdown(final long intervalForcibly) {
        // 初次调用时 this.available 为 true
        if (this.available) {
            this.available = false;
            // 设置初次关闭的时间戳
            this.firstShutdownTimestamp = System.currentTimeMillis();
            // 尝试释放资源
            // release 只有在引用次数小于 1 的情况下才会释放资源
            // >>>>>>>>>
            this.release();
        // 如果引用次数大于 0
        } else if (this.getRefCount() > 0) {
            // 如果已经超过了其最大拒绝存活期
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                // 每执行一次，将引用数减少 1000
                this.refCount.set(-1000 - this.getRefCount());
                // 尝试释放资源
                // release 只有在引用次数小于 1 的情况下才会释放资源
                this.release();
            }
        }
    }

    public void release() {
        // 将引用次数减1
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;

        synchronized (this) {
            // 如果引用数小于等于 0，则执行 cleanup 方法
            // >>>>>>>>>
            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
