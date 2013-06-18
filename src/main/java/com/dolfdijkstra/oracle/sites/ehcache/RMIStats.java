/*
 * Copyright 2010 Dolf Dijkstra. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dolfdijkstra.oracle.sites.ehcache;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
/**
 * @author Dolf Dijkstra
 *
 */

public class RMIStats implements RMIStatsMBean {

    private AtomicLong messageCount = new AtomicLong();

    private volatile Avg averageSendTime = new Avg();
    private volatile Avg averageTotal = new Avg();

    private AtomicInteger maxNumOfPeers = new AtomicInteger();

    private AtomicInteger minNumOfPeers = new AtomicInteger(Integer.MAX_VALUE);

    private AtomicInteger lastNumOfPeers = new AtomicInteger();

    @Override
    public long getMessageCount() {
        return messageCount.get();
    }

    @Override
    public long getAverageSendTime() {
        return Math.round(averageSendTime.avg);
    }

    @Override
    public int getMaxNumOfPeers() {
        return maxNumOfPeers.get();
    }

    @Override
    public int getMinNumOfPeers() {

        return minNumOfPeers.get();
    }

    @Override
    public int getLastNumOfPeers() {
        return lastNumOfPeers.get();
    }

    void addPeers(int num) {
        int max = Math.max(maxNumOfPeers.get(), num);
        int min = Math.min(minNumOfPeers.get(), num);
        minNumOfPeers.set(min);
        maxNumOfPeers.set(max);
        lastNumOfPeers.set(num);

    }

    void addMessages(int msgCount) {
        messageCount.addAndGet(msgCount);

    }

    void addElapsed(long elapsed) {
        averageSendTime.add(elapsed);

    }

    @Override
    public synchronized void reset() {
        messageCount.set(0);

        averageSendTime = new Avg();
        averageTotal = new Avg();

        maxNumOfPeers.set(0);

        minNumOfPeers.set(0);

        lastNumOfPeers.set(0);

    }

    @Override
    public long getMessagesSendCount() {
        return averageSendTime.count.get();
    }

    public void addElapsedAll(long elapsedTotal) {
        averageTotal.add(elapsedTotal);

    }

    @Override
    public long getAverageAllPeersSendTime() {
        return Math.round(averageTotal.avg);
    }

}
