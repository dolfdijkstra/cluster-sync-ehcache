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

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.rmi.RemoteException;
import java.rmi.UnmarshalException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.Status;
import net.sf.ehcache.distribution.CacheManagerPeerProvider;
import net.sf.ehcache.distribution.CachePeer;
import net.sf.ehcache.distribution.EventMessage;
import net.sf.ehcache.distribution.RMISynchronousCacheReplicator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dolf Dijkstra
 * 
 */

public class RMIAsynchronousCacheReplicator extends RMISynchronousCacheReplicator {

    private static final Logger LOG = LoggerFactory.getLogger(RMIAsynchronousCacheReplicator.class.getName());
    private static final Logger TIME_LOG = LoggerFactory.getLogger(RMIAsynchronousCacheReplicator.class.getName()
            + ".time");

    /**
     * A thread which handles replication, so that replication can take place
     * asynchronously and not hold up the cache
     */
    protected Thread replicationThread = new ReplicationThread();

    /**
     * A queue of updates.
     */
    protected final LinkedBlockingQueue<CacheEventMessage> replicationQueue;

    protected final RMIStats stats = new RMIStats();
    private MBeanServer mBeanServer;
    private ObjectName jmxName;

    private ExecutorService executor;

    private boolean parallel = true;
    private boolean shootAndForget = false;

    /**
     * Constructor for internal and subclass use
     */
    public RMIAsynchronousCacheReplicator(boolean replicatePuts, boolean replicatePutsViaCopy,
            boolean replicateUpdates, boolean replicateUpdatesViaCopy, boolean replicateRemovals) {
        super(replicatePuts, replicatePutsViaCopy, replicateUpdates, replicateUpdatesViaCopy, replicateRemovals);
        status = Status.STATUS_ALIVE;

        if (parallel) {
            executor = Executors.newCachedThreadPool(new DefaultThreadFactory(getClass().getSimpleName()));

            /*
             * queue is limited to 100 to stop overflow of message. 100 might still be too high.
             */
            
            replicationQueue = new LinkedBlockingQueue<CacheEventMessage>(100);

        } else {
            replicationQueue = new LinkedBlockingQueue<CacheEventMessage>();
        }
        replicationThread.start();
    }

    void registerJmxIfNeeded(Ehcache ehcache) {
        if (mBeanServer != null)
            return;
        this.mBeanServer = ManagementFactory.getPlatformMBeanServer();
        replicationThread
                .setName("Replication Thread-" + ehcache.getCacheManager().getName() + "-" + ehcache.getName());

        try {
            jmxName = createObjectName(ehcache);
            mBeanServer.registerMBean(stats, jmxName);
        } catch (Exception e) {
            LOG.error("Exception on registring jxm bean: " + e.getMessage(), e);
        }

    }

    protected ObjectName createObjectName(Ehcache ehcache) throws MalformedObjectNameException {
        return createObjectName(ehcache.getCacheManager().getName(), ehcache.getName());
    }

    /**
     * Creates an object name using the scheme
     * "net.sf.ehcache:type=RMIClusterReplicatorStatistics,CacheManager=<cacheManagerName>,name=<cacheName>"
     */
    static ObjectName createObjectName(String cacheManagerName, String cacheName) {
        ObjectName objectName;
        try {
            objectName = new ObjectName("net.sf.ehcache:type=RMIClusterReplicatorStatistics,CacheManager="
                    + cacheManagerName + ",name=" + cacheName);
        } catch (MalformedObjectNameException e) {
            throw new CacheException(e);
        }
        return objectName;
    }

    private void replicationThreadMain() {
        while (alive()) {
            try {
                CacheEventMessage msg = replicationQueue.take();
                LinkedList<CacheEventMessage> work = new LinkedList<CacheEventMessage>();
                work.add(msg);
                while ((msg = replicationQueue.poll()) != null) {
                    work.add(msg);
                }
                flushReplicationQueue(work);
            } catch (InterruptedException e) {
                LOG.debug("Spool Thread interrupted.");
                return;
            } catch (Throwable e) {
                LOG.error("Exception on flushing of replication queue: " + e.getMessage() + ". Continuing...", e);

            }
        }
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation queues the put notification for in-order replication
     * to peers.
     * 
     * @param cache the cache emitting the notification
     * @param element the element which was just put into the cache.
     */
    public final void notifyElementPut(final Ehcache cache, final Element element) throws CacheException {
        if (notAlive()) {
            return;
        }

        if (!replicatePuts) {
            return;
        }

        if (replicatePutsViaCopy) {
            if (!element.isSerializable()) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Object with key " + element.getObjectKey()
                            + " is not Serializable and cannot be replicated.");
                }
                return;
            }
            addToReplicationQueue(new CacheEventMessage(EventMessage.PUT, cache, element, null));
        } else {
            if (!element.isKeySerializable()) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Object with key " + element.getObjectKey()
                            + " does not have a Serializable key and cannot be replicated via invalidate.");
                }
                return;
            }
            addToReplicationQueue(new CacheEventMessage(EventMessage.REMOVE, cache, null, element.getKey()));
        }

    }

    /**
     * Called immediately after an element has been put into the cache and the
     * element already existed in the cache. This is thus an update.
     * <p/>
     * The {@link net.sf.ehcache.Cache#put(net.sf.ehcache.Element)} method will
     * block until this method returns.
     * <p/>
     * Implementers may wish to have access to the Element's fields, including
     * value, so the element is provided. Implementers should be careful not to
     * modify the element. The effect of any modifications is undefined.
     * 
     * @param cache the cache emitting the notification
     * @param element the element which was just put into the cache.
     */
    public final void notifyElementUpdated(final Ehcache cache, final Element element) throws CacheException {
        if (notAlive()) {
            return;
        }
        if (!replicateUpdates) {
            return;
        }

        if (replicateUpdatesViaCopy) {
            if (!element.isSerializable()) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Object with key " + element.getObjectKey()
                            + " is not Serializable and cannot be updated via copy.");
                }
                return;
            }
            addToReplicationQueue(new CacheEventMessage(EventMessage.PUT, cache, element, null));
        } else {
            if (!element.isKeySerializable()) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Object with key " + element.getObjectKey()
                            + " does not have a Serializable key and cannot be replicated via invalidate.");
                }
                return;
            }
            addToReplicationQueue(new CacheEventMessage(EventMessage.REMOVE, cache, null, element.getKey()));
        }
    }

    /**
     * Called immediately after an attempt to remove an element. The remove
     * method will block until this method returns.
     * <p/>
     * This notification is received regardless of whether the cache had an
     * element matching the removal key or not. If an element was removed, the
     * element is passed to this method, otherwise a synthetic element, with
     * only the key set is passed in.
     * <p/>
     * 
     * @param cache the cache emitting the notification
     * @param element the element just deleted, or a synthetic element with just
     *            the key set if no element was removed.
     */
    public final void notifyElementRemoved(final Ehcache cache, final Element element) throws CacheException {
        if (notAlive()) {
            return;
        }

        if (!replicateRemovals) {
            return;
        }

        if (!element.isKeySerializable()) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Key " + element.getObjectKey() + " is not Serializable and cannot be replicated.");
            }
            return;
        }
        addToReplicationQueue(new CacheEventMessage(EventMessage.REMOVE, cache, null, element.getKey()));
    }

    /**
     * Called during {@link net.sf.ehcache.Ehcache#removeAll()} to indicate that
     * the all elements have been removed from the cache in a bulk operation.
     * The usual
     * {@link #notifyElementRemoved(net.sf.ehcache.Ehcache,net.sf.ehcache.Element)}
     * is not called.
     * <p/>
     * This notification exists because clearing a cache is a special case. It
     * is often not practical to serially process notifications where
     * potentially millions of elements have been bulk deleted.
     * 
     * @param cache the cache emitting the notification
     */
    public void notifyRemoveAll(final Ehcache cache) {
        if (notAlive()) {
            return;
        }

        if (!replicateRemovals) {
            return;
        }

        addToReplicationQueue(new CacheEventMessage(EventMessage.REMOVE_ALL, cache, null, null));
    }

    /**
     * Adds a message to the queue.
     * <p/>
     * This method checks the state of the replication thread and warns if it
     * has stopped and then discards the message.
     * 
     * @param cacheEventMessage
     */
    protected void addToReplicationQueue(CacheEventMessage cacheEventMessage) {
        if (!replicationThread.isAlive()) {
            LOG.error("CacheEventMessages cannot be added to the replication queue because the replication thread has died.");
        } else {
            replicationQueue.add(cacheEventMessage);
        }
    }

    /**
     * 
     * 
     */
    private void flushReplicationQueue(LinkedList<CacheEventMessage> messageList) {

        // TODO: filter out duplicate messages
        final Ehcache cache = messageList.peek().cache;
        registerJmxIfNeeded(cache);
        List<CachePeer> cachePeers = listRemoteCachePeers(cache);

        final List<EventMessage> resolvedEventMessages = extractAndResolveEventMessages(messageList);
        if (!resolvedEventMessages.isEmpty()) {

            int msgCount = resolvedEventMessages.size();
            int peerCount = cachePeers.size();
            stats.addPeers(peerCount);
            stats.addMessages(msgCount);
            long t0 = System.nanoTime();
            List<Future<?>> futures = new ArrayList<Future<?>>(cachePeers.size());

            for (final CachePeer cachePeer : cachePeers) {
                final Runnable callable = new Runnable() {

                    @Override
                    public void run() {
                        sendToPeer(cache, resolvedEventMessages, cachePeer);

                    }
                };

                Future<?> f = createFuture(callable);
                futures.add(f);
            }

            if (!shootAndForget) {
                for (Future<?> f : futures) {
                    try {
                        f.get();
                    } catch (InterruptedException e) {
                        break;
                    } catch (ExecutionException e) {
                        LOG.error("Exception on replication of removeNotification. " + e.getMessage()
                                + ". Continuing...", e);

                    }
                }
            }

            long elapsedTotal = (System.nanoTime() - t0) / 1000;
            stats.addElapsedAll(elapsedTotal);
            if (TIME_LOG.isDebugEnabled()) {
                TIME_LOG.debug(String.format("Sending list of %1$d messages to %2$d peers took %3$dus.", msgCount,
                        peerCount, elapsedTotal));
            }

        }
        if (LOG.isWarnEnabled()) {
            int eventMessagesNotResolved = messageList.size() - resolvedEventMessages.size();
            if (eventMessagesNotResolved > 0) {
                LOG.warn(eventMessagesNotResolved + " messages were discarded on replicate due to reclamation of "
                        + "SoftReferences by the VM. Consider increasing the maximum heap size and/or setting the "
                        + "starting heap size to a higher value.");
            }

        }
    }

    protected Future<?> createFuture(final Runnable callable) {
        if (parallel) {
            return executor.submit(callable);
        } else {

            @SuppressWarnings({ "unchecked", "rawtypes" })
            FutureTask<?> f = new FutureTask(callable, null);
            f.run();
            return f;

        }
    }

    protected void sendToPeer(Ehcache cache, List<EventMessage> messages, CachePeer cachePeer) {
        try {
            long t = System.nanoTime();

            cachePeer.send(messages);
            long elapsed = (System.nanoTime() - t) / 1000;
            stats.addElapsed(elapsed);
            if (TIME_LOG.isDebugEnabled()) {
                String url = null;
                url = getPeerUrl(cachePeer);
                TIME_LOG.debug(String.format("Sending a list of %1$d messages to peer %2$s took %3$dus.",
                        messages.size(), url, elapsed));
            }
        } catch (java.rmi.ConnectException e) {
            String url = null;
            url = getPeerUrl(cachePeer);
            LOG.debug("Unable to send message to remote peer " + url + " due to connection problems.  Message was: "
                    + e.getMessage());
            unregister(cache, cachePeer);
        } catch (java.rmi.NoSuchObjectException e) {
            LOG.debug("Unable to send message to remote peer due to NoSuchObjectException.  Message was: "
                    + e.getMessage());
            unregister(cache, cachePeer);

        } catch (UnmarshalException e) {
            String message = e.getMessage();
            String url = null;
            url = getPeerUrl(cachePeer);
            if (message.indexOf("Read time out") != 0) {
                LOG.warn("Unable to send message to remote peer " + url
                        + " due to socket read timeout. Consider increasing"
                        + " the socketTimeoutMillis setting in the cacheManagerPeerListenerFactory. " + "Message was: "
                        + e.getMessage());
            } else {
                LOG.debug("Unable to send message to remote peer " + url + ".  Message was: " + e.getMessage());
            }
        } catch (Throwable t) {
            String url = null;
            url = getPeerUrl(cachePeer);
            LOG.warn("Unable to send message to remote peer " + url + ".  Message was: " + t.getMessage(), t);
        }
    }

    protected String getPeerUrl(CachePeer cachePeer) {
        String url = null;
        try {
            url = cachePeer.getUrl();
        } catch (RemoteException e) {
            LOG.debug(e.getMessage(), e);
        }
        return url;
    }

    private void unregister(Ehcache cache, CachePeer peer) {
        String url = null;
        try {
            url = getPeerUrl(peer);
            CacheManagerPeerProvider provider = cache.getCacheManager().getCacheManagerPeerProvider("RMI");
            provider.unregisterPeer(url);
        } catch (Throwable e) {
            LOG.warn("Error during unregistring peer " + url + ": " + e.getMessage(), e);

        }

    }

    /**
     * Package protected List of cache peers
     * 
     * @param cache
     * @return a list of {@link CachePeer} peers for the given cache, excluding
     *         the local peer.
     */
    @SuppressWarnings("unchecked")
    static List<CachePeer> listRemoteCachePeers(Ehcache cache) {
        CacheManagerPeerProvider provider = cache.getCacheManager().getCacheManagerPeerProvider("RMI");
        return provider.listRemoteCachePeers(cache);
    }

    /**
     * Extracts CacheEventMessages and attempts to get a hard reference to the
     * underlying EventMessage
     * <p/>
     * If an EventMessage has been invalidated due to SoftReference collection
     * of the Element, it is not propagated. This only affects puts and updates
     * via copy.
     * 
     * @param replicationQueueCopy
     * @return a list of EventMessages which were able to be resolved
     */
    private static List<EventMessage> extractAndResolveEventMessages(List<CacheEventMessage> replicationQueueCopy) {
        LinkedList<EventMessage> list = new LinkedList<EventMessage>();
        for (CacheEventMessage e : replicationQueueCopy) {
            EventMessage eventMessage = e.getEventMessage();
            if (eventMessage != null && eventMessage.isValid()) {
                list.add(eventMessage);
            }
        }
        return list;
    }

    /**
     * A background daemon thread that writes objects to the file.
     */
    private final class ReplicationThread extends Thread {
        public ReplicationThread() {
            super("Replication Thread");
            setDaemon(true);
            setPriority(Thread.NORM_PRIORITY);
        }

        /**
         * RemoteDebugger thread method.
         */
        public final void run() {
            replicationThreadMain();
        }
    }

    /**
     * A wrapper around an EventMessage, which enables the element to be
     * enqueued along with what is to be done with it.
     * <p/>
     * The wrapper holds a {@link java.lang.ref.SoftReference} to the
     * {@link EventMessage}, so that the queue is never the cause of an
     * {@link OutOfMemoryError}
     */
    private static class CacheEventMessage {

        private final Ehcache cache;
        private final EventMessage eventMessage;

        public CacheEventMessage(int event, Ehcache cache, Element element, Serializable key) {
            eventMessage = new EventMessage(event, key, element);
            this.cache = cache;
        }

        /**
         * Gets the component EventMessage
         */
        public final EventMessage getEventMessage() {
            return eventMessage;
        }

    }

    /**
     * Give the replicator a chance to flush the replication queue, then cleanup
     * and free resources when no longer needed
     */
    public final void dispose() {
        status = Status.STATUS_SHUTDOWN;
        replicationThread.interrupt();
        try {
            replicationThread.join();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for the replication thread to finish: " + e.getMessage(), e);
        }
        if (mBeanServer != null && jmxName != null) {
            try {
                mBeanServer.unregisterMBean(jmxName);
            } catch (javax.management.InstanceNotFoundException e) {
                // ignore
            } catch (Exception e) {
                LOG.warn("Unable to unregister bean " + jmxName + " from : " + e.getMessage(), e);
            }
            jmxName = null;
            mBeanServer = null;
        }
    }

    /**
     * Creates a clone of this listener. This method will only be called by
     * ehcache before a cache is initialized.
     * <p/>
     * This may not be possible for listeners after they have been initialized.
     * Implementations should throw CloneNotSupportedException if they do not
     * support clone.
     * 
     * @return a clone
     * @throws CloneNotSupportedException if the listener could not be cloned.
     */
    public Object clone() throws CloneNotSupportedException {
        // shutup checkstyle
        super.clone();
        return new RMIAsynchronousCacheReplicator(replicatePuts, replicatePutsViaCopy, replicateUpdates,
                replicateUpdatesViaCopy, replicateRemovals);
    }

    static class DefaultThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        DefaultThreadFactory(String prefix) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = prefix + "-" + poolNumber.getAndIncrement() + "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }

}
