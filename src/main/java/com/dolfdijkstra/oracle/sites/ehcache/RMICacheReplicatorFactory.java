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

import java.util.Properties;

import net.sf.ehcache.distribution.RMISynchronousCacheReplicator;
import net.sf.ehcache.event.CacheEventListener;
import net.sf.ehcache.event.CacheEventListenerFactory;
import net.sf.ehcache.util.PropertyUtil;

/**
 * @author Dolf Dijkstra
 *
 */
public class RMICacheReplicatorFactory extends CacheEventListenerFactory {

    private static final String REPLICATE_PUTS = "replicatePuts";
    private static final String REPLICATE_PUTS_VIA_COPY = "replicatePutsViaCopy";
    private static final String REPLICATE_UPDATES = "replicateUpdates";
    private static final String REPLICATE_UPDATES_VIA_COPY = "replicateUpdatesViaCopy";
    private static final String REPLICATE_REMOVALS = "replicateRemovals";
    private static final String REPLICATE_ASYNCHRONOUSLY = "replicateAsynchronously";

    /**
     * Create a <code>CacheEventListener</code> which is also a CacheReplicator.
     * <p/>
     * The defaults if properties are not specified are:
     * <ul>
     * <li>replicatePuts=true
     * <li>replicatePutsViaCopy=true
     * <li>replicateUpdates=true
     * <li>replicateUpdatesViaCopy=true
     * <li>replicateRemovals=true;
     * <li>replicateAsynchronously=true
     * </ul>
     * 
     * @param properties implementation specific properties. These are
     *            configured as comma separated name value pairs in ehcache.xml
     *            e.g.
     *            <p/>
     *            <code>
     *                   &lt;cacheEventListenerFactory class="net.sf.ehcache.distribution.RMICacheReplicatorFactory"
     *                   properties="
     *                   replicateAsynchronously=true,
     *                   replicatePuts=true
     *                   replicateUpdates=true
     *                   replicateUpdatesViaCopy=true
     *                   replicateRemovals=true
     *                   "/&gt;</code>
     * @return a constructed CacheEventListener
     */
    public final CacheEventListener createCacheEventListener(Properties properties) {
        boolean replicatePuts = extractReplicatePuts(properties);
        boolean replicatePutsViaCopy = extractReplicatePutsViaCopy(properties);
        boolean replicateUpdates = extractReplicateUpdates(properties);
        boolean replicateUpdatesViaCopy = extractReplicateUpdatesViaCopy(properties);
        boolean replicateRemovals = extractReplicateRemovals(properties);
        boolean replicateAsynchronously = extractReplicateAsynchronously(properties);

        if (replicateAsynchronously) {
            return new RMIAsynchronousCacheReplicator(replicatePuts, replicatePutsViaCopy, replicateUpdates,
                    replicateUpdatesViaCopy, replicateRemovals);
        } else {
            return new RMISynchronousCacheReplicator(replicatePuts, replicatePutsViaCopy, replicateUpdates,
                    replicateUpdatesViaCopy, replicateRemovals);
        }
    }

    /**
     * Extracts the value of replicateAsynchronously from the properties
     * 
     * @param properties
     */
    protected boolean extractReplicateAsynchronously(Properties properties) {
        boolean replicateAsynchronously;
        String replicateAsynchronouslyString = PropertyUtil.extractAndLogProperty(REPLICATE_ASYNCHRONOUSLY, properties);
        if (replicateAsynchronouslyString != null) {
            replicateAsynchronously = PropertyUtil.parseBoolean(replicateAsynchronouslyString);
        } else {
            replicateAsynchronously = true;
        }
        return replicateAsynchronously;
    }

    /**
     * Extracts the value of replicateRemovals from the properties
     * 
     * @param properties
     */
    protected boolean extractReplicateRemovals(Properties properties) {
        boolean replicateRemovals;
        String replicateRemovalsString = PropertyUtil.extractAndLogProperty(REPLICATE_REMOVALS, properties);
        if (replicateRemovalsString != null) {
            replicateRemovals = PropertyUtil.parseBoolean(replicateRemovalsString);
        } else {
            replicateRemovals = true;
        }
        return replicateRemovals;
    }

    /**
     * Extracts the value of replicateUpdatesViaCopy from the properties
     * 
     * @param properties
     */
    protected boolean extractReplicateUpdatesViaCopy(Properties properties) {
        boolean replicateUpdatesViaCopy;
        String replicateUpdatesViaCopyString = PropertyUtil.extractAndLogProperty(REPLICATE_UPDATES_VIA_COPY,
                properties);
        if (replicateUpdatesViaCopyString != null) {
            replicateUpdatesViaCopy = PropertyUtil.parseBoolean(replicateUpdatesViaCopyString);
        } else {
            replicateUpdatesViaCopy = true;
        }
        return replicateUpdatesViaCopy;
    }

    /**
     * Extracts the value of replicatePutsViaCopy from the properties
     * 
     * @param properties
     */
    protected boolean extractReplicatePutsViaCopy(Properties properties) {
        boolean replicatePutsViaCopy;
        String replicatePutsViaCopyString = PropertyUtil.extractAndLogProperty(REPLICATE_PUTS_VIA_COPY, properties);
        if (replicatePutsViaCopyString != null) {
            replicatePutsViaCopy = PropertyUtil.parseBoolean(replicatePutsViaCopyString);
        } else {
            replicatePutsViaCopy = true;
        }
        return replicatePutsViaCopy;
    }

    /**
     * Extracts the value of replicateUpdates from the properties
     * 
     * @param properties
     */
    protected boolean extractReplicateUpdates(Properties properties) {
        boolean replicateUpdates;
        String replicateUpdatesString = PropertyUtil.extractAndLogProperty(REPLICATE_UPDATES, properties);
        if (replicateUpdatesString != null) {
            replicateUpdates = PropertyUtil.parseBoolean(replicateUpdatesString);
        } else {
            replicateUpdates = true;
        }
        return replicateUpdates;
    }

    /**
     * Extracts the value of replicatePuts from the properties
     * 
     * @param properties
     */
    protected boolean extractReplicatePuts(Properties properties) {
        boolean replicatePuts;
        String replicatePutsString = PropertyUtil.extractAndLogProperty(REPLICATE_PUTS, properties);
        if (replicatePutsString != null) {
            replicatePuts = PropertyUtil.parseBoolean(replicatePutsString);
        } else {
            replicatePuts = true;
        }
        return replicatePuts;
    }

}
