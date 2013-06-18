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
/**
 * @author Dolf Dijkstra
 *
 */

public interface RMIStatsMBean {

    long getMessageCount();

    long getMessagesSendCount();

    long getAverageSendTime();
    
    long getAverageAllPeersSendTime();

    int getMaxNumOfPeers();

    int getMinNumOfPeers();

    int getLastNumOfPeers();

    void reset();

}
