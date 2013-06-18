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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MBeanServer;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.management.ManagementService;
/**
 * @author Dolf Dijkstra
 *
 */

public class RMITester {
    public static final String INSTANCE_KEY = ManagementFactory.getRuntimeMXBean().getName() + new Random().nextInt();

    public static void main(final String... a) throws InterruptedException, NumberFormatException, IOException {

        final AtomicBoolean alive = new AtomicBoolean(true);

        if (a.length > 1) {
            final ServerSocket socket = new ServerSocket(Integer.parseInt(a[1]));
            Thread t = new Thread() {

                @Override
                public void run() {
                    try {

                        Socket s = socket.accept();
                        // s.getInputStream();
                        s.close();
                        socket.close();
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        alive.set(false);
                    }

                }

            };
            t.setDaemon(true);
            t.start();
        }

        final CacheManager mgr = new CacheManager(a[0] + ".xml");
        Thread t = new Thread("Shutdown hook") {

            @Override
            public void run() {
                System.out.println("shutting down from " + Thread.currentThread().getName());
                mgr.shutdown();

            }

        };

        Runtime.getRuntime().addShutdownHook(t);

        mgr.setName(a[0]);
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        ManagementService registry = new ManagementService(mgr, mBeanServer, true, true, true, true);

        registry.init();
        Cache notifier = mgr.getCache("notifier");
        Random r = new Random();
        int i = 0;
        try {
            while (alive.get()) {
                Thread.sleep(r.nextInt(300));
                notifier.put(new Element("FOO", System.currentTimeMillis()));
                notifier.flush();// persist it
                i++;
                notifier.remove(INSTANCE_KEY); // notify the invalidation to
                                               // others

            }
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            System.out.println("Messages send: " + i);
            registry.dispose();
            t.run();
            Runtime.getRuntime().removeShutdownHook(t);
        }
    }

}
