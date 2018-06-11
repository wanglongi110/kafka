/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import scala.collection.JavaConverters;


public class MetadataCacheBenchmark {
    private volatile boolean running = true;

    int numBrokers = 150;
    int numTopics = 3500;
    int maxPartitionsPerTopic = 100;
    int replicationFactor = 2;
    int numUpdaters = 1;
    double updateRateLimit = 10.0; //qps
    int numReaders = 5;
    boolean partialUpdate = true;

    private final ListenerName listener = new ListenerName("listener");

    private final AtomicLong updateCounter = new AtomicLong();
    private final AtomicLong readCounter = new AtomicLong();

    //@Test
    public void benchmarkAllTheThings() throws Exception {
        //long seed = System.currentTimeMillis();
        long seed = 666;
        System.err.println("seed is " + seed);
        Random r = new Random(seed);

        MetadataCache cache = new MetadataCache(666);
        UpdateMetadataRequest fullRequest = buildRequest(r, -1);
        UpdateMetadataRequest partialRequest = buildRequest(r, 1);

        cache.updateCache(0, fullRequest); //initial data (useful in case there are no writers)

        Set<String> allTopics = new HashSet<>();
        for (int i = 0; i < numTopics; i++) {
            allTopics.add("topic-" + i);
        }
        scala.collection.mutable.Set<String> topicsScalaSet = JavaConverters.asScalaSetConverter(allTopics).asScala();

        Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                running = false;
                System.err.println("thread " + t + " died");
                e.printStackTrace(System.err);
                System.exit(1);
            }
        };
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < numUpdaters; i++) {
            UpdateMetadataRequest req = partialUpdate ? partialRequest : fullRequest;

            Runnable updaterRunnable;
            if (updateRateLimit > 0) {
                updaterRunnable = new RateLimitedUpdateRunnable(updateRateLimit, cache, req);
            } else {
                updaterRunnable = new UpdateRunnable(cache, req);
            }
            Thread updaterThread = new Thread(updaterRunnable, "updater-" + i);
            updaterThread.setDaemon(true);
            updaterThread.setUncaughtExceptionHandler(exceptionHandler);
            threads.add(updaterThread);
        }

        for (int i = 0; i < numReaders; i++) {
            ReadRunnable readRunnable = new ReadRunnable(cache, topicsScalaSet);
            Thread readerThread = new Thread(readRunnable, "reader-" + i);
            readerThread.setDaemon(true);
            readerThread.setUncaughtExceptionHandler(exceptionHandler);
            threads.add(readerThread);
        }

        for (Thread t : threads) {
            t.start();
        }

        long prevTime = System.currentTimeMillis();
        long prevUpdates = 0;
        long prevReads = 0;

        long now;
        long updates;
        long reads;

        long timeDiff;
        long updateDiff;
        long readDiff;

        double updateQps;
        double readQps;

        while (running) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(30));
            now = System.currentTimeMillis();
            updates = updateCounter.longValue();
            reads = readCounter.longValue();

            timeDiff = now - prevTime;
            updateDiff = updates - prevUpdates;
            readDiff = reads - prevReads;

            updateQps = ((double) updateDiff * 1000) / timeDiff;
            readQps = ((double) readDiff * 1000) / timeDiff;

            prevTime = now;
            prevUpdates = updates;
            prevReads = reads;

            System.err.println("updates: " + updateQps + " / sec");
            System.err.println("reads: " + readQps + " / sec");
        }
    }

    private UpdateMetadataRequest buildRequest(Random random, int numTopicsOverride) {
        int controllerEpoch = 0;
        int totalPartitions = 0;
        Set<UpdateMetadataRequest.Broker> liveBrokers = new HashSet<>();

        for (int i = 0; i < numBrokers; i++) {
            UpdateMetadataRequest.EndPoint endPoint =
                new UpdateMetadataRequest.EndPoint("host-" + i, 6666, SecurityProtocol.PLAINTEXT, listener);
            UpdateMetadataRequest.Broker broker =
                new UpdateMetadataRequest.Broker(i, Collections.singletonList(endPoint), "rack-" + i);
            liveBrokers.add(broker);
        }

        Map<TopicPartition, UpdateMetadataRequest.PartitionState> partitions = new HashMap<>();

        int topicCount = numTopicsOverride > 0 ? numTopicsOverride : numTopics;

        for (int i = 0; i < topicCount; i++) {
            String topicName = "topic-" + i;
            int numPartitions = 1 + random.nextInt(maxPartitionsPerTopic);
            for (int j = 0; j < numPartitions; j++) {
                TopicPartition tp = new TopicPartition(topicName, j);
                List<Integer> replicas = pick(replicationFactor, numBrokers, random);
                UpdateMetadataRequest.PartitionState state =
                    new UpdateMetadataRequest.PartitionState(controllerEpoch, replicas.get(0), 0, replicas, 0, replicas,
                        Collections.<Integer>emptyList());
                partitions.put(tp, state);
            }
            totalPartitions += numPartitions;
        }

        UpdateMetadataRequest.Builder builder =
            new UpdateMetadataRequest.Builder((short) 4, 0, controllerEpoch, partitions, liveBrokers);

        UpdateMetadataRequest request = builder.build((short) 4);
        System.err.println("request has " + totalPartitions + " TPs total");
        return request;
    }

    private List<Integer> pick(int howMany, int from, Random random) {
        List<Integer> result = new ArrayList<>(howMany);
        while (result.size() < howMany) {
            int chosen = random.nextInt(from); //exclusive
            if (!result.contains(chosen)) {
                result.add(chosen);
            }
        }
        return result;
    }

    private class UpdateRunnable implements Runnable {
        private final MetadataCache cache;
        private final UpdateMetadataRequest request;
        private int counter = 0;

        public UpdateRunnable(MetadataCache cache, UpdateMetadataRequest request) {
            this.cache = cache;
            this.request = request;
        }

        @Override
        public void run() {
            while (running) {
                cache.updateCache(counter++, request);
                updateCounter.incrementAndGet();
            }
        }
    }

    private class RateLimitedUpdateRunnable implements Runnable {
        private final MetadataCache cache;
        private final UpdateMetadataRequest request;
        private int counter = 0;

        private final double targetQps;
        private final int intervalMillis;

        public RateLimitedUpdateRunnable(double targetQps, MetadataCache cache, UpdateMetadataRequest request) {
            this.cache = cache;
            this.request = request;
            this.targetQps = targetQps;
            this.intervalMillis = (int) (1000.0 / targetQps);
        }

        @Override
        public void run() {
            while (running) {
                long start = System.currentTimeMillis();
                cache.updateCache(counter++, request);
                long end = System.currentTimeMillis();
                updateCounter.incrementAndGet();
                long took = end - start;
                long remaining = intervalMillis - took;
                if (remaining > 0) {
                    try {
                        Thread.sleep(remaining);
                    } catch (Exception e) {
                        e.printStackTrace(System.err);
                    }
                }
            }
        }
    }

    private class ReadRunnable implements Runnable {
        private final MetadataCache cache;
        private final scala.collection.mutable.Set<String> topics;

        public ReadRunnable(MetadataCache cache, scala.collection.mutable.Set<String> topics) {
            this.cache = cache;
            this.topics = topics;
        }

        @Override
        public void run() {
            while (running) {
                cache.getTopicMetadata(topics, listener, false);
                readCounter.incrementAndGet();
            }
        }
    }
}
