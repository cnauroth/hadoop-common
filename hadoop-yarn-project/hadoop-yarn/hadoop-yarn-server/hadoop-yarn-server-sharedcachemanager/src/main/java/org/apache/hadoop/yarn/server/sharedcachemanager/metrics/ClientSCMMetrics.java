/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.sharedcachemanager.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * This class is for maintaining  client requests metrics
 * and publishing them through the metrics interfaces.
 */
@Private
@Unstable
@Metrics(about="Client SCM metrics", context="yarn")
public class ClientSCMMetrics {

  private static final Log LOG = LogFactory.getLog(ClientSCMMetrics.class);
  final MetricsRegistry registry;

  ClientSCMMetrics() {
    registry = new MetricsRegistry("clientRequests");
    LOG.debug("Initialized " + registry);
  }

  enum Singleton {
    INSTANCE;

    ClientSCMMetrics impl;

    synchronized ClientSCMMetrics init(Configuration conf) {
      if (impl == null) {
        impl = create();
      }
      return impl;
    }
  }

  public static ClientSCMMetrics initSingleton(Configuration conf) {
    return Singleton.INSTANCE.init(conf);
  }

  public static ClientSCMMetrics getInstance() {
    ClientSCMMetrics topMetrics = Singleton.INSTANCE.impl;
    if (topMetrics == null) {
      throw new IllegalStateException(
          "The ClientSCMMetrics singleton instance is not initialized."
          + " Have you called init first?");
    }
    return topMetrics;
  }

  static ClientSCMMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();

    ClientSCMMetrics metrics = new ClientSCMMetrics();
    ms.register("clientRequests", null, metrics);
    return metrics;
  }

  @Metric("Number of cache hits") MutableCounterLong cacheHits;
  @Metric("Number of cache misses") MutableCounterLong cacheMisses;
  @Metric("Number of cache releases") MutableCounterLong cacheReleases;

  /**
   * One cache hit event
   */
  public void incCacheHitCount() {
    cacheHits.incr();
  }

  /**
   * One cache miss event
   */
  public void incCacheMissCount() {
    cacheMisses.incr();
  }

  /**
   * One cache release event
   */
  public void incCacheRelease() {
    cacheReleases.incr();
  }

  public long getCacheHits() { return cacheHits.value(); }
  public long getCacheMisses() { return cacheMisses.value(); }
  public long getCacheReleases() { return cacheReleases.value(); }

}
