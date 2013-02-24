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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.StartupProgress.Phase;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

@InterfaceAudience.Private
public class StartupProgressMetrics implements MetricsSource {

  private static final MetricsInfo STARTUP_PROGRESS_METRICS_INFO =
    createMetricsInfo("StartupProgress", "NameNode startup progress");

  private final StartupProgress startupProgress;

  public StartupProgressMetrics(StartupProgress startupProgress) {
    this.startupProgress = startupProgress;
    DefaultMetricsSystem.instance().register(
      STARTUP_PROGRESS_METRICS_INFO.name(),
      STARTUP_PROGRESS_METRICS_INFO.description(), this);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    StartupProgress.View prog = startupProgress.createView();
    MetricsRecordBuilder builder = collector.addRecord(
      STARTUP_PROGRESS_METRICS_INFO);

    builder.addCounter(createMetricsInfo("ElapsedTime", "overall elapsed time"),
      prog.getElapsedTime());
    builder.addGauge(createMetricsInfo("PercentComplete",
      "overall percent complete"), prog.getPercentComplete());

    for (Phase phase: StartupProgress.getVisiblePhases()) {
      addCounter(builder, phase, "Count", " count", prog.getCount(phase));
      addCounter(builder, phase, "ElapsedTime", " elapsed time",
        prog.getElapsedTime(phase));
      addCounter(builder, phase, "Total", " total", prog.getTotal(phase));
      addGauge(builder, phase, "PercentComplete", " percent complete",
        prog.getPercentComplete(phase));
    }
  }

  private static void addCounter(MetricsRecordBuilder builder, Phase phase,
      String nameSuffix, String descSuffix, long value) {
    MetricsInfo metricsInfo = createMetricsInfo(phase.getName() + nameSuffix,
      phase.getDescription() + descSuffix);
    builder.addCounter(metricsInfo, value);
  }

  private static void addGauge(MetricsRecordBuilder builder, Phase phase,
      String nameSuffix, String descSuffix, float value) {
    MetricsInfo metricsInfo = createMetricsInfo(phase.getName() + nameSuffix,
      phase.getDescription() + descSuffix);
    builder.addGauge(metricsInfo, value);
  }

  private static MetricsInfo createMetricsInfo(final String name,
      final String desc) {
    return new MetricsInfo() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public String description() {
        return desc;
      }
    };
  }
}
