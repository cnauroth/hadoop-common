/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class Step implements Comparable<Step> {
  private static final AtomicInteger SEQUENCE = new AtomicInteger();

  private final String file;
  private final int sequenceNumber;
  private final Long size;
  private final StepType type;

  public Step(StepType type) {
    this(type, null, null);
  }

  public Step(String file, long size) {
    this(null, file, size);
  }

  public Step(StepType type, String file) {
    this(type, file, null);
  }

  public Step(StepType type, String file, Long size) {
    this.file = file;
    this.sequenceNumber = SEQUENCE.incrementAndGet();
    this.size = size;
    this.type = type;
  }

  @Override
  public int compareTo(Step other) {
    return new CompareToBuilder().append(file, other.file)
      .append(sequenceNumber, other.sequenceNumber).toComparison();
  }

  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == null || otherObj.getClass() != getClass()) {
      return false;
    }
    Step other = (Step)otherObj;
    return new EqualsBuilder().append(this.file, other.file)
      .append(this.size, other.size).append(this.type, other.type).isEquals();
  }

  public String getFile() {
    return file;
  }

  public Long getSize() {
    return size;
  }

  public StepType getType() {
    return type;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(file).append(size).append(type)
      .toHashCode();
  }
}
