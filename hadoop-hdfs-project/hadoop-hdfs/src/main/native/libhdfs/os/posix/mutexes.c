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

/* POSIX implementation is a simple passthrough to pthreads mutexes. */

#include <pthread.h>
#include <stdio.h>

#include "os/mutexes.h"

mutex hdfsHashMutex = PTHREAD_MUTEX_INITIALIZER;
mutex jvmMutex = PTHREAD_MUTEX_INITIALIZER;

int mutex_lock(mutex *m) {
  int ret = pthread_mutex_lock(m);
  if (ret) {
    fprintf(stderr, "mutex_lock: pthread_mutex_lock failed with error %d\n",
      ret);
  }
  return ret;
}

int mutex_unlock(mutex *m) {
  int ret = pthread_mutex_unlock(m);
  if (ret) {
    fprintf(stderr, "mutex_unlock: pthread_mutex_unlock failed with error %d\n",
      ret);
  }
  return ret;
}
