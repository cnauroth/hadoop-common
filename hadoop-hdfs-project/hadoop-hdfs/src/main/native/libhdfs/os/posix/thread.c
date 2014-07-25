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

/* POSIX implementation is a simple passthrough to pthreads threads. */

#include "os/thread.h"

#include <pthread.h>
#include <stdio.h>

/*
 * Define a helper structure and function that adapts function pointer provided
 * by caller to the type required by pthread_create.
 */
struct threadProcedureBinding {
  threadProcedure bindingStart;
  void *bindingArg;
};

static void* runProcedure(void *binding) {
  struct threadProcedureBinding *runBinding = binding;
  runBinding->bindingStart(runBinding->bindingArg);
  return NULL;
}

int threadCreate(thread *t, threadProcedure start, void *arg) {
  int ret;
  struct threadProcedureBinding binding;
  binding.bindingStart = start;
  binding.bindingArg = arg;
  ret = pthread_create(t, NULL, runProcedure, &binding);
  if (ret) {
    fprintf(stderr, "threadCreate: pthread_create failed with error %d\n", ret);
  }
  return ret;
}

int threadJoin(thread *t) {
  int ret = pthread_join(*t, NULL);
  if (ret) {
    fprintf(stderr, "threadJoin: pthread_join failed with error %d\n", ret);
  }
  return ret;
}
