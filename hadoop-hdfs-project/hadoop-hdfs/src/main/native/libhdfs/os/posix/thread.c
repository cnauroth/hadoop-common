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

struct thread_procedure_binding {
  thread_procedure binding_start;
  void *binding_arg;
};

static void* run_procedure(void *binding) {
  struct thread_procedure_binding *run_binding = binding;
  run_binding->binding_start(run_binding->binding_arg);
  return NULL;
}

int thread_create(thread *t, thread_procedure start, void *arg) {
  int ret;
  struct thread_procedure_binding binding;
  binding.binding_start = start;
  binding.binding_arg = arg;
  ret = pthread_create(t, NULL, run_procedure, &binding);
  if (ret) {
    fprintf(stderr, "thread_create: pthread_create failed with error %d\n", ret);
  }
  return ret;
}

int thread_join(thread *t) {
  int ret = pthread_join(*t, NULL);
  if (ret) {
    fprintf(stderr, "thread_join: pthread_join failed with error %d\n", ret);
  }
  return ret;
}
