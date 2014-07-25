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

/* Windows implementation is a simple passthrough to Windows threads. */

#include <stdio.h>
#include <windows.h>

#include "os/thread.h"

struct thread_procedure_binding {
  thread_procedure binding_start;
  LPVOID binding_arg;
};

static DWORD run_procedure(LPVOID binding) {
  struct thread_procedure_binding *run_binding = binding;
  run_binding->binding_start(run_binding->binding_arg);
  return 0;
}

int thread_create(thread *t, thread_procedure start, void *arg) {
  DWORD ret = 0;
  HANDLE h;
  struct thread_procedure_binding binding;
  binding.binding_start = start;
  binding.binding_arg = arg;
  h = CreateThread(NULL, 0, run_procedure, &binding, 0, NULL);
  if (h) {
    *t = h;
  } else {
    ret = GetLastError();
    fprintf(stderr, "thread_create: CreateThread failed with error %d\n", ret);
  }
  return ret;
}

int thread_join(thread *t) {
  DWORD ret = WaitForSingleObject(*t, INFINITE);
  switch (ret) {
  case WAIT_OBJECT_0:
    return ret;
  case WAIT_FAILED:
    ret = GetLastError();
    fprintf(stderr, "thread_join: WaitForSingleObject failed with error %d\n",
      ret);
    return ret;
  default:
    fprintf(stderr, "thread_join: WaitForSingleObject unexpected error %d\n",
      ret);
    return ret;
  }
}
