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

#include <jni.h>
#include <stdio.h>
#include <windows.h>

#include "os/thread_local_storage.h"

/** Key that allows us to retrieve thread-local storage */
static DWORD gTlsIndex = TLS_OUT_OF_INDEXES;

static void detach_current_thread_from_jvm(JNIEnv *env)
{
  JavaVM *vm;
  jint ret;

  ret = (*env)->GetJavaVM(env, &vm);
  if (ret) {
    fprintf(stderr,
      "detach_current_thread_from_jvm: GetJavaVM failed with error %d\n",
      ret);
    (*env)->ExceptionDescribe(env);
  } else {
    (*vm)->DetachCurrentThread(vm);
  }
}

static void NTAPI tls_callback(PVOID h, DWORD reason, PVOID pv)
{
  switch (reason) {
  case DLL_PROCESS_ATTACH:
    fprintf(stderr, "tls_callback: DLL_PROCESS_ATTACH\n");
    break;
  case DLL_THREAD_ATTACH:
    fprintf(stderr, "tls_callback: DLL_THREAD_ATTACH\n");
    break;
  case DLL_THREAD_DETACH:
    fprintf(stderr, "tls_callback: DLL_THREAD_DETACH\n");
    break;
  case DLL_PROCESS_DETACH:
    fprintf(stderr, "tls_callback: DLL_PROCESS_DETACH\n");
    break;
  }
}
#pragma const_seg(".CRT$XLB")
extern const PIMAGE_TLS_CALLBACK pTlsCallback;
const PIMAGE_TLS_CALLBACK pTlsCallback = tls_callback;
#pragma const_seg()

int thread_local_storage_get(JNIEnv **env)
{
  LPVOID tls;
  if (TLS_OUT_OF_INDEXES == gTlsIndex) {
    gTlsIndex = TlsAlloc();
    if (TLS_OUT_OF_INDEXES == gTlsIndex) {
      fprintf(stderr,
        "thread_local_storage_get: TlsAlloc failed with error %d\n",
        TLS_OUT_OF_INDEXES);
      return TLS_OUT_OF_INDEXES;
    }
  }
  tls = TlsGetValue(gTlsIndex);
  /*
   * According to documentation, TlsGetValue cannot fail as long as the index is
   * a valid index from a successful TlsAlloc call.  This error handling is
   * purely defensive.
   */
  if (!tls) {
    fprintf(stderr, "thread_local_storage_get: TlsGetValue failed\n");
    return -1;
  }
  *env = tls;
  return 0;
}

int thread_local_storage_set(JNIEnv *env)
{
  DWORD ret = 0;
  if (!TlsSetValue(gTlsIndex, (LPVOID)env)) {
    ret = GetLastError();
    fprintf(stderr,
      "thread_local_storage_set: TlsSetValue failed with error %d\n",
      ret);
    detach_current_thread_from_jvm(env);
  }
  return ret;
}
