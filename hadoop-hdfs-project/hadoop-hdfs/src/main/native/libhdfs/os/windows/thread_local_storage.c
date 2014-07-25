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

#include "os/thread_local_storage.h"

#include <jni.h>
#include <stdio.h>
#include <windows.h>

/** Key that allows us to retrieve thread-local storage */
static DWORD gTlsIndex = TLS_OUT_OF_INDEXES;

static void detachCurrentThreadFromJvm(JNIEnv *env)
{
  JavaVM *vm;
  jint ret;

  ret = (*env)->GetJavaVM(env, &vm);
  if (ret) {
    fprintf(stderr,
      "detachCurrentThreadFromJvm: GetJavaVM failed with error %d\n",
      ret);
    (*env)->ExceptionDescribe(env);
  } else {
    (*vm)->DetachCurrentThread(vm);
  }
}

/*
 * Unlike pthreads, the Windows API does not seem to provide a convenient way to
 * hook a callback onto thread shutdown.  However, the Windows portable
 * executable format does define a concept of thread-local storage callbacks.
 * Here, we define a function and instruct the linker to set a pointer to that
 * function in the segment for thread-local storage callbacks.  See page 85 of
 * Microsoft Portable Executable and Common Object File Format Specification:
 * http://msdn.microsoft.com/en-us/gg463119.aspx
 */
static void NTAPI tlsCallback(PVOID h, DWORD reason, PVOID pv)
{
  switch (reason) {
  case DLL_PROCESS_ATTACH:
    fprintf(stderr, "tlsCallback: DLL_PROCESS_ATTACH\n");
    break;
  case DLL_THREAD_ATTACH:
    fprintf(stderr, "tlsCallback: DLL_THREAD_ATTACH\n");
    break;
  case DLL_THREAD_DETACH:
    fprintf(stderr, "tlsCallback: DLL_THREAD_DETACH\n");
    break;
  case DLL_PROCESS_DETACH:
    fprintf(stderr, "tlsCallback: DLL_PROCESS_DETACH\n");
    break;
  }
}
#pragma const_seg(".CRT$XLB")
extern const PIMAGE_TLS_CALLBACK pTlsCallback;
const PIMAGE_TLS_CALLBACK pTlsCallback = tlsCallback;
#pragma const_seg()

int threadLocalStorageGet(JNIEnv **env)
{
  LPVOID tls;
  if (TLS_OUT_OF_INDEXES == gTlsIndex) {
    gTlsIndex = TlsAlloc();
    if (TLS_OUT_OF_INDEXES == gTlsIndex) {
      fprintf(stderr,
        "threadLocalStorageGet: TlsAlloc failed with error %d\n",
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
    fprintf(stderr, "threadLocalStorageGet: TlsGetValue failed\n");
    return -1;
  }
  *env = tls;
  return 0;
}

int threadLocalStorageSet(JNIEnv *env)
{
  DWORD ret = 0;
  if (!TlsSetValue(gTlsIndex, (LPVOID)env)) {
    ret = GetLastError();
    fprintf(stderr,
      "threadLocalStorageSet: TlsSetValue failed with error %d\n",
      ret);
    detachCurrentThreadFromJvm(env);
  }
  return ret;
}
