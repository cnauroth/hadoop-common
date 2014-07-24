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

#ifndef LIBHDFS_PLATFORM_H
#define LIBHDFS_PLATFORM_H

/*
 * Platform-specific overrides for Windows.
 */

#include <stdio.h>
#include <windows.h>
#include <winsock.h>

/*
 * O_ACCMODE defined to match Linux definition.
 */
#ifndef O_ACCMODE
#define O_ACCMODE 0x0003
#endif

/*
 * Windows has a different name for its maximum path length constant.
 */
#ifndef PATH_MAX
#define PATH_MAX MAX_PATH
#endif

/*
 * Windows does not define EDQUOT and ESTALE in errno.h.  The closest equivalents
 * are these constants from winsock.h.
 */
#ifndef EDQUOT
#define EDQUOT WSAEDQUOT
#endif

#ifndef ESTALE
#define ESTALE WSAESTALE
#endif

/*
 * gcc-style type-checked format arguments are not supported on Windows, so just
 * stub this macro.
 */
#define TYPE_CHECKED_PRINTF_FORMAT(formatArg, varArgs)

/*
 * Define macros for various string formatting functions not defined on Windows.
 * Where possible, we reroute to one of the secure CRT variants.  On Windows,
 * the preprocessor does support variadic macros, even though they weren't
 * defined until C99.
 */
#define snprintf(a, b, c, ...) \
  _snprintf_s((a), (b), _TRUNCATE, (c), __VA_ARGS__)
#define strncpy(a, b, c) \
  strncpy_s((a), (c), (b), _TRUNCATE)
#define strtok_r(a, b, c) \
  strtok_s((a), (b), (c))
#define vsnprintf(a, b, c, d) \
  vsnprintf_s((a), (b), _TRUNCATE, (c), (d))

/*
 * Mutex data type defined as CRITICAL_SECTION, not a HANDLE to a Windows mutex.
 * We only need synchronization of multiple threads within the same process, not
 * synchronization across processes, so CRITICAL_SECTION is sufficient.
 */
typedef CRITICAL_SECTION mutex;

#endif
