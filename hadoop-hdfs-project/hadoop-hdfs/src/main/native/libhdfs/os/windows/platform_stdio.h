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

#ifndef LIBHDFS_PLATFORM_STDIO_H
#define LIBHDFS_PLATFORM_STDIO_H

/*
 * On Windows, the stdio.h header does exist, but we also need to add some
 * definitions manually that are missing.
 */
#include <stdio.h>
#include <stdlib.h>
#include <windows.h>

#define PATH_MAX MAX_PATH

/*
 * On Windows, the preprocessor does support variadic macros, even though they
 * technically weren't defined until C99.
 */
#define snprintf(a, b, c, ...) \
  _snprintf_s((a), (b), _TRUNCATE, (c), __VA_ARGS__)
#define strncpy(a, b, c) \
  strncpy_s((a), (c), (b), _TRUNCATE)
#define strtok_r(a, b, c) \
  strtok_s((a), (b), (c))
#define vsnprintf(a, b, c, d) \
  vsnprintf_s((a), (b), _TRUNCATE, (c), (d))

#endif
