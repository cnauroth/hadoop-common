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

#ifndef LIBHDFS_MUTEXES_H
#define LIBHDFS_MUTEXES_H

/*
 * Defines abstraction over platform-specific threads.
 */

#include "platform.h"

/** Pointer to function to run in thread. */
typedef void (*threadProcedure)(void *);

/**
 * Creates and immediately starts a new thread.
 *
 * @param t thread to create
 * @param start starting address of procedure to run in thread
 * @param arg argument to pass to thread procedure
 * @return 0 if successful, non-zero otherwise
 */
int threadCreate(thread *t, threadProcedure start, void *arg);

/**
 * Joins to the given thread, blocking if necessary.
 *
 * @param t thread to join
 * @return 0 if successful, non-zero otherwise
 */
int threadJoin(thread *t);

#endif
