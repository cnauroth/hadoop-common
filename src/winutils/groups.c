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
#include "winutils.h"

//----------------------------------------------------------------------------
// Function: PrintGroups
//
// Description:
//	Print group names to the console standard output for the given user
//
// Returns:
//	TRUE: on success
//
// Notes:
//
static BOOL PrintGroups(LPLOCALGROUP_USERS_INFO_0 groups, DWORD entries)
{
  BOOL ret = TRUE;
  LPLOCALGROUP_USERS_INFO_0 pTmpBuf = groups;
  DWORD i;

  for (i = 0; i < entries; i++)
  {
    if (pTmpBuf == NULL)
    {
      ret = FALSE;
      break;
    }

    if (i != 0)
    {
      wprintf(L" ");
    }
    wprintf(L"%s", pTmpBuf->lgrui0_name);

    pTmpBuf++;
  }

  if (ret)
    wprintf(L"\n");

  return ret;
}

//----------------------------------------------------------------------------
// Function: Groups
//
// Description:
//	The main method for groups command
//
// Returns:
//	0: on success
//
// Notes:
//
//
int Groups(int argc, wchar_t *argv[])
{
  LPWSTR input = NULL;

  LPWSTR currentUser = NULL;
  DWORD cchCurrentUser = 0;

  LPLOCALGROUP_USERS_INFO_0 groups = NULL;
  DWORD entries = 0;

  DWORD dwRtnCode = ERROR_SUCCESS;

  int ret = EXIT_SUCCESS;

  if (argc != 2 && argc != 1)
  {
    fwprintf(stderr, L"Incorrect command line arguments.\n\n");
    GroupsUsage(argv[0]);
    return EXIT_FAILURE;
  }

  if (argc == 1)
  {
    GetUserNameW(currentUser, &cchCurrentUser);
    if (GetLastError() == ERROR_INSUFFICIENT_BUFFER)
    {
      currentUser = (LPWSTR) LocalAlloc(LPTR,
        (cchCurrentUser + 1) * sizeof(wchar_t));
      if (!currentUser)
      {
        ReportErrorCode(L"LocalAlloc", GetLastError());
        ret = EXIT_FAILURE;
        goto GroupsEnd;
      }
      if (GetUserNameW(currentUser, &cchCurrentUser))
        input = currentUser;
      else
      {
        ReportErrorCode(L"GetUserName", GetLastError());
        ret = EXIT_FAILURE;
        goto GroupsEnd;
      }
    }
    else
    {
      ReportErrorCode(L"GetUserName", GetLastError());
      ret = EXIT_FAILURE;
      goto GroupsEnd;
    }
  }
  else
  {
    input = argv[1];
  }

  if ((dwRtnCode = GetLocalGroupsForUser(input, &groups, &entries))
    != ERROR_SUCCESS)
  {
    ReportErrorCode(L"GetLocalGroupsForUser", dwRtnCode);
    ret = EXIT_FAILURE;
    goto GroupsEnd;
  }

  if (!PrintGroups(groups, entries))
  {
    ret = EXIT_FAILURE;
  }

GroupsEnd:
  LocalFree(currentUser);
  if (groups != NULL) NetApiBufferFree(groups);
  return ret;
}

void GroupsUsage(LPCWSTR program)
{
  fwprintf(stdout, L"\
Usage: %s [USERNAME]\n\
Print group information of the specified USERNAME\
(the current user by default).\n",
program);
}
