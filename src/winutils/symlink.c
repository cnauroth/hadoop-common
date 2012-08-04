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

#include "common.h"

/* Adopted from MSDN at:
 * http://msdn.microsoft.com/en-us/library/windows/hardware/ff552012.aspx
 * Windows Driver Kit is required to use the oroginal data structure.
 */
#define MOUNT_POINT_REPARSE_HEADER_SIZE 8
typedef struct _MOUNT_POINT_REPARSE_DATA_BUFFER {
  ULONG  ReparseTag;
  USHORT ReparseDataLength;
  USHORT Reserved;
  USHORT SubstituteNameOffset;
  USHORT SubstituteNameLength;
  USHORT PrintNameOffset;
  USHORT PrintNameLength;
  WCHAR PathBuffer[1];
} MOUNT_POINT_REPARSE_DATA_BUFFER, *PMOUNT_POINT_REPARSE_DATA_BUFFER;


//----------------------------------------------------------------------------
// Function: EnablePrivilege
//
// Description:
//	Check if the process has the given privilege. If yes, enable the privilege
//  to the process's access token.
//
// Returns:
//	TRUE: on success
//
// Notes:
//
static BOOL EnablePrivilege(__in LPCWSTR privilegeName)
{
  HANDLE hToken;
  TOKEN_PRIVILEGES tp;
  DWORD dwErrCode;

  if (!OpenProcessToken(GetCurrentProcess(),
    TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY, &hToken))
  {
    ReportErrorCode(L"OpenProcessToken", GetLastError());
    return FALSE;
  }

  tp.PrivilegeCount = 1;
  if (!LookupPrivilegeValueW(NULL,
    privilegeName, &(tp.Privileges[0].Luid)))
  {
    ReportErrorCode(L"LookupPrivilegeValue", GetLastError());
    CloseHandle(hToken);
    return FALSE;
  }
  tp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;

  // As stated on MSDN, we need to use GetLastError() to check if
  // AdjustTokenPrivileges() adjusted all of the specified privileges.
  //
  AdjustTokenPrivileges(hToken, FALSE, &tp, 0, NULL, NULL);
  dwErrCode = GetLastError();
  CloseHandle(hToken);

  return dwErrCode == ERROR_SUCCESS;
}

//----------------------------------------------------------------------------
// Function: CreateJunctionPoint
//
// Description:
//	Create a junction point to the target.
//
// Returns:
//	TRUE: on success
//
// Notes:
//
static BOOL CreateJunctionPointW(
  __in  LPWSTR lpLinkFileName,
  __in  LPWSTR lpTargetFileName)
{
  HANDLE hDir = INVALID_HANDLE_VALUE;
  size_t fileNameSize = 0;

  LPWSTR fullFileName = NULL;
  DWORD fullFileNameSize = 0;

  LPWSTR substituteName = NULL;
  DWORD substituteNameSize = 0;
  
  PMOUNT_POINT_REPARSE_DATA_BUFFER rd = NULL;
  DWORD dwSize = 0;

  BOOL bRet = FALSE;

  // The substitue name for the junction structure must contain the path
  // prefixed with the "non-parsed" prefix "\??\", and terminated with the
  // backslash character, for example "\??\C:\Users\".
  //
  static LPCWSTR targetPrefx = L"\\??\\";
  static WCHAR targetSuffix = L'\\';

  fullFileNameSize = GetFullPathName(lpTargetFileName, 0, NULL, NULL);
  if(fullFileNameSize == 0)
  {
    ReportErrorCode(L"GetFullPathName", GetLastError());
    goto CreateJunctionPointWEnd;
  }

  // The size returned inlcuding NULL terminiator; excluding NULL terminator in
  // size to align with the rest string sizes.
  //
  fullFileNameSize--;
  if ((fullFileName = (LPWSTR) LocalAlloc(LPTR,
    (fullFileNameSize + 1) * sizeof(WCHAR))) == NULL)
  {
    ReportErrorCode(L"LocalAlloc", GetLastError());
    return FALSE;
  }
  if (GetFullPathName(lpTargetFileName,
    fullFileNameSize + 1, fullFileName, NULL) == 0)
  {
    ReportErrorCode(L"GetFullPathName", GetLastError());
    goto CreateJunctionPointWEnd;
  }

  // New size = filename size + length of prefx + length of sufffix (optional),
  // excluding NULL terminator
  //
  substituteNameSize = fullFileNameSize + 4;
  if (fullFileName[fullFileNameSize] != targetSuffix)
    substituteNameSize++;

  if ((substituteName = (LPWSTR) LocalAlloc(LPTR,
    (substituteNameSize + 1) * sizeof(WCHAR))) == NULL)
  {
    ReportErrorCode(L"LocalAlloc", GetLastError());
    return FALSE;
  }

  // Build substitue name string
  //
  if (FAILED(StringCchCopy(substituteName, substituteNameSize + 1, targetPrefx)))
    goto CreateJunctionPointWEnd;
  if (FAILED(StringCchCat(substituteName, substituteNameSize + 1, fullFileName)))
    goto CreateJunctionPointWEnd;
  if (fullFileName[fullFileNameSize] != targetSuffix)
    substituteName[substituteNameSize - 1] = targetSuffix;

  // Obtain target file name length; target file is used as the print name
  //
  if (FAILED(StringCchLengthW(lpTargetFileName, STRSAFE_MAX_CCH, &fileNameSize)))
    goto CreateJunctionPointWEnd;
  
  if ((rd = (PMOUNT_POINT_REPARSE_DATA_BUFFER) LocalAlloc(LPTR,
    MAXIMUM_REPARSE_DATA_BUFFER_SIZE)) == NULL)
  {
    ReportErrorCode(L"LocalAlloc", GetLastError());
    goto CreateJunctionPointWEnd;
  }

  // Create a reparse point structure following the MSND description:
  // http://msdn.microsoft.com/en-us/library/cc232007
  //
  rd->ReparseTag = IO_REPARSE_TAG_MOUNT_POINT;
  rd->Reserved = 0;

  rd->SubstituteNameOffset = 0;

  rd->SubstituteNameLength =
    (USHORT) substituteNameSize * sizeof(WCHAR);

  rd->PrintNameOffset =
    rd->SubstituteNameLength + sizeof(WCHAR);

  rd->PrintNameLength =
    (USHORT) fileNameSize * sizeof(WCHAR);

  // From MSDN: size of the PathBuffer field, in bytes, plus 8
  //
  rd->ReparseDataLength = (USHORT)
    rd->PrintNameLength + sizeof(WCHAR) +
    rd->SubstituteNameLength + sizeof(WCHAR) + 8;

  if (rd->ReparseDataLength + MOUNT_POINT_REPARSE_HEADER_SIZE <=
    (USHORT)(MAXIMUM_REPARSE_DATA_BUFFER_SIZE / sizeof(WCHAR)))
  {
    if (FAILED(StringCchCopy(
      rd->PathBuffer +
      rd->SubstituteNameOffset / sizeof(WCHAR),
      substituteNameSize + 1, substituteName)))
      goto CreateJunctionPointWEnd;
    if(FAILED(StringCchCopy(
      rd->PathBuffer +
      rd->PrintNameOffset / sizeof(WCHAR),
      fileNameSize + 1, lpTargetFileName)))
      goto CreateJunctionPointWEnd;
  }
  else
  {
    goto CreateJunctionPointWEnd;
  }

  // Create the directory for the junction point. CreateFile() cannot create
  // a directory. If the program is terminated later before the clean up code
  // at end of the function is reached, we will end up with a empty directory.
  //
  if (!CreateDirectory(lpLinkFileName, NULL))
  {
    ReportErrorCode(L"CreateDirectory", GetLastError());
    goto CreateJunctionPointWEnd;
  }

  hDir = CreateFile(lpLinkFileName,
    GENERIC_WRITE,
    FILE_SHARE_READ | FILE_SHARE_WRITE,
    NULL,
    OPEN_EXISTING,
    FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS,
    NULL);
  if (hDir == INVALID_HANDLE_VALUE)
  {
    ReportErrorCode(L"CreateFile", GetLastError());
    goto CreateJunctionPointWEnd;
  }

  if (!DeviceIoControl(hDir, FSCTL_SET_REPARSE_POINT, rd,
    rd->ReparseDataLength + MOUNT_POINT_REPARSE_HEADER_SIZE,
    NULL, 0, &dwSize, NULL))
  {
    ReportErrorCode(L"DeviceIoControl", GetLastError());
    goto CreateJunctionPointWEnd;
  }

  bRet = TRUE;

CreateJunctionPointWEnd:
  if (hDir != INVALID_HANDLE_VALUE)
  {
    CloseHandle(hDir);
    if (!bRet)
    {
      // Remove the directory in case of failure
      if (!RemoveDirectory(lpLinkFileName))
        ReportErrorCode(L"RemoveDirectory", GetLastError());
    }
  }
  LocalFree(fullFileName);
  LocalFree(substituteName);
  LocalFree(rd);
  return bRet;
}

//----------------------------------------------------------------------------
// Function: Symlink
//
// Description:
//	The main method for symlink command
//
// Returns:
//	0: on success
//
// Notes:
//
int Symlink(int argc, wchar_t *argv[])
{
  PWSTR longLinkName = NULL;
  PWSTR longFileName = NULL;
  DWORD dwErrorCode = ERROR_SUCCESS;

  BOOL isDir = FALSE;

  DWORD dwRtnCode = ERROR_SUCCESS;
  DWORD dwFlag = 0;

  int ret = SUCCESS;

  if (argc != 3)
  {
    SymlinkUsage();
    return FAILURE;
  }

  dwErrorCode = ConvertToLongPath(argv[1], &longLinkName);
  if (dwErrorCode != ERROR_SUCCESS)
  {
    ret = FAILURE;
    goto SymlinkEnd;
  }
  dwErrorCode = ConvertToLongPath(argv[2], &longFileName);
  if (dwErrorCode != ERROR_SUCCESS)
  {
    ret = FAILURE;
    goto SymlinkEnd;
  }

  // Check if the the process's access token has the privilege to create
  // symbolic links. Without this step, the call to CreateSymbolicLink() from
  // users have the privilege to create symbolic links will still succeed.
  // This is just an additional step to do the privilege check by not using
  // error code from CreateSymbolicLink() method.
  //
  if (!EnablePrivilege(L"SeCreateSymbolicLinkPrivilege"))
  {
    fwprintf(stderr,
      L"No privilege to create symbolic links.\n");
    ret = SYMLINK_NO_PRIVILEGE;
    goto SymlinkEnd;
  }

  if ((dwRtnCode = DirectoryCheck(longFileName, &isDir)) != ERROR_SUCCESS)
  {
    ReportErrorCode(L"DirectoryCheck", dwRtnCode);
    ret = FAILURE;
    goto SymlinkEnd;
  }

  if (isDir)
    dwFlag = SYMBOLIC_LINK_FLAG_DIRECTORY;

  if (!CreateSymbolicLinkW(longLinkName, longFileName, dwFlag))
  {
    ReportErrorCode(L"CreateSymbolicLink", GetLastError());
    ret = FAILURE;
    goto SymlinkEnd;
  }

SymlinkEnd:
  LocalFree(longLinkName);
  LocalFree(longFileName);
  return ret;
}

void SymlinkUsage()
{
    fwprintf(stdout, L"\
Usage: symlink [LINKNAME] [FILENAME]\n\
Creates a symbolic link\n\
\n\
0 is returned on success.\n\
2 is returned if the user does no have privilege to create symbolic links.\n\
1 is returned for all other errors.\n\
\n\
The default security settings in Windows disallow non-elevated administrators\n\
and all non-administrators from creating symbolic links. The security settings\n\
for symbolic links can be changed in the Local Security Policy management\n\
console.\n");
}

