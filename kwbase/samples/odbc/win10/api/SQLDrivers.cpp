// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>

int main()
{
    SQLHENV henv;
    SQLRETURN retcode;
    SQLSMALLINT fDirection = SQL_FETCH_FIRST;
    SQLWCHAR DriverDesc[256];
    SQLSMALLINT DescLength;
    SQLSMALLINT BufferLen = sizeof(DriverDesc) / sizeof(DriverDesc[0]);

    // alloc handle
    retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (SQL_SUCCESS != retcode)
    {
        printf("SQLAllocHandle(ENV) failed\n");
        return 1;
    }

    // set odbc version 3.x
    retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (SQL_SUCCESS != retcode)
    {
        printf("SQLSetEnvAttr failed\n");
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }


    while (SQL_SUCCESS == (retcode = SQLDriversW(henv, fDirection, DriverDesc, BufferLen, &DescLength, NULL, 0, NULL)))
    {
        wprintf(L"Driver Description: %s\n", DriverDesc);
        fDirection = SQL_FETCH_NEXT;
    }
    if (SQL_NO_DATA == retcode)
    {
        wprintf(L"No more drivers to list.\n");
    }
    else
    {
        // handle error
        printf("SQLDriversW failed\n");
    }

    // clear
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
