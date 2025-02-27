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

// Function to display error messages
void show_error(SQLHANDLE handle, SQLSMALLINT handle_type)
{
    SQLWCHAR sql_state[6];
    SQLINTEGER native_error;
    SQLWCHAR error_msg[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT msg_len;
    SQLGetDiagRecW(handle_type, handle, 1, sql_state, &native_error, error_msg, sizeof(error_msg), &msg_len);
    wprintf(L"SQL Error: %s - %s\n", sql_state, error_msg);
}

int main()
{
    // Initialize ODBC environment and connection handles
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLHSTMT hstmt;
    SQLRETURN retcode;

    // Allocate environment handle
    retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        wprintf(L"Error allocating environment handle\n");
        return 1;
    }

    // Set environment attribute to use ODBC 3.x
    retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        wprintf(L"Error setting environment attribute\n");
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Allocate connection handle
    retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        wprintf(L"Error allocating connection handle\n");
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }
    SQLWCHAR dsn[] = L"PostgreSQL35W";
    SQLWCHAR uid[] = L"root";
    SQLWCHAR pwd[] = L"123456";
    // Connect to the data source
    retcode = SQLConnectW(hdbc, dsn, SQL_NTS, uid, SQL_NTS, pwd, SQL_NTS);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        wprintf(L"Error connecting to data source\n");
        show_error(hdbc, SQL_HANDLE_DBC);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Allocate statement handle
    retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        wprintf(L"Error allocating statement handle\n");
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Set statement attribute to enable metadata ID
    SQLULEN metadata_id = 1; // Enable metadata ID
    retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_METADATA_ID, (SQLPOINTER)metadata_id, SQL_IS_UINTEGER);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        wprintf(L"Error setting statement attribute for metadata ID\n");
        show_error(hstmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Execute a query to retrieve metadata
    SQLWCHAR sqlStatement[] = L"SELECT * FROM t2 WHERE 1=0;";
    retcode = SQLExecDirectW(hstmt, sqlStatement, SQL_NTS);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        wprintf(L"Error executing statement to retrieve metadata\n");
        show_error(hstmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Process the result set metadata
    SQLSMALLINT num_cols;
    retcode = SQLNumResultCols(hstmt, &num_cols);
    if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO)
    {
        wprintf(L"Error getting number of result set columns\n");
        show_error(hstmt, SQL_HANDLE_STMT);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    wprintf(L"Number of result set columns: %d\n", num_cols);

    // Free resources and disconnect
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
