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

#include <stdio.h>
#include <stdlib.h>
#include <sql.h>
#include <sqlext.h>

void handleOdbcError(SQLHANDLE handle, SQLSMALLINT handleType, SQLRETURN retCode)
{
    SQLCHAR sqlState[6];
    SQLINTEGER nativeError;
    SQLCHAR errMsg[1024];
    SQLSMALLINT errMsgLen;

    SQLGetDiagRec(handleType, handle, 1, sqlState, &nativeError, errMsg, 1024, &errMsgLen);
    printf("SQL Error: %s - %d - %s\n", sqlState, nativeError, errMsg);
}

int main()
{
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLHSTMT hstmt;
    SQLRETURN ret;

    // Allocate environment handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        printf("Error allocating environment handle\n");
        return 1;
    }

    // Set environment attributes
    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        printf("Error setting environment attributes\n");
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Allocate connection handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        printf("Error allocating connection handle\n");
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Connect to the data source
    ret = SQLConnect(hdbc, (SQLCHAR *)"kwdb", SQL_NTS, (SQLCHAR *)"root", SQL_NTS, (SQLCHAR *)"123456", SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        handleOdbcError(hdbc, SQL_HANDLE_DBC, ret);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Allocate statement handle
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        printf("Error allocating statement handle\n");
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    // Execute a query
    ret = SQLExecDirect(hstmt, (SQLCHAR *)"SELECT * FROM t1", SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        printf("Error executing query\n");
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        return 1;
    }

    SQLWCHAR szBaseTableName[256];
    SQLSMALLINT cbBaseTableName;
    ret = SQLColAttribute(hstmt, 1, SQL_DESC_BASE_TABLE_NAME, szBaseTableName, sizeof(szBaseTableName), &cbBaseTableName, NULL);
    if (ret == SQL_SUCCESS)
    {
        printf("Column is from table: %s\n", szBaseTableName);
    }
    else
    {
        printf("Failed to get base table name for column \n");
    }

    // SQLLEN colCount;
    // ret = SQLNumResultCols(hstmt, &colCount);
    // if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    //     printf("Error getting number of result columns\n");
    //     SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    //     SQLDisconnect(hdbc);
    //     SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    //     SQLFreeHandle(SQL_HANDLE_ENV, henv);
    //     return 1;
    // }

    // for (int i = 1; i <= colCount; ++i) {
    //     SQLCHAR colName[256];
    //     SQLSMALLINT colNameLen;
    //     SQLSMALLINT dataType;
    //     SQLULEN colSize;
    //     SQLSMALLINT decimalDigits;
    //     SQLSMALLINT nullable;
    //     SQLCHAR baseTableName[256];
    //     SQLLEN baseTableNameLen;

    //     ret = SQLDescribeCol(hstmt, i, colName, sizeof(colName), &colNameLen, &dataType, &colSize, &decimalDigits, &nullable);
    //     if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    //         printf("Error describing column %d\n", i);
    //         continue;
    //     }

    //     // Get SQL_DESC_BASE_TABLE_NAME attribute
    //     ret = SQLColAttribute(hstmt, i, SQL_DESC_BASE_TABLE_NAME, baseTableName, sizeof(baseTableName), &baseTableNameLen, NULL);
    //     if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    //         printf("Error getting SQL_DESC_BASE_TABLE_NAME attribute for column %s\n", colName);
    //         continue;
    //     }

    //     printf("Column: %s, Base Table Name: %s\n", colName, baseTableName);
    // }

    // Cleanup
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}
