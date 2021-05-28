/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.proxy.backend.text.transaction;

import org.apache.shardingsphere.proxy.backend.communication.jdbc.connection.BackendConnection;
import org.apache.shardingsphere.proxy.backend.communication.jdbc.transaction.BackendTransactionManager;
import org.apache.shardingsphere.proxy.backend.response.header.ResponseHeader;
import org.apache.shardingsphere.proxy.backend.response.header.update.UpdateResponseHeader;
import org.apache.shardingsphere.proxy.backend.text.TextProtocolBackendHandler;
import org.apache.shardingsphere.sql.parser.sql.common.statement.tcl.*;

import java.sql.SQLException;

/**
 * Do transaction operation.
 */
public final class TransactionBackendHandler implements TextProtocolBackendHandler {

    private final TCLStatement tclStatement;

    private final BackendTransactionManager backendTransactionManager;

    private final boolean isInTransaction;

    public TransactionBackendHandler(final TCLStatement tclStatement, final BackendConnection backendConnection) {
        this.tclStatement = tclStatement;
        backendTransactionManager = new BackendTransactionManager(backendConnection);
        this.isInTransaction = backendConnection.getTransactionStatus().isInTransaction();
    }

    @Override
    public ResponseHeader execute() throws SQLException {
        if (tclStatement instanceof BeginTransactionStatement) {
            backendTransactionManager.begin();
        }
        if (tclStatement instanceof SetAutoCommitStatement) {
            SetAutoCommitStatement setAutoCommitStatement = (SetAutoCommitStatement) tclStatement;
            if (setAutoCommitStatement.isAutoCommit() && isInTransaction) {
                backendTransactionManager.commit();
            }
            if (!setAutoCommitStatement.isAutoCommit()) {
                backendTransactionManager.begin();
            }
        }
        if (tclStatement instanceof CommitStatement) {
            backendTransactionManager.commit();
        }
        if (tclStatement instanceof RollbackStatement) {
            backendTransactionManager.rollback();
        }
        if (tclStatement instanceof SavepointStatement) {
            backendTransactionManager.setSavepoint(((SavepointStatement) tclStatement).getSavepointName());
        }
        if (tclStatement instanceof ReleaseSavepointStatement) {
            backendTransactionManager.releaseSavepoint(((ReleaseSavepointStatement) tclStatement).getSavepointName());
        }
        if (tclStatement instanceof RollbackToSavepointStatement) {
            backendTransactionManager.rollbackTo(((RollbackToSavepointStatement) tclStatement).getSavepointName());
        }
        return new UpdateResponseHeader(tclStatement);
    }
}
