/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze;

import java.util.List;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.AlterTable;
import io.crate.sql.tree.AlterTableOpenClose;
import io.crate.sql.tree.AlterTableRenameTable;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.Table;

class AlterTableAnalyzer {

    private final Schemas schemas;
    private final NodeContext nodeCtx;

    AlterTableAnalyzer(Schemas schemas, NodeContext nodeCtx) {
        this.schemas = schemas;
        this.nodeCtx = nodeCtx;
    }

    AnalyzedAlterTable analyze(AlterTable<Expression> node,
                               ParamTypeHints paramTypeHints,
                               CoordinatorTxnCtx txnCtx) {
        var exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.TO_LITERAL_VALIDATE_NAME, null);

        CoordinatorSessionSettings sessionSettings = txnCtx.sessionSettings();
        var exprCtx = new ExpressionAnalysisContext(sessionSettings);
        AlterTable<Symbol> alterTable = node.map(x -> exprAnalyzerWithFieldsAsString.convert(x, exprCtx));
        TableInfo TableInfo = schemas.findRelation(
            alterTable.table().getName(),
            Operation.ALTER_BLOCKS,
            sessionSettings.sessionUser(),
            sessionSettings.searchPath()
        );

        return new AnalyzedAlterTable(TableInfo, alterTable);
    }


    AnalyzedAlterTableRenameTable analyze(AlterTableRenameTable<Expression> node, CoordinatorSessionSettings sessionSettings) {
        if (!node.table().partitionProperties().isEmpty()) {
            throw new UnsupportedOperationException("Renaming a single partition is not supported");
        }

        // we do not support renaming to a different schema, thus the target table identifier must not include a schema
        // this is an artificial limitation, technically it can be done
        List<String> newIdentParts = node.newName().getParts();
        if (newIdentParts.size() > 1) {
            throw new IllegalArgumentException("Target table name must not include a schema");
        }

        QualifiedName qName = node.blob()
            ? RelationName.fromBlobTable(node.table()).toQualifiedName()
            : node.table().getName();
        RelationInfo source = schemas.findRelation(
            qName,
            Operation.ALTER_TABLE_RENAME,
            sessionSettings.sessionUser(),
            sessionSettings.searchPath()
        );
        RelationName sourceName = source.ident();
        RelationName targetName = new RelationName(sourceName.schema(), newIdentParts.get(0));
        targetName.ensureValidForRelationCreation();
        boolean isPartitioned = source instanceof DocTableInfo docTable && docTable.isPartitioned();
        return new AnalyzedAlterTableRenameTable(sourceName, targetName, isPartitioned);
    }

    public AnalyzedAlterTableOpenClose analyze(AlterTableOpenClose<Expression> node,
                                               ParamTypeHints paramTypeHints,
                                               CoordinatorTxnCtx txnCtx) {
        var exprAnalyzerWithFieldsAsStrings = new ExpressionAnalyzer(
            txnCtx, nodeCtx, paramTypeHints, FieldProvider.TO_LITERAL_VALIDATE_NAME, null);
        CoordinatorSessionSettings sessionSettings = txnCtx.sessionSettings();
        var exprCtx = new ExpressionAnalysisContext(sessionSettings);

        Table<Symbol> table = node.table().map(x -> exprAnalyzerWithFieldsAsStrings.convert(x, exprCtx));
        QualifiedName qName = node.blob()
            ? RelationName.fromBlobTable(node.table()).toQualifiedName()
            : node.table().getName();

        Operation operation = node.openTable() ? Operation.ALTER_OPEN : Operation.ALTER_CLOSE;
        DocTableInfo tableInfo = schemas.findRelation(
            qName,
            operation,
            sessionSettings.sessionUser(),
            sessionSettings.searchPath()
        );
        return new AnalyzedAlterTableOpenClose(tableInfo, table, node.openTable());
    }
}
