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

package io.crate.planner.node.ddl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import io.crate.execution.ddl.tables.AddColumnRequest;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.AnalyzedAlterTableAddColumn;
import io.crate.analyze.AnalyzedColumnDefinition;
import io.crate.analyze.AnalyzedTableElements;
import io.crate.analyze.BoundAddColumn;
import io.crate.analyze.SymbolEvaluator;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.CheckConstraint;
import io.crate.types.ObjectType;

import static io.crate.analyze.AnalyzedTableElements.toMapping;

public class AlterTableAddColumnPlan implements Plan {

    private final AnalyzedAlterTableAddColumn alterTable;

    public AlterTableAddColumnPlan(AnalyzedAlterTableAddColumn alterTable) {
        this.alterTable = alterTable;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) throws Exception {
        if (plannerContext.clusterState().getNodes().getMinNodeVersion().onOrAfter(Version.V_5_1_0)) {
            // validate and resolve expressions/analyzer settings
            AnalyzedTableElements<Object> tableElementsEvaluated = validate(
                alterTable,
                plannerContext.transactionContext(),
                dependencies.nodeContext(),
                params,
                subQueryResults
            );
            AnalyzedTableElements.validateAndBuildSettings(
                tableElementsEvaluated, dependencies.fulltextAnalyzerResolver());
            AnalyzedTableElements.finalizeAndValidate(
                alterTable.tableInfo().ident(),
                alterTable.analyzedTableElementsWithExpressions(),
                tableElementsEvaluated
            );


            var addColumnRequest = createRequest(alterTable.tableInfo(), tableElementsEvaluated);
            dependencies.alterTableOperation().executeAlterTableAddColumn(addColumnRequest)
                .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
        } else {
            BoundAddColumn stmt = bind(
                alterTable,
                plannerContext.transactionContext(),
                dependencies.nodeContext(),
                params,
                subQueryResults,
                dependencies.fulltextAnalyzerResolver());


            dependencies.alterTableOperation().executeAlterTableAddColumn(stmt)
                .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
        }
    }

    @VisibleForTesting
    public static AddColumnRequest createRequest(DocTableInfo docTableInfo, AnalyzedTableElements<Object> tableElementsEvaluated) {
        assert tableElementsEvaluated.columns().size() == 1 : "Add column must contain exactly one column";
        AnalyzedColumnDefinition colToAdd = tableElementsEvaluated.columns().get(0);

        // Reference creation is similar to AnalyzedTableElements.buildReference()
        // but cannot use it as it passes fixed values for some params: ColumnPolicy.DYNAMIC, IndexType.PLAIN, nullable:true, hasDocValues:false
        // which cannot be constant in case of add column.

        DataType<?> type = colToAdd.dataType() == null ? DataTypes.UNDEFINED : colToAdd.dataType();
        DataType<?> realType = ArrayType.NAME.equals(colToAdd.collectionType())
            ? new ArrayType<>(type)
            : type;

        Reference ref = new SimpleReference(
            new ReferenceIdent(docTableInfo.ident(), colToAdd.ident()),
            RowGranularity.DOC, // column in ADD COLUMN cannot be PARTITIONED BY, only one option is possible.
            realType,
            colToAdd.objectType(), // Relevant only if added column has OBJECT type
            colToAdd.indexConstraint() != null ? colToAdd.indexConstraint() : IndexType.NONE, // cannot be null as streaming Reference would fail. redundant, we have it in AddColumnRequest column properties
            !colToAdd.hasNotNullConstraint(),
            !AnalyzedColumnDefinition.docValuesSpecifiedAndDisabled(colToAdd), // redundant, we have it in AddColumnRequest column properties
            colToAdd.position, // redundant, we have it in AddColumnRequest column properties
            null
        );
        if (colToAdd.formattedGeneratedExpression() != null) {
            ref = new GeneratedReference(ref, colToAdd.formattedGeneratedExpression(), null);
        }

        return new AddColumnRequest(
            docTableInfo.ident(),
            docTableInfo.isPartitioned(),
            ref,
            AnalyzedColumnDefinition.toMapping(colToAdd),
            colToAdd.hasPrimaryKeyConstraint(),
            colToAdd.formattedGeneratedExpression(),
            tableElementsEvaluated.getCheckConstraints()
        );
    }

    /**
     * Common validation required for pre-5.1.0 plan and post-5.1.0 plan.
     * Resolves generated and default expressions as well.
     */
    static AnalyzedTableElements<Object> validate(AnalyzedAlterTableAddColumn alterTable,
                         CoordinatorTxnCtx txnCtx,
                         NodeContext nodeCtx,
                         Row params,
                         SubQueryResults subQueryResults) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            nodeCtx,
            x,
            params,
            subQueryResults
        );
        DocTableInfo tableInfo = alterTable.tableInfo();
        AnalyzedTableElements<Object> tableElements = alterTable.analyzedTableElements().map(eval);

        for (AnalyzedColumnDefinition<Object> column : tableElements.columns()) {
            ensureColumnLeafsAreNew(column, tableInfo);
        }

        ensureNoIndexDefinitions(tableElements.columns());
        return tableElements;
    }

    /**
     * Used for BWC with versions < 5.1.0
     */
    @VisibleForTesting
    @Deprecated
    public static BoundAddColumn bind(AnalyzedAlterTableAddColumn alterTable,
                                      CoordinatorTxnCtx txnCtx,
                                      NodeContext nodeCtx,
                                      Row params,
                                      SubQueryResults subQueryResults,
                                      FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        AnalyzedTableElements<Object> tableElements = validate(alterTable, txnCtx, nodeCtx, params, subQueryResults);
        DocTableInfo tableInfo = alterTable.tableInfo();

        addExistingPrimaryKeys(tableInfo, tableElements);

        addExistingCheckConstraints(tableInfo, tableElements);
        // validate table elements
        Settings tableSettings = AnalyzedTableElements.validateAndBuildSettings(
            tableElements, fulltextAnalyzerResolver);
        AnalyzedTableElements.finalizeAndValidate(
            tableInfo.ident(),
            alterTable.analyzedTableElementsWithExpressions(),
            tableElements
        );

        Map<String, Object> mapping = toMapping(tableElements);

        int numCurrentPks = tableInfo.primaryKey().size();
        if (tableInfo.primaryKey().contains(DocSysColumns.ID)) {
            numCurrentPks -= 1;
        }

        boolean hasNewPrimaryKeys = AnalyzedTableElements.primaryKeys(tableElements).size() > numCurrentPks;
        boolean hasGeneratedColumns = alterTable.analyzedTableElementsWithExpressions().hasGeneratedColumns();
        return new BoundAddColumn(
            tableInfo,
            tableElements,
            tableSettings,
            mapping,
            hasNewPrimaryKeys,
            hasGeneratedColumns
        );
    }

    private static void ensureColumnLeafsAreNew(AnalyzedColumnDefinition<Object> column, TableInfo tableInfo) {
        if ((!column.isParentColumn() || !column.hasChildren()) && tableInfo.getReference(column.ident()) != null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                             "The table %s already has a column named %s",
                                                             tableInfo.ident().sqlFqn(),
                                                             column.ident().sqlFqn()));
        }
        for (AnalyzedColumnDefinition<Object> child : column.children()) {
            ensureColumnLeafsAreNew(child, tableInfo);
        }
    }

    static void addExistingPrimaryKeys(DocTableInfo tableInfo, AnalyzedTableElements<Object> tableElements) {
        LinkedHashSet<ColumnIdent> pkIncludingAncestors = new LinkedHashSet<>();
        for (ColumnIdent pkIdent : tableInfo.primaryKey()) {
            if (pkIdent.name().equals(DocSysColumns.Names.ID)) {
                continue;
            }
            ColumnIdent maybeParent = pkIdent;
            pkIncludingAncestors.add(maybeParent);
            while ((maybeParent = maybeParent.getParent()) != null) {
                pkIncludingAncestors.add(maybeParent);
            }
        }
        ArrayList<ColumnIdent> columnsToBuildHierarchy = new ArrayList<>(pkIncludingAncestors);
        // We want to have the root columns earlier in the list so that the loop below can be sure parent elements are already present in `columns`
        columnsToBuildHierarchy.sort(Comparator.comparingInt(c -> c.path().size()));
        HashMap<ColumnIdent, AnalyzedColumnDefinition<Object>> columns = new HashMap<>();
        for (ColumnIdent column : columnsToBuildHierarchy) {
            ColumnIdent parent = column.getParent();
            // sort of `columnsToBuildHierarchy` ensures parent would already have been processed and must be present in columns
            AnalyzedColumnDefinition<Object> parentDef = columns.get(parent);

            Reference reference = Objects.requireNonNull(
                tableInfo.getReference(column),
                "Must be able to retrieve Reference for any column that is part of `primaryKey()`");

            AnalyzedColumnDefinition<Object> columnDef = new AnalyzedColumnDefinition<>(reference.position(), parentDef);
            columns.put(column, columnDef);
            columnDef.ident(column);
            if (tableInfo.primaryKey().contains(column)) {
                columnDef.setPrimaryKeyConstraint();
            }

            if (reference.valueType().id() != ObjectType.ID) {
                columnDef.indexConstraint(reference.indexType());
            }

            // We are mirroring PK type as is (including type's internal settings if there are any)
            // We cannot resolve type by name via columnDef.dataType(reference.valueType().getName())
            // as internal parameters, such as VARCHAR length can be lost.
            columnDef.dataType(reference.valueType());

            if (parentDef != null) {
                parentDef.addChild(columnDef);
            }
            if (column.isTopLevel()) {
                tableElements.add(columnDef);
            }
        }
        for (ColumnIdent columnIdent : tableInfo.partitionedBy()) {
            AnalyzedTableElements.changeToPartitionedByColumn(tableElements, columnIdent, true, tableInfo.ident());
        }
    }

    private static void ensureNoIndexDefinitions(List<AnalyzedColumnDefinition<Object>> columns) {
        for (AnalyzedColumnDefinition<Object> column : columns) {
            if (column.isIndexColumn()) {
                throw new UnsupportedOperationException(
                    "Adding an index using ALTER TABLE ADD COLUMN is not supported");
            }
            ensureNoIndexDefinitions(column.children());
        }
    }

    private static void addExistingCheckConstraints(DocTableInfo tableInfo, AnalyzedTableElements<Object> tableElements) {
        List<CheckConstraint<Symbol>> checkConstraints = tableInfo.checkConstraints();
        for (int i = 0; i < checkConstraints.size(); i++) {
            tableElements.addCheckConstraint(tableInfo.ident(), checkConstraints.get(i));
        }
    }
}
