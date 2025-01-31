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

package io.crate.expression.predicate;

import static io.crate.execution.dml.ArrayIndexer.ARRAY_LENGTH_FIELD_SUPPORTED_VERSION;
import static io.crate.lucene.LuceneQueryBuilder.genericFunctionFilter;
import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;

import java.util.Collections;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.execution.dml.ArrayIndexer;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.SysColumns;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.StorageSupport;
import io.crate.types.TypeSignature;

public class IsNullPredicate<T> extends Scalar<Boolean, T> {

    public static final String NAME = "op_isnull";
    public static final Signature SIGNATURE = Signature.builder(NAME, FunctionType.SCALAR)
        .argumentTypes(TypeSignature.parse("E"))
        .returnType(DataTypes.BOOLEAN.getTypeSignature())
        .features(Feature.DETERMINISTIC, Feature.NOTNULL)
        .typeVariableConstraints(typeVariable("E"))
        .build();

    public static void register(Functions.Builder builder) {
        builder.add(
            SIGNATURE,
            IsNullPredicate::new
        );
    }

    private IsNullPredicate(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx, NodeContext nodeCtx) {
        assert symbol != null : "function must not be null";
        assert symbol.arguments().size() == 1 : "function's number of arguments must be 1";
        Symbol arg = symbol.arguments().get(0);
        if (arg instanceof Input<?> input) {
            return Literal.of(input.value() == null);
        }
        return symbol;
    }

    @Override
    @SafeVarargs
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<T> ... args) {
        assert args.length == 1 : "number of args must be 1";
        return args[0].value() == null;
    }

    @Override
    public Query toQuery(Function function, Context context) {
        List<Symbol> arguments = function.arguments();
        assert arguments.size() == 1 : "`<expression> IS NULL` function must have one argument";
        if (arguments.get(0) instanceof Reference ref) {
            if (!ref.isNullable()) {
                return new MatchNoDocsQuery("`x IS NULL` on column that is NOT NULL can't match");
            }
            Query refExistsQuery = refExistsQuery(ref, context);
            return refExistsQuery == null ? null : Queries.not(refExistsQuery);
        }
        return null;
    }


    @Nullable
    public static Query refExistsQuery(Reference ref, Context context) {
        String field = ref.storageIdent();
        DataType<?> valueType = ref.valueType();
        boolean canUseFieldsExist = ref.hasDocValues() || ref.indexType() == IndexType.FULLTEXT;
        if (valueType instanceof ArrayType<?>) {
            if (context.tableInfo().versionCreated().onOrAfter(ARRAY_LENGTH_FIELD_SUPPORTED_VERSION)) {
                // Array columns in tables on and after 5.9 indexes _array_length_ fields. For null rows, nothing is indexed
                // such that FieldExistsQuery can be used.
                return ArrayIndexer.arrayLengthExistsQuery(ref, context.tableInfo()::getReference);
            } else {
                if (canUseFieldsExist) {
                    return new BooleanQuery.Builder()
                        .setMinimumNumberShouldMatch(1)
                        .add(new FieldExistsQuery(field), Occur.SHOULD)
                        .add(Queries.not(isNullFuncToQuery(ref, context)), Occur.SHOULD)
                        .build();
                } else {
                    return null;
                }
            }
        }
        StorageSupport<?> storageSupport = valueType.storageSupport();
        if (ref instanceof DynamicReference) {
            if (context.tableInfo().isIgnoredOrImmediateChildOfIgnored(ref)) {
                // Not indexed, need to use source lookup
                return null;
            }
            return new MatchNoDocsQuery("DynamicReference/type without storageSupport does not exist");
        } else if (canUseFieldsExist) {
            return new FieldExistsQuery(field);
        } else if (context.tableInfo().isIgnoredOrImmediateChildOfIgnored(ref)) {
            // Not indexed, need to use source lookup
            return null;
        } else if (storageSupport != null) {
            if (valueType instanceof ObjectType objType) {
                if (objType.innerTypes().isEmpty()) {
                    return null;
                }
                BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder()
                    .setMinimumNumberShouldMatch(1);
                for (var entry : objType.innerTypes().entrySet()) {
                    String childColumn = entry.getKey();
                    Reference childRef = context.getRef(ref.column().getChild(childColumn));
                    if (childRef == null) {
                        return null;
                    }
                    Query refExistsQuery = refExistsQuery(childRef, context);
                    if (refExistsQuery == null) {
                        return null;
                    }
                    booleanQuery.add(refExistsQuery, Occur.SHOULD);
                }
                return booleanQuery
                    // Even if a child columns exist, an object can have empty values. Example:
                    //  CREATE TABLE t (obj OBJECT as (x int));
                    //  INSERT INTO t (obj) VALUES ({});
                    .add(Queries.not(isNullFuncToQuery(ref, context)), Occur.SHOULD)
                    .build();
            }
            if (ref.indexType() == IndexType.NONE) {
                return null;
            } else {
                return new ConstantScoreQuery(new TermQuery(new Term(SysColumns.FieldNames.NAME, field)));
            }
        } else {
            return null;
        }
    }

    static Query isNullFuncToQuery(Symbol arg, Context context) {
        Function isNullFunction = new Function(
            IsNullPredicate.SIGNATURE,
            Collections.singletonList(arg),
            DataTypes.BOOLEAN
        );
        return genericFunctionFilter(isNullFunction, context);
    }

}
