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

package io.crate.expression.operator.any;

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;

import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.Operator;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.FunctionType;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.TypeVariableConstraint;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.EqQuery;
import io.crate.types.StorageSupport;
import io.crate.types.TypeSignature;

public final class AnyNeqOperator extends AnyOperator<Object> {

    public static final String NAME = OPERATOR_PREFIX + ComparisonExpression.Type.NOT_EQUAL.getValue();
    public static final Signature SIGNATURE = Signature.builder(NAME, FunctionType.SCALAR)
        .argumentTypes(TypeSignature.parse("E"),
            TypeSignature.parse("array(E)"))
        .returnType(Operator.RETURN_TYPE.getTypeSignature())
        .features(Feature.DETERMINISTIC)
        .typeVariableConstraints(TypeVariableConstraint.typeVariable("E"))
        .build();

    AnyNeqOperator(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    boolean matches(Object probe, Object candidate) {
        return leftType.compare(probe, candidate) != 0;
    }

    @Override
    protected Query refMatchesAnyArrayLiteral(Function any, Reference probe, Literal<?> candidates, Context context) {
        if (ArrayType.dimensions(candidates.valueType()) > 1) {
            return null;
        }
        var nonNullValues = filterNullValues(candidates);
        if (nonNullValues.isEmpty()) {
            return new MatchNoDocsQuery("Cannot match unless there is at least one non-null candidate");
        }
        //  col != ANY ([1,2,3]) --> not(col=1 and col=2 and col=3)
        String columnName = probe.storageIdent();
        BooleanQuery.Builder andBuilder = new BooleanQuery.Builder();
        for (Object value : nonNullValues) {
            var fromPrimitive = EqOperator.fromPrimitive(
                probe.valueType(),
                columnName,
                value,
                probe.hasDocValues(),
                probe.indexType());
            if (fromPrimitive == null) {
                return null;
            }
            andBuilder.add(fromPrimitive, Occur.MUST);
        }
        Query exists = IsNullPredicate.refExistsQuery(probe, context);
        return new BooleanQuery.Builder()
            .add(Queries.not(andBuilder.build()), Occur.MUST)
            .add(exists, Occur.FILTER)
            .build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Query literalMatchesAnyArrayRef(Literal<?> probe, Reference candidates) {
        // 1 != any ( col ) -->  gt 1 or lt 1
        if (DataTypes.isArray(probe.valueType())) {
            return null;
        }
        String columnName = candidates.storageIdent();
        StorageSupport<?> storageSupport = probe.valueType().storageSupport();
        if (storageSupport == null) {
            return null;
        }
        EqQuery eqQuery = storageSupport.eqQuery();
        if (eqQuery == null) {
            return null;
        }
        Object value = probe.value();
        BooleanQuery.Builder query = new BooleanQuery.Builder();
        query.setMinimumNumberShouldMatch(1);
        var gt = eqQuery.rangeQuery(
            columnName,
            value,
            null,
            false,
            false,
            candidates.hasDocValues(),
            candidates.indexType() != IndexType.NONE);
        var lt = eqQuery.rangeQuery(
            columnName,
            null,
            value,
            false,
            false,
            candidates.hasDocValues(),
            candidates.indexType() != IndexType.NONE);
        if (lt == null || gt == null) {
            assert lt != null || gt == null : "If lt is null, gt must be null";
            return null;
        }
        query.add(gt, Occur.SHOULD);
        query.add(lt, Occur.SHOULD);
        return query.build();
    }

    @Override
    protected Query literalMatchesAnyArrayRef(Function any, Literal<?> probe, Reference candidates, Context context) {
        return literalMatchesAnyArrayRef(probe, candidates);
    }
}
