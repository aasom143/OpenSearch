/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.DelegatedPredicateFunction;
import org.opensearch.analytics.spi.DelegatedSubtreeConvertor;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Bottom-up classifier for filter condition trees. Classifies each node as
 * delegated (targets a non-operator backend) or resolved (native), combining
 * same-backend delegated siblings under AND/OR/NOT into a single serialized
 * expression via the backend's {@link DelegatedSubtreeConvertor}.
 *
 * @opensearch.internal
 */
final class DelegatedPredicateCombiner {

    private final String operatorBackend;
    private final List<FieldStorageInfo> fieldStorage;
    private final CapabilityRegistry registry;
    private final RexBuilder rexBuilder;
    private final List<DelegatedExpression> delegatedExpressions;

    DelegatedPredicateCombiner(
        String operatorBackend,
        List<FieldStorageInfo> fieldStorage,
        CapabilityRegistry registry,
        RexBuilder rexBuilder,
        List<DelegatedExpression> delegatedExpressions
    ) {
        this.operatorBackend = operatorBackend;
        this.fieldStorage = fieldStorage;
        this.registry = registry;
        this.rexBuilder = rexBuilder;
        this.delegatedExpressions = delegatedExpressions;
    }

    /** Bottom-up: classify each node as Delegated (carries the RexNode subtree) or Resolved. */
    Classified classify(RexNode node, Function<OperatorAnnotation, RexNode> applyFn) {
        if (node instanceof AnnotatedPredicate ap) {
            String backend = ap.getViableBackends().getFirst();
            if (!backend.equals(operatorBackend) && canSerialize(ap, backend)) {
                return new Delegated(backend, node, ap.getAnnotationId());
            } else if (!ap.getPerformanceDelegationBackends().isEmpty()) {
                String peerBackend = ap.getPerformanceDelegationBackends().getFirst();
                if (canSerialize(ap, peerBackend)) {
                    return new Delegated(peerBackend, node, ap.getAnnotationId());
                }
            }
            return new Resolved(applyFn.apply(ap));
        }

        if (node instanceof RexCall call) {
            SqlKind kind = call.getKind();
            if (kind != SqlKind.AND && kind != SqlKind.OR && kind != SqlKind.NOT) {
                return new Resolved(resolveCallChildren(call, applyFn));
            }

            List<Classified> kids = new ArrayList<>(call.getOperands().size());
            for (RexNode operand : call.getOperands()) {
                kids.add(classify(operand, applyFn));
            }
            return combine(call, kids);
        }

        return new Resolved(node);
    }

    /** Combines classified children of an AND/OR/NOT call into a single Classified result. */
    private Classified combine(RexCall call, List<Classified> kids) {
        List<Delegated> delegatedChildren = new ArrayList<>();
        List<Object> ordered = new ArrayList<>();
        String commonBackend = null;
        boolean multiBackend = false;

        for (Classified c : kids) {
            if (c instanceof Delegated d) {
                delegatedChildren.add(d);
                ordered.add(d);
                if (commonBackend == null) commonBackend = d.backend();
                else if (!commonBackend.equals(d.backend())) multiBackend = true;
            } else {
                ordered.add(((Resolved) c).node());
            }
        }

        if (delegatedChildren.isEmpty() || multiBackend) {
            return new Resolved(materializeIndividually(call, ordered));
        }

        DelegatedSubtreeConvertor convertor = registry.getBackend(commonBackend).getDelegatedSubtreeConvertor();
        if (convertor == null) {
            return new Resolved(materializeIndividually(call, ordered));
        }

        if (ordered.size() == delegatedChildren.size()) {
            // All children are delegated to the same backend — bubble up as one Delegated subtree
            int firstId = delegatedChildren.getFirst().firstAnnotationId();
            return new Delegated(commonBackend, call, firstId);
        }

        // Mixed: some delegated, some native. Combine the delegated children into one expression.
        int firstId = delegatedChildren.getFirst().firstAnnotationId();
        RexNode combinedSubtree = buildCombinedSubtree(call, delegatedChildren);
        byte[] combined = convertor.convertSubtree(combinedSubtree, fieldStorage);
        return new Resolved(materializeCombined(call, ordered, firstId, commonBackend, combined));
    }

    private RexNode materializeIndividually(RexCall call, List<Object> ordered) {
        List<RexNode> newOperands = new ArrayList<>();
        for (Object item : ordered) {
            if (item instanceof Delegated d) {
                byte[] bytes = convertSingleDelegated(d);
                delegatedExpressions.add(new DelegatedExpression(d.firstAnnotationId(), d.backend(), bytes));
                newOperands.add(DelegatedPredicateFunction.makeCall(rexBuilder, d.firstAnnotationId()));
            } else {
                newOperands.add((RexNode) item);
            }
        }
        return call.clone(call.getType(), newOperands);
    }

    private RexNode materializeCombined(RexCall call, List<Object> ordered, int firstId, String backend, byte[] combined) {
        delegatedExpressions.add(new DelegatedExpression(firstId, backend, combined));

        List<RexNode> newOperands = new ArrayList<>();
        newOperands.add(DelegatedPredicateFunction.makeCall(rexBuilder, firstId));
        for (Object item : ordered) {
            if (!(item instanceof Delegated)) {
                newOperands.add((RexNode) item);
            }
        }
        return call.clone(call.getType(), newOperands);
    }

    /** Converts a single Delegated node (leaf or subtree) into bytes via the backend's convertor. */
    private byte[] convertSingleDelegated(Delegated d) {
        DelegatedSubtreeConvertor convertor = registry.getBackend(d.backend()).getDelegatedSubtreeConvertor();
        if (convertor == null) {
            throw new IllegalStateException("Backend [" + d.backend() + "] declares delegation but has no DelegatedSubtreeConvertor");
        }
        return convertor.convertSubtree(d.subtree(), fieldStorage);
    }

    /** Finalizes a Delegated result: converts to bytes, emits DelegatedExpression, returns placeholder. */
    RexNode finalizeDelegated(Delegated d) {
        byte[] bytes = convertSingleDelegated(d);
        delegatedExpressions.add(new DelegatedExpression(d.firstAnnotationId(), d.backend(), bytes));
        return DelegatedPredicateFunction.makeCall(rexBuilder, d.firstAnnotationId());
    }

    /** Checks if the peer backend has a serializer for this predicate's function. */
    private boolean canSerialize(AnnotatedPredicate ap, String peerBackend) {
        RexNode original = ap.unwrap();
        if (original instanceof RexCall originalCall) {
            ScalarFunction fn = ScalarFunction.fromSqlOperatorWithFallback(originalCall.getOperator());
            return fn != null && registry.getBackend(peerBackend).delegatedPredicateSerializers().containsKey(fn);
        }
        return false;
    }

    /**
     * Builds a RexNode subtree from the delegated children of a mixed AND/OR/NOT call,
     * preserving the boolean operator. Only includes the delegated operands.
     */
    private RexNode buildCombinedSubtree(RexCall call, List<Delegated> delegatedChildren) {
        if (delegatedChildren.size() == 1) {
            return delegatedChildren.getFirst().subtree();
        }
        List<RexNode> subtrees = delegatedChildren.stream().map(Delegated::subtree).map(n -> (RexNode) n).toList();
        return call.clone(call.getType(), subtrees);
    }

    private RexNode resolveCallChildren(RexCall call, Function<OperatorAnnotation, RexNode> applyFn) {
        List<RexNode> newOperands = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            Classified c = classify(operand, applyFn);
            if (c instanceof Delegated d) {
                byte[] bytes = convertSingleDelegated(d);
                delegatedExpressions.add(new DelegatedExpression(d.firstAnnotationId(), d.backend(), bytes));
                newOperands.add(DelegatedPredicateFunction.makeCall(rexBuilder, d.firstAnnotationId()));
            } else {
                newOperands.add(((Resolved) c).node());
            }
        }
        return call.clone(call.getType(), newOperands);
    }

    /** Tagged result from bottom-up classification. */
    interface Classified {}

    record Delegated(String backend, RexNode subtree, int firstAnnotationId) implements Classified {
    }

    record Resolved(RexNode node) implements Classified {
    }
}
