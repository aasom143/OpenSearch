/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.AnnotationResolver;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.DelegatedPredicateFunction;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.DelegationPossibleFunction;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ScalarFunction;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * Drives fragment conversion for all {@link StagePlan} alternatives in a {@link QueryDAG}.
 * Strips annotations from each resolved fragment and dispatches to the backend's
 * {@link FragmentConvertor} using composable calls — backends never traverse the plan.
 *
 * <p>Dispatch logic for PR2 (pure shard-scan path):
 * <ul>
 *   <li>Leaf = {@link OpenSearchTableScan}, top = {@link OpenSearchAggregate}(PARTIAL):
 *       {@code convertFragment} on everything below partial agg,
 *       then {@code attachPartialAggOnTop}</li>
 *   <li>Leaf = {@link OpenSearchTableScan}, top = anything else:
 *       {@code convertFragment} on the full fragment</li>
 *   <li>Leaf = {@link OpenSearchStageInputScan} (reduce stage):
 *       {@code convertFragment} on the final agg (ExchangeReducer stripped),
 *       then {@code attachFragmentOnTop} for any operators above it</li>
 * </ul>
 *
 * <p>TODO: as shuffle joins/aggregates and delegation are added, introduce a
 * {@code FragmentConversionStrategy} abstraction so each shape encapsulates its own
 * dispatch logic rather than growing this class with more {@code instanceof} checks.
 *
 * @opensearch.internal
 */
public class FragmentConversionDriver {

    private static final Logger LOGGER = LogManager.getLogger(FragmentConversionDriver.class);

    private FragmentConversionDriver() {}

    /**
     * Converts all {@link StagePlan} alternatives in the DAG, populating
     * {@link StagePlan#convertedBytes()} on each plan.
     */
    public static void convertAll(QueryDAG dag, CapabilityRegistry registry) {
        convertStage(dag.rootStage(), registry);
        // Root stage executes locally at coordinator — store factory for instruction dispatch.
        Stage root = dag.rootStage();
        if (root.getExchangeSinkProvider() != null && !root.getPlanAlternatives().isEmpty()) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(root.getPlanAlternatives().getFirst().backendId());
            root.setInstructionHandlerFactory(backend.getInstructionHandlerFactory());
        }
    }

    private static void convertStage(Stage stage, CapabilityRegistry registry) {
        for (Stage child : stage.getChildStages()) {
            convertStage(child, registry);
        }
        List<StagePlan> converted = new ArrayList<>(stage.getPlanAlternatives().size());
        for (StagePlan plan : stage.getPlanAlternatives()) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(plan.backendId());
            FragmentConvertor convertor = backend.getFragmentConvertor();

            // Derive filter tree shape BEFORE stripping (annotations must be intact)
            OpenSearchFilter filter = RelNodeUtils.findNode(plan.resolvedFragment(), OpenSearchFilter.class);
            FilterTreeShape treeShape = filter != null
                ? FilterTreeShapeDeriver.derive(filter, plan.backendId())
                : FilterTreeShape.NO_DELEGATION;

            IntraOperatorDelegationBytes delegationBytes = new IntraOperatorDelegationBytes(registry);
            byte[] bytes = convert(plan.resolvedFragment(), convertor, delegationBytes);

            // Assemble instruction list
            List<DelegatedExpression> delegated = delegationBytes.getResult();
            List<InstructionNode> instructions = assembleInstructions(backend, plan, treeShape, delegationBytes);

            converted.add(plan.withConvertedBytes(bytes, delegated).withInstructions(instructions));
            LOGGER.info(
                "Stage [{}] converted: treeShape={}, delegatedExpressions={}{}",
                plan.backendId(),
                treeShape,
                delegated.size(),
                delegated.isEmpty()
                    ? ""
                    : " [ids=" + delegated.stream().map(d -> String.valueOf(d.getAnnotationId())).toList() + "]"
            );
        }
        stage.setPlanAlternatives(converted);
        // Store factory on coordinator-reduce stages (local execution, no serialization needed).
        // Shard stages get the factory from the local backend plugin at the data node.
        if (stage.getExchangeSinkProvider() != null && !converted.isEmpty()) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(converted.getFirst().backendId());
            stage.setInstructionHandlerFactory(backend.getInstructionHandlerFactory());
        }
    }

    private static List<InstructionNode> assembleInstructions(
        AnalyticsSearchBackendPlugin backend,
        StagePlan plan,
        FilterTreeShape treeShape,
        IntraOperatorDelegationBytes delegationBytes
    ) {
        FragmentInstructionHandlerFactory factory = backend.getInstructionHandlerFactory();
        LinkedList<InstructionNode> instructions = new LinkedList<>();
        RelNode leaf = findLeaf(plan.resolvedFragment());

        if (leaf instanceof OpenSearchTableScan) {
            List<DelegatedExpression> delegated = delegationBytes.getResult();
            if (!delegated.isEmpty()) {
                // Delegation exists — use ShardScanWithDelegationInstructionNode which carries
                // treeShape + count for the driving backend to configure its custom scan operator
                factory.createShardScanWithDelegationNode(treeShape, delegated.size()).ifPresent(instructions::add);
            } else {
                factory.createShardScanNode().ifPresent(instructions::add);
            }
        }
        return instructions;
    }

    /**
     * Accumulates serialized delegated query bytes during fragment conversion.
     *
     * <p>The resolver performs a single bottom-up traversal of the filter condition tree,
     * classifying each node as delegated (targets a non-operator backend like Lucene) or
     * native (evaluated by the driving backend). Same-backend delegated siblings under
     * AND/OR/NOT are combined into a single BooleanQuery via
     * {@link BackendCapabilityProvider#combineDelegatedPredicates}, producing one
     * {@link DelegatedExpression} and one Lucene collector call per row group instead of
     * multiple round-trips.
     */
    static final class IntraOperatorDelegationBytes {
        private final CapabilityRegistry registry;
        private List<DelegatedExpression> delegatedExpressions;

        IntraOperatorDelegationBytes(CapabilityRegistry registry) {
            this.registry = registry;
        }

        /**
         * Creates an annotation resolver that does a single bottom-up traversal.
         * Each node is classified as DELEGATED (pure same-backend) or RESOLVED (native/mixed).
         * At mixed boundaries, all delegated siblings are combined into one BooleanQuery.
         */
        Function<OperatorAnnotation, RexNode> resolverFor(OpenSearchRelNode operator, RexBuilder rexBuilder) {
            String operatorBackend = operator.getViableBackends().getFirst();
            List<FieldStorageInfo> fieldStorage = operator.getOutputFieldStorage();
            return new AnnotationResolver() {

                @Override
                public RexNode resolveTree(RexNode condition) {
                    Classified result = classify(condition);
                    if (result instanceof Delegated d) {
                        if (delegatedExpressions == null) delegatedExpressions = new ArrayList<>();
                        delegatedExpressions.add(new DelegatedExpression(d.firstAnnotationId(), d.backend(), d.combinedBytes()));
                        return DelegatedPredicateFunction.makeCall(rexBuilder, d.firstAnnotationId());
                    }
                    return ((Resolved) result).node;
                }

                @Override
                public RexNode apply(OperatorAnnotation annotation) {
                    String annotationBackend = annotation.getViableBackends().getFirst();
                    if (annotationBackend.equals(operatorBackend)) {
                        // Performance-delegation candidate: dual-viable predicate kept on the operator's backend,
                        // but a peer can be opportunistically consulted at runtime. Wrap with delegation_possible
                        // so the original predicate is preserved AND the peer can be reached via annotationId.
                        if (annotation instanceof AnnotatedPredicate ap && !ap.getPerformanceDelegationBackends().isEmpty()) {
                            // TODO: pick the best peer instead of the first when more than two backends are viable.
                            String peerBackend = ap.getPerformanceDelegationBackends().getFirst();
                            RexNode original = ap.unwrap();
                            if (!(original instanceof RexCall originalCall)) {
                                throw new IllegalStateException("Performance-delegation candidate must wrap a RexCall: " + original);
                            }
                            // Performance-delegated predicates are typically SqlBinaryOperators (=, <, >, etc.),
                            // not SqlFunctions like MATCH_PHRASE. fromSqlOperatorWithFallback handles both.
                            ScalarFunction function = ScalarFunction.fromSqlOperatorWithFallback(originalCall.getOperator());
                            DelegatedPredicateSerializer serializer = registry.getBackend(peerBackend)
                                .getCapabilityProvider()
                                .delegatedPredicateSerializers()
                                .get(function);
                            if (serializer == null) {
                                // Delegated backend declared filter capability for this op but doesn't
                                // ship a serializer for it (e.g. Lucene declares LESS_THAN_OR_EQUAL but
                                // only EqualsSerializer is wired today). Without a serializer we can't
                                // emit a delegation_possible(...) marker, so fall back to native: just
                                // unwrap as a regular predicate evaluated by the operator's own backend.
                                // Same end result as a single-viable predicate — no perf delegation for
                                // this leaf, correctness preserved. CapabilityRegistry startup validation
                                // will eventually catch the capability/serializer mismatch at boot and reject
                                // the plugin instead of silently degrading at query time.
                                LOGGER.debug(
                                    "Performance-delegation skipped: no serializer for [{}] on delegated backend [{}]; falling back to native on operator [{}]",
                                    function,
                                    peerBackend,
                                    operatorBackend
                                );
                                return annotation.unwrap();
                            }
                            byte[] serialized = serializer.serialize(originalCall, fieldStorage);
                            LOGGER.debug(
                                "Performance-delegated annotation [id={}]: {} kept on operator [{}], wrapped for peer [{}], serialized {} bytes",
                                ap.getAnnotationId(),
                                function,
                                operatorBackend,
                                peerBackend,
                                serialized.length
                            );
                            if (delegatedExpressions == null) {
                                delegatedExpressions = new ArrayList<>();
                            }
                            delegatedExpressions.add(new DelegatedExpression(ap.getAnnotationId(), peerBackend, serialized));
                            return DelegationPossibleFunction.makeCall(rexBuilder, originalCall, ap.getAnnotationId());
                        }
                        LOGGER.debug("Native annotation [id={}]: backend [{}] matches operator", annotation.getAnnotationId(), operatorBackend);
                        return annotation.unwrap();
                    }
                    RexNode original = annotation.unwrap();
                    if (!(original instanceof RexCall originalCall) || !(originalCall.getOperator() instanceof SqlFunction sqlFunction)) {
                        throw new IllegalStateException("Delegated expression must be a SqlFunction call: " + original);
                    }
                    ScalarFunction function = ScalarFunction.fromSqlFunction(sqlFunction);
                    DelegatedPredicateSerializer serializer = registry.getBackend(annotationBackend)
                        .getCapabilityProvider()
                        .delegatedPredicateSerializers()
                        .get(function);
                    if (serializer == null) {
                        throw new IllegalStateException(
                            "No DelegatedPredicateSerializer for ["
                                + function
                                + "] on backend ["
                                + annotationBackend
                                + "]. CapabilityRegistry should have rejected this at startup."
                        );
                    }
                    byte[] serialized = serializer.serialize(originalCall, fieldStorage);
                    LOGGER.debug(
                        "Delegated annotation [id={}]: {} from operator [{}] to [{}], serialized {} bytes",
                        annotation.getAnnotationId(),
                        function,
                        operatorBackend,
                        annotationBackend,
                        serialized.length
                    );
                    if (delegatedExpressions == null) {
                        delegatedExpressions = new ArrayList<>();
                    }
                    delegatedExpressions.add(
                        new DelegatedExpression(annotation.getAnnotationId(), annotationBackend, serialized)
                    );
                    return annotation.makePlaceholder(rexBuilder);
                }

                /** Bottom-up: classify each node as Delegated (carries combined bytes) or Resolved. */
                private Classified classify(RexNode node) {
                    if (node instanceof AnnotatedPredicate ap) {
                        String backend = ap.getViableBackends().getFirst();
                        if (!backend.equals(operatorBackend) && ap.getPerformanceDelegationBackends().isEmpty()) {
                            byte[] serialized = serializeAnnotation(ap, backend);
                            if (serialized != null) {
                                return new Delegated(backend, serialized, ap.getAnnotationId());
                            }
                        }
                        return new Resolved(apply(ap));
                    }

                    if (node instanceof RexCall call) {
                        SqlKind kind = call.getKind();
                        if (kind != SqlKind.AND
                            && kind != SqlKind.OR
                            && kind != SqlKind.NOT) {
                            return new Resolved(resolveCallChildren(call, rexBuilder));
                        }

                        // Single pass: classify children, separate delegated from resolved
                        List<Delegated> delegatedChildren = new ArrayList<>();
                        List<Object> ordered = new ArrayList<>(); // Delegated or RexNode
                        String commonBackend = null;
                        boolean multiBackend = false;

                        for (RexNode operand : call.getOperands()) {
                            Classified c = classify(operand);
                            if (c instanceof Delegated d) {
                                delegatedChildren.add(d);
                                ordered.add(d);
                                if (commonBackend == null) commonBackend = d.backend;
                                else if (!commonBackend.equals(d.backend)) multiBackend = true;
                            } else {
                                ordered.add(((Resolved) c).node);
                            }
                        }

                        // No delegated children or multi-backend — just rebuild with resolved
                        if (delegatedChildren.isEmpty() || multiBackend) {
                            return new Resolved(materializeIndividually(call, ordered, rexBuilder));
                        }

                        // Combine all delegated children into one
                        List<byte[]> bytesList = delegatedChildren.stream().map(d -> d.combinedBytes).toList();
                        int firstId = delegatedChildren.getFirst().firstAnnotationId;
                        BackendCapabilityProvider cap = registry.getBackend(commonBackend).getCapabilityProvider();
                        byte[] combined = cap.combineDelegatedPredicates(bytesList, kind);

                        // Combining failed — materialize each individually
                        if (combined == null) {
                            return new Resolved(materializeIndividually(call, ordered, rexBuilder));
                        }

                        // All delegated, no native children — bubble up
                        if (ordered.size() == delegatedChildren.size()) {
                            return new Delegated(commonBackend, combined, firstId);
                        }

                        // Mixed — materialize combined placeholder + keep native children
                        return new Resolved(materializeCombined(call, ordered, firstId, commonBackend, combined, rexBuilder));
                    }

                    return new Resolved(node);
                }

                private RexNode materializeIndividually(RexCall call, List<Object> ordered, RexBuilder rb) {
                    List<RexNode> newOperands = new ArrayList<>();
                    for (Object item : ordered) {
                        if (item instanceof Delegated d) {
                            if (delegatedExpressions == null) delegatedExpressions = new ArrayList<>();
                            delegatedExpressions.add(new DelegatedExpression(d.firstAnnotationId, d.backend, d.combinedBytes));
                            newOperands.add(DelegatedPredicateFunction.makeCall(rb, d.firstAnnotationId));
                        } else {
                            newOperands.add((RexNode) item);
                        }
                    }
                    return call.clone(call.getType(), newOperands);
                }

                private RexNode materializeCombined(
                    RexCall call, List<Object> ordered,
                    int firstId, String backend, byte[] combined, RexBuilder rb
                ) {
                    if (delegatedExpressions == null) delegatedExpressions = new ArrayList<>();
                    delegatedExpressions.add(new DelegatedExpression(firstId, backend, combined));

                    List<RexNode> newOperands = new ArrayList<>();
                    newOperands.add(DelegatedPredicateFunction.makeCall(rb, firstId));
                    for (Object item : ordered) {
                        if (!(item instanceof Delegated)) {
                            newOperands.add((RexNode) item);
                        }
                    }
                    return call.clone(call.getType(), newOperands);
                }

                private byte[] serializeAnnotation(AnnotatedPredicate ap, String backend) {
                    RexNode original = ap.unwrap();
                    if (!(original instanceof RexCall originalCall)
                        || !(originalCall.getOperator() instanceof SqlFunction sqlFunction)) {
                        return null;
                    }
                    ScalarFunction function = ScalarFunction.fromSqlFunction(sqlFunction);
                    DelegatedPredicateSerializer serializer = registry.getBackend(backend)
                        .getCapabilityProvider().delegatedPredicateSerializers().get(function);
                    if (serializer == null) return null;
                    return serializer.serialize(originalCall, fieldStorage);
                }

                private RexNode resolveCallChildren(RexCall call, RexBuilder rb) {
                    List<RexNode> newOperands = new ArrayList<>();
                    boolean changed = false;
                    for (RexNode operand : call.getOperands()) {
                        Classified c = classify(operand);
                        RexNode resolved;
                        if (c instanceof Delegated d) {
                            if (delegatedExpressions == null) delegatedExpressions = new ArrayList<>();
                            delegatedExpressions.add(new DelegatedExpression(d.firstAnnotationId, d.backend, d.combinedBytes));
                            resolved = DelegatedPredicateFunction.makeCall(rb, d.firstAnnotationId);
                        } else {
                            resolved = ((Resolved) c).node;
                        }
                        newOperands.add(resolved);
                        if (resolved != operand) changed = true;
                    }
                    return changed ? call.clone(call.getType(), newOperands) : call;
                }
            };
        }

        List<DelegatedExpression> getResult() {
            return delegatedExpressions != null ? delegatedExpressions : List.of();
        }

        /** Tagged result from bottom-up classification. */
        interface Classified {}

        record Delegated(String backend, byte[] combinedBytes, int firstAnnotationId) implements Classified {
        }

        record Resolved(RexNode node) implements Classified {
        }
    }

    /**
     * Dispatches conversion based on the fragment's leaf and top node types.
     */
    static byte[] convert(RelNode resolvedFragment, FragmentConvertor convertor, IntraOperatorDelegationBytes delegationBytes) {
        RelNode leaf = findLeaf(resolvedFragment);

        if (leaf instanceof OpenSearchTableScan) {
            // Partial agg at top: convert everything below it, then attach partial agg on top.
            // strippedInputs passed to stripAnnotations for schema validity (LogicalAggregate needs its inputs).
            if (resolvedFragment instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.PARTIAL) {
                List<RelNode> strippedInputs = agg.getInputs().stream().map(input -> strip(input, delegationBytes)).toList();
                byte[] innerBytes = convertor.convertFragment(strippedInputs.getFirst());
                Function<OperatorAnnotation, RexNode> resolver = delegationBytes.resolverFor(agg, agg.getCluster().getRexBuilder());
                RelNode strippedAgg = agg.stripAnnotations(strippedInputs, resolver);
                return convertor.attachPartialAggOnTop(strippedAgg, innerBytes);
            }

            RelNode stripped = strip(resolvedFragment, delegationBytes);
            return convertor.convertFragment(stripped);
        }

        if (leaf instanceof OpenSearchStageInputScan) {
            return convertReduceFragment(resolvedFragment, convertor, delegationBytes);
        }

        if (leaf instanceof org.opensearch.analytics.planner.rel.OpenSearchValues) {
            // Coord-only literal source — convert the whole fragment via the same isthmus
            // path as reduce fragments. isthmus emits ReadRel.VirtualTable for the Values
            // leaf; DataFusion executes it locally without any input partitions.
            RelNode stripped = strip(resolvedFragment, delegationBytes);
            return convertor.convertFragment(stripped);
        }

        throw new IllegalStateException(
            "Unknown leaf type [" + leaf.getClass().getSimpleName() + "]. " + "Add a FragmentConversionStrategy for this leaf type."
        );
    }

    /**
     * Reduce stage conversion: strips ExchangeReducer, converts the final agg fragment
     * (with StageInputScan as leaf for schema), then attaches any operators above it
     * (Sort, Project, etc.) via attachFragmentOnTop.
     *
     * <p>Single-input ancestors of a single gathered subtree (Sort/Project/Aggregate over
     * a partial agg) reach convertFragment as soon as we see a node whose inputs
     * are all ExchangeReducers, and attach via attachFragmentOnTop on the way back up.
     *
     * <p>Multi-input nodes (Join, Union, Intersect, Minus) are converted as a single
     * subtree via convertFragment: isthmus handles all of them natively, and
     * rewriting OpenSearchStageInputScan leaves to plain TableScans (inside the convertor)
     * lets the whole gathered subtree serialize in one pass. No post-conversion
     * substrait-level stitching is needed.
     */
    private static byte[] convertReduceFragment(RelNode node, FragmentConvertor convertor, IntraOperatorDelegationBytes delegationBytes) {
        return convertReduceNode(node, convertor, false, delegationBytes);
    }

    private static byte[] convertReduceNode(
        RelNode node,
        FragmentConvertor convertor,
        boolean finalAggConverted,
        IntraOperatorDelegationBytes delegationBytes
    ) {
        if (node instanceof OpenSearchExchangeReducer) {
            // Strip ExchangeReducer — StageInputScan below it is the schema source.
            return convertor.convertFragment(strip(node.getInputs().getFirst(), delegationBytes));
        }
        if (node instanceof OpenSearchRelNode openSearchNode) {
            List<RelNode> strippedInputs = node.getInputs().stream().map(input -> strip(input, delegationBytes)).toList();
            Function<OperatorAnnotation, RexNode> resolver = delegationBytes.resolverFor(openSearchNode, node.getCluster().getRexBuilder());
            RelNode strippedNode = openSearchNode.stripAnnotations(strippedInputs, resolver);

            if (!finalAggConverted) {
                // First OpenSearchRelNode whose ALL inputs are ExchangeReducers is treated as the
                // boundary between the coordinator-side fragment and the data-node child stages.
                // For single-input shapes (Sort/Project/Aggregate over a partial agg) this is the
                // final-aggregate operator; for multi-input shapes (Union) every branch is itself
                // an ER → StageInputScan, and the entire Union+ER subtree is converted as one
                // fragment so all branches end up in the same Substrait plan reading from their
                // respective input partitions.
                boolean allChildrenAreExchangeReducer = !node.getInputs().isEmpty()
                    && node.getInputs().stream().allMatch(input -> input instanceof OpenSearchExchangeReducer);
                if (allChildrenAreExchangeReducer && node.getInputs().size() == 1) {
                    List<RelNode> finalAggInputs = new ArrayList<>(node.getInputs().size());
                    for (RelNode input : node.getInputs()) {
                        // Skip the ER, keep StageInputScan below it as the leaf for schema inference.
                        finalAggInputs.add(strip(input.getInputs().getFirst(), delegationBytes));
                    }
                    RelNode finalAggFragment = openSearchNode.stripAnnotations(finalAggInputs, resolver);
                    return convertor.convertFragment(finalAggFragment);
                }
            }

            // Multi-input node (Join, Union, Intersect, Minus): isthmus handles all of them
            // natively. The whole subtree — multi-input node + its branches + ERs +
            // StageInputScans — serializes in one convertFragment pass. The convertor's
            // StageInputScan → plain TableScan rewrite makes the leaves isthmus-friendly without
            // any post-conversion substrait-level stitching.
            if (node.getInputs().size() >= 2) {
                return convertor.convertFragment(strip(node, delegationBytes));
            }

            // Single-input operator above the final-fragment boundary — convert child first, then attach.
            byte[] innerBytes = convertReduceNode(node.getInputs().getFirst(), convertor, false, delegationBytes);
            return convertor.attachFragmentOnTop(strippedNode, innerBytes);
        }
        throw new IllegalStateException("Unexpected reduce stage node: " + node.getClass().getSimpleName());
    }

    /** Recursively strips annotations bottom-up. Keeps OpenSearchStageInputScan as-is. */
    private static RelNode strip(RelNode node, IntraOperatorDelegationBytes delegationBytes) {
        if (node instanceof OpenSearchStageInputScan) {
            return node; // kept for schema inference at reduce stage
        }
        if (node instanceof OpenSearchExchangeReducer) {
            return strip(node.getInputs().getFirst(), delegationBytes);
        }
        List<RelNode> strippedChildren = new ArrayList<>(node.getInputs().size());
        for (RelNode input : node.getInputs()) {
            strippedChildren.add(strip(input, delegationBytes));
        }
        if (node instanceof OpenSearchRelNode openSearchNode) {
            Function<OperatorAnnotation, RexNode> resolver = delegationBytes.resolverFor(openSearchNode, node.getCluster().getRexBuilder());
            return openSearchNode.stripAnnotations(strippedChildren, resolver);
        }
        return node;
    }

    private static RelNode findLeaf(RelNode node) {
        if (node.getInputs().isEmpty()) {
            return node;
        }
        return findLeaf(node.getInputs().getFirst());
    }
}
