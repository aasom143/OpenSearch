/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.MockDataFusionBackend;
import org.opensearch.analytics.planner.MockLuceneBackend;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.AnnotatedProjectExpression;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegatedPredicateFunction;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.DelegationPossibleFunction;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.InstructionType;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ShardScanWithDelegationInstructionNode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link FragmentConversionDriver} — verifies annotations are stripped
 * and the correct {@link FragmentConvertor} method is called per leaf type,
 * across a range of query shapes.
 */
public class FragmentConversionDriverTests extends BasePlannerRulesTests {

    private static final Logger LOGGER = LogManager.getLogger(FragmentConversionDriverTests.class);

    private static final Set<String> OPENSEARCH_OPERATORS = Set.of(
        OpenSearchFilter.class.getSimpleName(),
        OpenSearchAggregate.class.getSimpleName(),
        OpenSearchTableScan.class.getSimpleName(),
        OpenSearchSort.class.getSimpleName(),
        OpenSearchProject.class.getSimpleName()
    );

    // AggregateCallAnnotation is stored on OpenSearchAggregate as a side-map (not in
    // aggCall.rexList), so it never appears in the plan's textual representation —
    // hence it's not in this set. AnnotatedPredicate and AnnotatedProjectExpression
    // still ride in their parent operators' RexNodes and DO appear textually.
    private static final Set<String> ANNOTATION_MARKERS = Set.of(
        AnnotatedPredicate.class.getSimpleName(),
        AnnotatedProjectExpression.class.getSimpleName()
    );

    // ---- Helpers ----

    private MockDataFusionBackend dfWithConvertor(RecordingConvertor convertor) {
        return new MockDataFusionBackend() {
            @Override
            public FragmentConvertor getFragmentConvertor() {
                return convertor;
            }
        };
    }

    private QueryDAG buildAndConvert(int shardCount, RelNode logicalPlan, RecordingConvertor convertor) {
        var df = dfWithConvertor(convertor);
        var context = buildContext("parquet", shardCount, intFields(), List.of(df));
        RelNode cboOutput = runPlanner(logicalPlan, context);
        LOGGER.info("Marked+CBO:\n{}", RelOptUtil.toString(cboOutput));
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        LOGGER.info("QueryDAG after conversion:\n{}", dag);
        return dag;
    }

    private void assertDoesntContainOperators(RelNode fragment, Set<String> forbidden) {
        assertNotNull(fragment);
        String planStr = RelOptUtil.toString(fragment);
        LOGGER.info("Stripped fragment:\n{}", planStr);
        for (String op : forbidden)
            assertFalse("Stripped fragment must not contain [" + op + "]", planStr.contains(op));
    }

    private void assertShardScanConverted(RecordingConvertor convertor, Stage stage) {
        assertEquals("expected exactly one alternative", 1, stage.getPlanAlternatives().size());
        assertNotNull("convertedBytes must be set", stage.getPlanAlternatives().getFirst().convertedBytes());
        assertTrue("convertFragment (shard-scan shape) must be called", convertor.shardScanCalled);
        assertEquals("test_index", convertor.shardScanTableName);
        assertDoesntContainOperators(convertor.shardScanFragment, OPENSEARCH_OPERATORS);
        assertDoesntContainOperators(convertor.shardScanFragment, ANNOTATION_MARKERS);
        // Instruction assertions
        StagePlan plan = stage.getPlanAlternatives().getFirst();
        assertFalse("instructions must not be empty", plan.instructions().isEmpty());
        assertEquals("first instruction must be SHARD_SCAN", InstructionType.SETUP_SHARD_SCAN, plan.instructions().getFirst().type());
    }

    private void assertReduceStageConverted(RecordingConvertor convertor, Stage stage) {
        assertEquals("expected exactly one alternative", 1, stage.getPlanAlternatives().size());
        assertNotNull("convertedBytes must be set", stage.getPlanAlternatives().getFirst().convertedBytes());
        assertTrue("convertFragment (final-agg shape) must be called", convertor.finalAggCalled);
        assertDoesntContainOperators(convertor.reduceFragment, OPENSEARCH_OPERATORS);
        assertDoesntContainOperators(convertor.reduceFragment, ANNOTATION_MARKERS);
        // Coord-side reduce stages no longer register FinalAggregateInstructionHandler.
        // DataFusion plans the substrait Aggregate's Partial+Final pair itself via the legacy
        // executeLocalPlan path; the previous SETUP_FINAL_AGGREGATE instruction routed through
        // Rust's apply_aggregate_mode strip, which corrupted column refs (cnt[sum]/cnt[count]).
        StagePlan plan = stage.getPlanAlternatives().getFirst();
        assertTrue("coord-side reduce instructions must be empty", plan.instructions().isEmpty());
    }

    // ---- Single-stage query shapes ----

    /**
     * Scan, Filter(Scan), Aggregate(Scan), Sort(Filter(Scan)) — single-shard plans now
     * have a coord stage above the data-node stage (since scans declare RANDOM, the
     * coord must gather). The test verifies convertFragment (shard-scan shape) is called on the
     * data-node child stage and the fragment is fully stripped.
     */
    public void testSingleStageQueryShapes() {
        RecordingConvertor scanConvertor = new RecordingConvertor();
        QueryDAG scanDag = buildAndConvert(1, stubScan(mockTable("test_index", "status", "size")), scanConvertor);
        assertShardScanConverted(scanConvertor, dataNodeStage(scanDag));

        RecordingConvertor filterConvertor = new RecordingConvertor();
        QueryDAG filterDag = buildAndConvert(
            1,
            LogicalFilter.create(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
            filterConvertor
        );
        assertShardScanConverted(filterConvertor, dataNodeStage(filterDag));

        RecordingConvertor aggConvertor = new RecordingConvertor();
        QueryDAG aggDag = buildAndConvert(1, makeAggregate(sumCall()), aggConvertor);
        assertShardScanConverted(aggConvertor, dataNodeStage(aggDag));

        RecordingConvertor sortConvertor = new RecordingConvertor();
        QueryDAG sortDag = buildAndConvert(
            1,
            makeSort(makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)), 10),
            sortConvertor
        );
        assertShardScanConverted(sortConvertor, dataNodeStage(sortDag));
    }

    /** Walks to the deepest leaf stage (the data-node fragment) — used by tests that
     *  assert SHARD_SCAN behavior, which lives on the data-node side regardless of
     *  whether a coord stage sits above. */
    private static Stage dataNodeStage(QueryDAG dag) {
        Stage current = dag.rootStage();
        while (!current.getChildStages().isEmpty()) {
            current = current.getChildStages().get(0);
        }
        return current;
    }

    // ---- Composed pipeline shapes ----

    /** Aggregate(Filter(Scan)) — most common OLAP shape. */
    public void testAggregateOnFilteredScan() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(
            1,
            makeAggregate(
                makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
                sumCall()
            ),
            convertor
        );
        assertShardScanConverted(convertor, dataNodeStage(dag));
    }

    /** Sort(Aggregate(Filter(Scan))) with limit — full OLAP pipeline. */
    public void testSortOnAggregateOnFilteredScan() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(
            1,
            makeSort(
                makeAggregate(
                    makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
                    sumCall()
                ),
                10
            ),
            convertor
        );
        assertShardScanConverted(convertor, dataNodeStage(dag));
    }

    // ---- Two-stage shapes ----

    /**
     * Multi-shard Aggregate(Scan) — child calls convertFragment (shard-scan shape),
     * root calls convertFragment (final-agg shape).
     */
    public void testTwoStageAggregateConversion() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(2, makeAggregate(sumCall()), convertor);

        assertEquals(1, dag.rootStage().getChildStages().size());
        assertReduceStageConverted(convertor, dag.rootStage());
        assertShardScanConverted(convertor, dag.rootStage().getChildStages().getFirst());
    }

    /**
     * Multi-shard Sort(Aggregate(Filter(Scan))) with limit — full OLAP pipeline, two stages.
     */
    public void testTwoStageSortOnAggregateOnFilteredScan() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(
            2,
            makeSort(
                makeAggregate(
                    makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
                    sumCall()
                ),
                10
            ),
            convertor
        );

        assertEquals(1, dag.rootStage().getChildStages().size());
        assertReduceStageConverted(convertor, dag.rootStage());
        assertShardScanConverted(convertor, dag.rootStage().getChildStages().getFirst());
    }

    // ---- Multi-input (join) coord fragment shapes ----

    /**
     * Coord-side fragment: Aggregate ← Join ← (ER ← ...) | (ER ← ...).
     * Both branches are gathered subtrees. convertReduceNode must convert the whole Join +
     * branches + ERs + StageInputScans subtree in a single {@code convertFragment (final-agg shape)}
     * pass — same path as Union / Intersect / Minus. No substrait-level join stitching.
     */
    public void testJoinDirectlyOverTwoExchanges() {
        RecordingConvertor convertor = new RecordingConvertor();
        QueryDAG dag = buildAndConvert(2, buildJoinOverTwoScans("test_index", "test_index"), convertor);

        // Find the coord-side join stage — the stage whose fragment contains the Join with
        // two exchange-gathered branches. The whole subtree converts in one pass.
        Stage joinStage = findStageWithTwoChildren(dag.rootStage());
        assertNotNull("expected a stage with 2 child stages (the coord-side Join stage)", joinStage);
        assertNotNull("join stage alternative must have convertedBytes", joinStage.getPlanAlternatives().getFirst().convertedBytes());
        assertTrue("convertFragment (final-agg shape) must be called for the Join subtree", convertor.finalAggCalled);
    }

    private static Stage findStageWithTwoChildren(Stage stage) {
        if (stage.getChildStages().size() == 2) return stage;
        for (Stage child : stage.getChildStages()) {
            Stage found = findStageWithTwoChildren(child);
            if (found != null) return found;
        }
        return null;
    }

    /**
     * Coord-side Union with pass-through operators (Sort/Project) between each arm and its
     * ER. Isthmus's SubstraitRelVisitor handles Union natively; convertReduceNode converts
     * the whole Union subtree as one convertFragment (final-agg shape) call — same path as Join.
     */
    public void testUnionOverPassthroughThenExchange() {
        RecordingConvertor convertor = new RecordingConvertor();
        MockDataFusionBackend dfWithUnion = new MockDataFusionBackend() {
            @Override
            protected Set<EngineCapability> supportedEngineCapabilities() {
                Set<EngineCapability> caps = new java.util.HashSet<>(super.supportedEngineCapabilities());
                caps.add(EngineCapability.UNION);
                return caps;
            }

            @Override
            public org.opensearch.analytics.spi.FragmentConvertor getFragmentConvertor() {
                return convertor;
            }
        };
        PlannerContext context = buildContext("parquet", 2, intFields(), List.of(dfWithUnion));
        RelNode logical = buildUnionOverSortedAggArms();
        RelNode cboOutput = runPlanner(logical, context);
        LOGGER.info("Marked+CBO:\n{}", RelOptUtil.toString(cboOutput));
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());

        Stage root = dag.rootStage();
        assertNotNull("root alternative must have convertedBytes", root.getPlanAlternatives().getFirst().convertedBytes());
        assertTrue("convertFragment (final-agg shape) must be called for the Union subtree", convertor.finalAggCalled);
    }

    /**
     * Builds a minimal shape that reproduces the AppendPipeCommandIT plan: a LogicalUnion
     * over two arms, each arm being {@code Sort ← Project ← Aggregate ← Scan} of the same
     * multi-shard table. After CBO each arm carries an ER above its aggregate (PARTIAL/FINAL
     * split or SINGLE-over-RANDOM gather), and the Union sits at the coord with two
     * pass-through operators above each ER.
     */
    private RelNode buildUnionOverSortedAggArms() {
        RelNode arm1 = buildSortedAggArm();
        RelNode arm2 = buildSortedAggArm();
        return LogicalUnion.create(List.of(arm1, arm2), true);
    }

    private RelNode buildSortedAggArm() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode project = LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(scan, 0), rexBuilder.makeInputRef(scan, 1)),
            List.of("status", "size")
        );
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            project,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), false),
            "s"
        );
        RelNode agg = LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(0), null, List.of(sumCall));
        return LogicalSort.create(
            agg,
            org.apache.calcite.rel.RelCollations.of(new org.apache.calcite.rel.RelFieldCollation(0)),
            null,
            null
        );
    }

    /**
     * Builds an After-HEP shape close to JoinCommandIT#testInnerJoin: a top-level
     * count Aggregate over a Join of two separately-aggregated, separately-projected
     * scans. After CBO each join side carries an ER above its partial-agg subtree.
     */
    private RelNode buildJoinOverTwoScans(String leftTable, String rightTable) {
        RelOptTable left = mockTable(leftTable, "status", "size");
        RelOptTable right = mockTable(rightTable, "status", "size");

        RelNode leftScan = stubScan(left);
        RelNode leftProject = LogicalProject.create(
            leftScan,
            List.of(),
            List.of(rexBuilder.makeInputRef(leftScan, 0), rexBuilder.makeInputRef(leftScan, 1)),
            List.of("status", "size")
        );
        RelNode rightScan = stubScan(right);
        RelNode rightProject = LogicalProject.create(
            rightScan,
            List.of(),
            List.of(rexBuilder.makeInputRef(rightScan, 0), rexBuilder.makeInputRef(rightScan, 1)),
            List.of("status", "size")
        );
        RelNode rightSorted = LogicalSort.create(
            rightProject,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(50000, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );

        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        RelNode join = LogicalJoin.create(leftProject, rightSorted, List.of(), cond, Set.of(), JoinRelType.INNER);

        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            join,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        return LogicalAggregate.create(join, List.of(), ImmutableBitSet.of(), null, List.of(countCall));
    }

    // ---- Delegation tagging tests ----

    private static final SqlFunction MATCH_PHRASE_FUNCTION = new SqlFunction(
        "MATCH_PHRASE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private static final SqlFunction FUZZY_FUNCTION = new SqlFunction(
        "FUZZY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    /** Records serialization calls for delegation tests. */
    private static class RecordingSerializer implements DelegatedPredicateSerializer {
        int callCount;
        final List<String> serializedFunctions = new ArrayList<>();

        @Override
        public byte[] serialize(RexCall call, List<FieldStorageInfo> fieldStorage) {
            callCount++;
            serializedFunctions.add(call.getOperator().getName());
            return ("delegated:" + call.getOperator().getName()).getBytes(StandardCharsets.UTF_8);
        }
    }

    private List<AnalyticsSearchBackendPlugin> delegationBackends(RecordingConvertor dfConvertor, RecordingSerializer serializer) {
        MockDataFusionBackend df = new MockDataFusionBackend() {
            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public FragmentConvertor getFragmentConvertor() {
                return dfConvertor;
            }
        };
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            protected Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                map.put(ScalarFunction.MATCH, serializer);
                map.put(ScalarFunction.WILDCARD, serializer);
                map.put(ScalarFunction.REGEXP, serializer);
                // EQUALS registered for performance-delegation tests (dual-viable predicate that
                // stays on the operator backend but is also serializable by the Lucene peer).
                map.put(ScalarFunction.EQUALS, serializer);
                return map;
            }
        };
        return List.of(df, lucene);
    }

    private QueryDAG buildDelegationDag(
        RexNode condition,
        RecordingConvertor dfConvertor,
        RecordingSerializer serializer,
        String[] fieldNames,
        SqlTypeName[] fieldTypes,
        Map<String, Map<String, Object>> fields
    ) {
        var backends = delegationBackends(dfConvertor, serializer);
        var context = buildContext("parquet", fields, backends);
        LogicalFilter filter = LogicalFilter.create(stubScan(mockTable("test_index", fieldNames, fieldTypes)), condition);
        RelNode cboOutput = runPlanner(filter, context);
        LOGGER.info("Marked+CBO:\n{}", RelOptUtil.toString(cboOutput));
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        return dag;
    }

    /** Single-field delegation helper. */
    private QueryDAG buildSingleFieldDelegationDag(RexNode condition, RecordingConvertor dfConvertor, RecordingSerializer serializer) {
        return buildDelegationDag(
            condition,
            dfConvertor,
            serializer,
            new String[] { "message" },
            new SqlTypeName[] { SqlTypeName.VARCHAR },
            Map.of("message", Map.of("type", "keyword", "index", true))
        );
    }

    /**
     * Three-field delegation helper:
     *   - field 0 = status   (integer, indexed) — DUAL-VIABLE for Lucene + DataFusion.
     *   - field 1 = message  (keyword, indexed) — DUAL-VIABLE for full-text + DataFusion.
     *   - field 2 = amount   (integer, NOT indexed) — SINGLE-VIABLE to DataFusion only;
     *     Lucene declares EQUALS as a capability for indexed numerics, so an index=false
     *     field is the simplest way to test the truly-native code path without touching
     *     {@link MockLuceneBackend}'s capability declarations.
     */
    private QueryDAG buildTwoFieldDelegationDag(RexNode condition, RecordingConvertor dfConvertor, RecordingSerializer serializer) {
        return buildDelegationDag(
            condition,
            dfConvertor,
            serializer,
            new String[] { "status", "message", "amount" },
            new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.VARCHAR, SqlTypeName.INTEGER },
            Map.of(
                "status",
                Map.of("type", "integer", "index", true),
                "message",
                Map.of("type", "keyword", "index", true),
                "amount",
                Map.of("type", "integer", "index", false)
            )
        );
    }

    // ---- Shared delegation assertions ----

    private static Stage leafStage(QueryDAG dag) {
        Stage stage = dag.rootStage();
        while (!stage.getChildStages().isEmpty()) {
            stage = stage.getChildStages().getFirst();
        }
        return stage;
    }

    private void assertDelegationResult(
        StagePlan plan,
        RecordingConvertor dfConvertor,
        RecordingSerializer serializer,
        int expectedDelegatedCount,
        boolean expectPlaceholder,
        boolean expectNativeEquals,
        List<String> expectedFunctions,
        FilterTreeShape expectedTreeShape
    ) {
        assertEquals("delegatedQueries count", expectedDelegatedCount, plan.delegatedExpressions().size());
        assertEquals("serializer call count", expectedDelegatedCount, serializer.callCount);
        assertEquals("serialized functions", expectedFunctions, serializer.serializedFunctions);

        String strippedPlan = RelOptUtil.toString(dfConvertor.shardScanFragment);
        LOGGER.info("Stripped plan:\n{}", strippedPlan);

        if (expectPlaceholder) {
            assertTrue(
                "Stripped plan should contain " + DelegatedPredicateFunction.NAME,
                strippedPlan.contains(DelegatedPredicateFunction.NAME)
            );
            assertFalse("Stripped plan should not contain MATCH_PHRASE", strippedPlan.contains("MATCH_PHRASE"));
            assertFalse("Stripped plan should not contain FUZZY", strippedPlan.contains("FUZZY"));
        } else {
            assertFalse(
                "Stripped plan should not contain " + DelegatedPredicateFunction.NAME,
                strippedPlan.contains(DelegatedPredicateFunction.NAME)
            );
        }

        if (expectNativeEquals) {
            assertTrue("Stripped plan should contain native equals", strippedPlan.contains("="));
        }

        // No annotation markers should survive stripping
        assertDoesntContainOperators(dfConvertor.shardScanFragment, ANNOTATION_MARKERS);

        // Instruction assertions: delegation plans must have SHARD_SCAN + FILTER_DELEGATION_FOR_INDEX
        if (expectedDelegatedCount > 0) {
            assertTrue(
                "delegation plan must have SHARD_SCAN_WITH_DELEGATION instruction",
                plan.instructions().stream().anyMatch(node -> node.type() == InstructionType.SETUP_SHARD_SCAN_WITH_DELEGATION)
            );
            ShardScanWithDelegationInstructionNode filterInstruction = (ShardScanWithDelegationInstructionNode) plan.instructions()
                .stream()
                .filter(node -> node.type() == InstructionType.SETUP_SHARD_SCAN_WITH_DELEGATION)
                .findFirst()
                .orElseThrow();
            assertEquals("delegatedPredicateCount in instruction", expectedDelegatedCount, filterInstruction.getDelegatedPredicateCount());
            assertEquals(
                "delegatedPredicateCount matches delegatedExpressions size",
                plan.delegatedExpressions().size(),
                filterInstruction.getDelegatedPredicateCount()
            );
            assertEquals("treeShape in instruction", expectedTreeShape, filterInstruction.getTreeShape());
        }
    }

    // ---- Single predicate ----

    /** Single delegated MATCH_PHRASE — replaced with placeholder, one entry in delegatedQueries. */
    public void testSingleDelegatedPredicate() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        QueryDAG dag = buildSingleFieldDelegationDag(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello world"), dfConvertor, serializer);
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertDelegationResult(plan, dfConvertor, serializer, 1, true, false, List.of("MATCH_PHRASE"), FilterTreeShape.CONJUNCTIVE);
    }

    /** Single native equals on a non-indexed field — single-viable to DataFusion, no delegation. */
    public void testSingleNativePredicate() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        // amount is index=false → only DataFusion can evaluate, Lucene is not viable.
        QueryDAG dag = buildTwoFieldDelegationDag(makeEquals(2, SqlTypeName.INTEGER, 200), dfConvertor, serializer);
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertDelegationResult(plan, dfConvertor, serializer, 0, false, true, List.of(), FilterTreeShape.NO_DELEGATION);
    }

    /**
     * Single dual-viable equals on an indexed integer field — DataFusion narrows the operator
     * but Lucene was also viable, so FragmentConversion wraps the predicate with the
     * {@code delegation_possible(original, annotationId)} marker for opportunistic per-RG
     * consultation. Asserts one delegated entry, CONJUNCTIVE shape, and that the original
     * predicate survives in the plan alongside the wrapper (unlike correctness delegation,
     * which replaces the original with a {@code delegated_predicate} placeholder).
     */
    public void testSinglePerformanceDelegatedPredicate() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        // status is index=true → both DataFusion and Lucene viable → performance-delegation.
        QueryDAG dag = buildTwoFieldDelegationDag(makeEquals(0, SqlTypeName.INTEGER, 200), dfConvertor, serializer);
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();

        // Wire-shape: one delegated expression (for the peer to compile lazily), serialized
        // by the recording serializer. Operator name comes from SqlBinaryOperator.getName() = "=".
        assertEquals("delegatedQueries count", 1, plan.delegatedExpressions().size());
        assertEquals("serializer call count", 1, serializer.callCount);
        assertEquals("serialized functions", List.of("="), serializer.serializedFunctions);

        // Tree shape carries CONJUNCTIVE so the driving backend knows about the performance leaf.
        ShardScanWithDelegationInstructionNode delegationInstruction = (ShardScanWithDelegationInstructionNode) plan.instructions()
            .stream()
            .filter(node -> node.type() == InstructionType.SETUP_SHARD_SCAN_WITH_DELEGATION)
            .findFirst()
            .orElseThrow(() -> new AssertionError("performance-delegation plan must have SHARD_SCAN_WITH_DELEGATION"));
        assertEquals("FilterTreeShape", FilterTreeShape.CONJUNCTIVE, delegationInstruction.getTreeShape());
        assertEquals("delegatedPredicateCount", 1, delegationInstruction.getDelegatedPredicateCount());

        // Plan-shape: delegation_possible wraps the original; native = survives.
        // (Correctness delegation would replace with delegated_predicate placeholder; perf preserves.)
        String strippedPlan = RelOptUtil.toString(dfConvertor.shardScanFragment);
        assertTrue(
            "Stripped plan should contain " + DelegationPossibleFunction.NAME,
            strippedPlan.contains(DelegationPossibleFunction.NAME)
        );
        assertFalse(
            "Stripped plan should NOT contain " + DelegatedPredicateFunction.NAME + " (this is performance, not correctness)",
            strippedPlan.contains(DelegatedPredicateFunction.NAME)
        );
        assertTrue("Stripped plan should still contain the native '=' (preserved by the wrapper)", strippedPlan.contains("="));
        assertDoesntContainOperators(dfConvertor.shardScanFragment, ANNOTATION_MARKERS);
    }

    // ---- AND conditions ----

    /** AND(native, delegated) — equals on non-indexed amount stays native; MATCH_PHRASE replaced. */
    public void testAndNativeAndDelegated() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        QueryDAG dag = buildTwoFieldDelegationDag(
            makeAnd(makeEquals(2, SqlTypeName.INTEGER, 200), makeFullTextCall(MATCH_PHRASE_FUNCTION, 1, "timeout error")),
            dfConvertor,
            serializer
        );
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertDelegationResult(plan, dfConvertor, serializer, 1, true, true, List.of("MATCH_PHRASE"), FilterTreeShape.CONJUNCTIVE);
    }

    /** AND(delegated, delegated) — both replaced, two entries in delegatedQueries. */
    public void testAndTwoDelegated() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        QueryDAG dag = buildSingleFieldDelegationDag(
            makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello"), makeFullTextCall(FUZZY_FUNCTION, 0, "wrld")),
            dfConvertor,
            serializer
        );
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertDelegationResult(
            plan,
            dfConvertor,
            serializer,
            2,
            true,
            false,
            List.of("MATCH_PHRASE", "FUZZY"),
            FilterTreeShape.CONJUNCTIVE
        );
    }

    /** AND(delegated, delegated) with combining support — combined into single DelegatedExpression. */
    public void testAndTwoDelegatedWithCombining() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        // Override the mock Lucene backend to support combining
        MockDataFusionBackend df = new MockDataFusionBackend() {
            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public FragmentConvertor getFragmentConvertor() {
                return dfConvertor;
            }
        };
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            protected Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                return map;
            }

            @Override
            public byte[] combineDelegatedPredicates(java.util.List<byte[]> predicates, org.apache.calcite.sql.SqlKind kind) {
                StringBuilder sb = new StringBuilder("combined:");
                for (byte[] b : predicates) {
                    sb.append(new String(b, java.nio.charset.StandardCharsets.UTF_8)).append("+");
                }
                return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
        };
        var backends = List.<AnalyticsSearchBackendPlugin>of(df, lucene);
        Map<String, Map<String, Object>> fields = Map.of("message", Map.of("type", "keyword", "index", true));
        var context = buildContext("parquet", fields, backends);
        RexNode condition = makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello"), makeFullTextCall(FUZZY_FUNCTION, 0, "wrld"));
        LogicalFilter filter = LogicalFilter.create(
            stubScan(mockTable("test_index", new String[] { "message" }, new SqlTypeName[] { SqlTypeName.VARCHAR })),
            condition
        );
        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();

        // Should produce 1 combined DelegatedExpression (not 2 individual ones)
        assertEquals("Should have 1 combined delegated expression", 1, plan.delegatedExpressions().size());
        // Serializer was called twice (once per predicate)
        assertEquals("serializer call count", 2, serializer.callCount);
        // The combined bytes should contain both predicates
        String combinedStr = new String(
            plan.delegatedExpressions().getFirst().getExpressionBytes(),
            java.nio.charset.StandardCharsets.UTF_8
        );
        assertTrue("Combined should contain MATCH_PHRASE", combinedStr.contains("MATCH_PHRASE"));
        assertTrue("Combined should contain FUZZY", combinedStr.contains("FUZZY"));
    }

    /** OR(delegated, delegated) with combining support — combined into single DelegatedExpression with OR structure. */
    public void testOrTwoDelegatedWithCombining() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        // Override the mock Lucene backend to support combining
        MockDataFusionBackend df = new MockDataFusionBackend() {
            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public FragmentConvertor getFragmentConvertor() {
                return dfConvertor;
            }
        };
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            protected Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                return map;
            }

            @Override
            public byte[] combineDelegatedPredicates(java.util.List<byte[]> predicates, org.apache.calcite.sql.SqlKind kind) {
                assertEquals("Should be OR", org.apache.calcite.sql.SqlKind.OR, kind);
                assertEquals("Should have 2 predicates", 2, predicates.size());
                StringBuilder sb = new StringBuilder("or_combined:");
                for (byte[] b : predicates) {
                    sb.append(new String(b, java.nio.charset.StandardCharsets.UTF_8)).append("+");
                }
                return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
        };
        var backends = List.<AnalyticsSearchBackendPlugin>of(df, lucene);
        Map<String, Map<String, Object>> fields = Map.of("message", Map.of("type", "keyword", "index", true));
        var context = buildContext("parquet", fields, backends);
        RexNode condition = rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.OR,
            makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello"),
            makeFullTextCall(FUZZY_FUNCTION, 0, "wrld")
        );
        LogicalFilter filter = LogicalFilter.create(
            stubScan(mockTable("test_index", new String[] { "message" }, new SqlTypeName[] { SqlTypeName.VARCHAR })),
            condition
        );
        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();

        // OR siblings targeting same backend should be combined into 1 DelegatedExpression
        assertEquals("Should have 1 combined delegated expression", 1, plan.delegatedExpressions().size());
        assertEquals("serializer call count", 2, serializer.callCount);
        String combinedStr = new String(
            plan.delegatedExpressions().getFirst().getExpressionBytes(),
            java.nio.charset.StandardCharsets.UTF_8
        );
        assertTrue("Combined should contain MATCH_PHRASE", combinedStr.contains("MATCH_PHRASE"));
        assertTrue("Combined should contain FUZZY", combinedStr.contains("FUZZY"));
    }

    // ---- Complex combining cases ----

    /** OR(AND(delegated, delegated), delegated) — pure Lucene nested structure → 1 combined. */
    public void testOrOfAndDelegated_PureLucene() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        MockDataFusionBackend df = new MockDataFusionBackend() {
            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public FragmentConvertor getFragmentConvertor() {
                return dfConvertor;
            }
        };
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            protected Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                return map;
            }

            @Override
            public byte[] combineDelegatedPredicates(java.util.List<byte[]> predicates, org.apache.calcite.sql.SqlKind kind) {
                StringBuilder sb = new StringBuilder("combined(" + kind + "):");
                for (byte[] b : predicates)
                    sb.append(new String(b, java.nio.charset.StandardCharsets.UTF_8)).append("+");
                return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
        };
        var backends = List.<AnalyticsSearchBackendPlugin>of(df, lucene);
        Map<String, Map<String, Object>> fields = Map.of("message", Map.of("type", "keyword", "index", true));
        var context = buildContext("parquet", fields, backends);
        // OR(AND(match_phrase, fuzzy), match_phrase)
        RexNode condition = rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.OR,
            makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello"), makeFullTextCall(FUZZY_FUNCTION, 0, "wrld")),
            makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "ru")
        );
        LogicalFilter filter = LogicalFilter.create(
            stubScan(mockTable("test_index", new String[] { "message" }, new SqlTypeName[] { SqlTypeName.VARCHAR })),
            condition
        );
        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();

        assertEquals("Pure Lucene nested OR(AND,leaf) should produce 1 expression", 1, plan.delegatedExpressions().size());
        assertEquals("serializer call count", 3, serializer.callCount);
    }

    /** OR(AND(a,b), AND(c,d)) — pure Lucene two AND groups under OR → 1 combined. */
    public void testOrOfTwoAndGroups_PureLucene() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        MockDataFusionBackend df = new MockDataFusionBackend() {
            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public FragmentConvertor getFragmentConvertor() {
                return dfConvertor;
            }
        };
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            protected Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                return map;
            }

            @Override
            public byte[] combineDelegatedPredicates(java.util.List<byte[]> predicates, org.apache.calcite.sql.SqlKind kind) {
                StringBuilder sb = new StringBuilder("combined(" + kind + "):");
                for (byte[] b : predicates)
                    sb.append(new String(b, java.nio.charset.StandardCharsets.UTF_8)).append("+");
                return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
        };
        var backends = List.<AnalyticsSearchBackendPlugin>of(df, lucene);
        Map<String, Map<String, Object>> fields = Map.of("message", Map.of("type", "keyword", "index", true));
        var context = buildContext("parquet", fields, backends);
        // OR(AND(match_phrase, fuzzy), AND(match_phrase, fuzzy))
        RexNode condition = rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.OR,
            makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello"), makeFullTextCall(FUZZY_FUNCTION, 0, "wrld")),
            makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "ru"), makeFullTextCall(FUZZY_FUNCTION, 0, "typo"))
        );
        LogicalFilter filter = LogicalFilter.create(
            stubScan(mockTable("test_index", new String[] { "message" }, new SqlTypeName[] { SqlTypeName.VARCHAR })),
            condition
        );
        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();

        assertEquals("OR(AND,AND) pure Lucene should produce 1 expression", 1, plan.delegatedExpressions().size());
        assertEquals("serializer call count", 4, serializer.callCount);
    }

    /** AND(OR(AND(a,b), c), d) — pure Lucene deep nesting → 1 combined. */
    public void testAndOrAndNested_PureLucene() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        MockDataFusionBackend df = new MockDataFusionBackend() {
            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public FragmentConvertor getFragmentConvertor() {
                return dfConvertor;
            }
        };
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            protected Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                return map;
            }

            @Override
            public byte[] combineDelegatedPredicates(java.util.List<byte[]> predicates, org.apache.calcite.sql.SqlKind kind) {
                StringBuilder sb = new StringBuilder("combined(" + kind + "):");
                for (byte[] b : predicates)
                    sb.append(new String(b, java.nio.charset.StandardCharsets.UTF_8)).append("+");
                return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
        };
        var backends = List.<AnalyticsSearchBackendPlugin>of(df, lucene);
        Map<String, Map<String, Object>> fields = Map.of("message", Map.of("type", "keyword", "index", true));
        var context = buildContext("parquet", fields, backends);
        // AND(OR(AND(match_phrase, fuzzy), match_phrase), fuzzy)
        RexNode orClause = rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.OR,
            makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello"), makeFullTextCall(FUZZY_FUNCTION, 0, "wrld")),
            makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "ru")
        );
        RexNode condition = makeAnd(orClause, makeFullTextCall(FUZZY_FUNCTION, 0, "http"));
        LogicalFilter filter = LogicalFilter.create(
            stubScan(mockTable("test_index", new String[] { "message" }, new SqlTypeName[] { SqlTypeName.VARCHAR })),
            condition
        );
        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();

        assertEquals("AND(OR(AND,leaf),leaf) pure Lucene should produce 1 expression", 1, plan.delegatedExpressions().size());
        assertEquals("serializer call count", 4, serializer.callCount);
    }

    /** AND(OR(AND(a,b), c), d, native) — mixed: Lucene subtree combined + native preserved. */
    public void testMixedAndOrAndWithNative() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        MockDataFusionBackend df = new MockDataFusionBackend() {
            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public FragmentConvertor getFragmentConvertor() {
                return dfConvertor;
            }
        };
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            protected Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                return map;
            }

            @Override
            public byte[] combineDelegatedPredicates(java.util.List<byte[]> predicates, org.apache.calcite.sql.SqlKind kind) {
                StringBuilder sb = new StringBuilder("combined(" + kind + "):");
                for (byte[] b : predicates)
                    sb.append(new String(b, java.nio.charset.StandardCharsets.UTF_8)).append("+");
                return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
        };
        var backends = List.<AnalyticsSearchBackendPlugin>of(df, lucene);
        // message=keyword(indexed), amount=integer(NOT indexed → native only)
        Map<String, Map<String, Object>> fields = Map.of(
            "message",
            Map.of("type", "keyword", "index", true),
            "amount",
            Map.of("type", "integer", "index", false)
        );
        var context = buildContext("parquet", fields, backends);
        // AND(OR(AND(match_phrase, fuzzy), match_phrase), fuzzy, amount=200)
        RexNode orClause = rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.OR,
            makeAnd(makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello"), makeFullTextCall(FUZZY_FUNCTION, 0, "wrld")),
            makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "ru")
        );
        RexNode condition = makeAnd(orClause, makeFullTextCall(FUZZY_FUNCTION, 0, "http"), makeEquals(1, SqlTypeName.INTEGER, 200));
        LogicalFilter filter = LogicalFilter.create(
            stubScan(
                mockTable(
                    "test_index",
                    new String[] { "message", "amount" },
                    new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER }
                )
            ),
            condition
        );
        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();

        // All 4 Lucene predicates combined into 1, native amount=200 stays separate
        assertEquals("Mixed: Lucene combined into 1 expression", 1, plan.delegatedExpressions().size());
        assertEquals("serializer call count", 4, serializer.callCount);
        // Verify the stripped plan still has AND (placeholder + native)
        assertTrue("AND should be preserved", RelOptUtil.toString(dfConvertor.shardScanFragment).contains("AND"));
    }

    // ---- Combining: shared helper ----

    /** Result holder for combining tests. */
    private record CombiningResult(StagePlan plan, RecordingConvertor convertor, RecordingSerializer serializer, List<String> combineLog) {
    }

    private static final SqlFunction WILDCARD_FN = new SqlFunction(
        "WILDCARD",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );
    private static final SqlFunction REGEXP_FN = new SqlFunction(
        "REGEXP",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    /**
     * Builds and runs the combining pipeline for a filter condition.
     * Uses 3 fields: message(keyword,indexed), amount(int,NOT indexed), count(int,NOT indexed).
     * Lucene accepts delegation for MATCH_PHRASE, FUZZY, WILDCARD, REGEXP.
     */
    private CombiningResult runCombining(RexNode condition) {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        List<String> combineLog = new ArrayList<>();
        MockDataFusionBackend df = new MockDataFusionBackend() {
            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public FragmentConvertor getFragmentConvertor() {
                return dfConvertor;
            }
        };
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            protected Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                Map<ScalarFunction, DelegatedPredicateSerializer> map = new HashMap<>(super.delegatedPredicateSerializers());
                map.put(ScalarFunction.MATCH_PHRASE, serializer);
                map.put(ScalarFunction.FUZZY, serializer);
                map.put(ScalarFunction.WILDCARD, serializer);
                map.put(ScalarFunction.REGEXP, serializer);
                return map;
            }

            @Override
            public byte[] combineDelegatedPredicates(java.util.List<byte[]> predicates, org.apache.calcite.sql.SqlKind kind) {
                combineLog.add(kind + ":" + predicates.size());
                StringBuilder sb = new StringBuilder("combined(" + kind + "):");
                for (byte[] b : predicates)
                    sb.append(new String(b, java.nio.charset.StandardCharsets.UTF_8)).append("+");
                return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
        };
        var backends = List.<AnalyticsSearchBackendPlugin>of(df, lucene);
        Map<String, Map<String, Object>> fields = Map.of(
            "message",
            Map.of("type", "keyword", "index", true),
            "amount",
            Map.of("type", "integer", "index", false),
            "count",
            Map.of("type", "integer", "index", false)
        );
        var context = buildContext("parquet", fields, backends);
        LogicalFilter filter = LogicalFilter.create(
            stubScan(
                mockTable(
                    "test_index",
                    new String[] { "message", "amount", "count" },
                    new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.INTEGER, SqlTypeName.INTEGER }
                )
            ),
            condition
        );
        RelNode cboOutput = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry());
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        return new CombiningResult(plan, dfConvertor, serializer, combineLog);
    }

    private RexNode matchPhrase(String query) {
        return makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, query);
    }

    private RexNode fuzzy(String query) {
        return makeFullTextCall(FUZZY_FUNCTION, 0, query);
    }

    private RexNode wildcard(String pattern) {
        return makeFullTextCall(WILDCARD_FN, 0, pattern);
    }

    private RexNode regexp(String pattern) {
        return makeFullTextCall(REGEXP_FN, 0, pattern);
    }

    private RexNode amountEquals(int val) {
        return makeEquals(1, SqlTypeName.INTEGER, val);
    }

    private RexNode countEquals(int val) {
        return makeEquals(2, SqlTypeName.INTEGER, val);
    }

    private RexNode or(RexNode... ops) {
        return rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.OR, ops);
    }

    private RexNode not(RexNode op) {
        return rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT, op);
    }

    private FilterTreeShape treeShapeOf(StagePlan plan) {
        return ((ShardScanWithDelegationInstructionNode) plan.instructions()
            .stream()
            .filter(n -> n.type() == InstructionType.SETUP_SHARD_SCAN_WITH_DELEGATION)
            .findFirst()
            .orElseThrow()).getTreeShape();
    }

    // ---- Combining tests (OR/NOT/mixed) ----

    /** match_phrase AND fuzzy OR amount=200 → OR(AND(lucene,lucene), native) — combined, INTERLEAVED. */
    public void testCombine_OrOfAndLuceneWithNative() {
        var r = runCombining(or(makeAnd(matchPhrase("hello"), fuzzy("wrld")), amountEquals(200)));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(2, r.serializer.callCount);
        assertTrue(r.combineLog.contains("AND:2"));
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** OR(AND(match_phrase, fuzzy), AND(amount=100, count=200)) — one Lucene arm combined, one native arm. */
    public void testCombine_OrOfAndLuceneWithAndNative() {
        var r = runCombining(or(makeAnd(matchPhrase("hello"), fuzzy("wrld")), makeAnd(amountEquals(100), countEquals(200))));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(2, r.serializer.callCount);
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** AND(OR(match_phrase, amount=200), fuzzy) — match_phrase can't merge with fuzzy across OR → 2 expressions. */
    public void testCombine_AndWithOrContainingMixed() {
        var r = runCombining(makeAnd(or(matchPhrase("hello"), amountEquals(200)), fuzzy("wrld")));
        assertEquals(2, r.plan.delegatedExpressions().size());
        assertEquals(2, r.serializer.callCount);
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** OR(AND(match_phrase, fuzzy), AND(wildcard, amount=200)) — left arm combined, right has wildcard separate. */
    public void testCombine_OrOfPureLuceneAndMixedAnd() {
        var r = runCombining(or(makeAnd(matchPhrase("hello"), fuzzy("wrld")), makeAnd(wildcard("h*llo"), amountEquals(200))));
        assertEquals(2, r.plan.delegatedExpressions().size());
        assertEquals(3, r.serializer.callCount);
        assertTrue(r.combineLog.contains("AND:2"));
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** OR(AND(match_phrase, fuzzy, wildcard), amount=200) — all 3 Lucene AND-combined, then OR with native. */
    public void testCombine_OrOfThreeLuceneAndNative() {
        var r = runCombining(or(makeAnd(matchPhrase("hello"), fuzzy("wrld"), wildcard("h*llo")), amountEquals(200)));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(3, r.serializer.callCount);
        assertTrue(r.combineLog.contains("AND:3"));
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** OR(AND(match_phrase, fuzzy), AND(wildcard, regexp)) — pure Lucene both sides, fully combined to 1. */
    public void testCombine_OrOfTwoAndGroupsPureLucene() {
        var r = runCombining(or(makeAnd(matchPhrase("hello"), fuzzy("wrld")), makeAnd(wildcard("h*llo"), regexp("h.llo"))));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(4, r.serializer.callCount);
        assertEquals(2, r.combineLog.stream().filter(s -> s.equals("AND:2")).count());
        assertTrue(r.combineLog.contains("OR:2"));
    }

    /** OR(match_phrase, amount=200, fuzzy) — flat interleaved OR. */
    public void testCombine_OrFlatInterleaved() {
        var r = runCombining(or(matchPhrase("hello"), amountEquals(200), fuzzy("wrld")));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(2, r.serializer.callCount);
        assertTrue(r.combineLog.contains("OR:2"));
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** OR(match_phrase, amount=100, fuzzy, count=200, wildcard) — 3 Lucene combined, 2 native preserved. */
    public void testCombine_OrFlatFivePredicates() {
        var r = runCombining(or(matchPhrase("hello"), amountEquals(100), fuzzy("wrld"), countEquals(200), wildcard("h*")));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(3, r.serializer.callCount);
        assertTrue(r.combineLog.contains("OR:3"));
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** AND(match_phrase, OR(fuzzy, amount=100), wildcard) — fuzzy materialized inside OR, match_phrase+wildcard may combine at AND. */
    public void testCombine_AndLuceneOrMixedLucene() {
        var r = runCombining(makeAnd(matchPhrase("hello"), or(fuzzy("wrld"), amountEquals(100)), wildcard("h*")));
        assertTrue("at least 1 expression", r.plan.delegatedExpressions().size() >= 1);
        assertEquals(3, r.serializer.callCount);
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** NOT(match_phrase) OR amount=200 — NOT wraps delegated, OR with native, INTERLEAVED. */
    public void testCombine_OrNotLuceneWithNative() {
        var r = runCombining(or(not(matchPhrase("hello")), amountEquals(200)));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(1, r.serializer.callCount);
        assertTrue(r.combineLog.contains("NOT:1"));
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** (match_phrase AND fuzzy AND NOT(wildcard)) OR amount=200 — AND combines all 3, then OR with native. */
    public void testCombine_OrOfAndWithNotAndNative() {
        var r = runCombining(or(makeAnd(matchPhrase("hello"), fuzzy("wrld"), not(wildcard("h*"))), amountEquals(200)));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(3, r.serializer.callCount);
        assertTrue(r.combineLog.contains("NOT:1"));
        assertTrue(r.combineLog.contains("AND:3"));
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** OR(NOT(match_phrase), NOT(fuzzy), amount=200) — both NOTs combine under OR, native stays. */
    public void testCombine_OrTwoNotsWithNative() {
        var r = runCombining(or(not(matchPhrase("hello")), not(fuzzy("wrld")), amountEquals(200)));
        assertEquals(1, r.plan.delegatedExpressions().size());
        assertEquals(2, r.serializer.callCount);
        assertEquals(2, r.combineLog.stream().filter(s -> s.equals("NOT:1")).count());
        assertTrue(r.combineLog.contains("OR:2"));
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    /** AND(OR(match_phrase, fuzzy), OR(wildcard, amount=200)) — first OR bubbles up, second is mixed. */
    public void testCombine_AndOfTwoOrs_OnePureOneMixed() {
        var r = runCombining(makeAnd(or(matchPhrase("hello"), fuzzy("wrld")), or(wildcard("h*"), amountEquals(200))));
        assertEquals(2, r.plan.delegatedExpressions().size());
        assertEquals(3, r.serializer.callCount);
        assertTrue(r.combineLog.contains("OR:2"));
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, treeShapeOf(r.plan));
    }

    // ---- OR conditions ----

    /** OR(native, delegated) — equals on non-indexed amount stays native; MATCH_PHRASE replaced. */
    public void testOrNativeAndDelegated() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        QueryDAG dag = buildTwoFieldDelegationDag(
            rexBuilder.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.OR,
                makeEquals(2, SqlTypeName.INTEGER, 200),
                makeFullTextCall(MATCH_PHRASE_FUNCTION, 1, "timeout error")
            ),
            dfConvertor,
            serializer
        );
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertDelegationResult(
            plan,
            dfConvertor,
            serializer,
            1,
            true,
            true,
            List.of("MATCH_PHRASE"),
            FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION
        );
        assertTrue("OR structure should be preserved", RelOptUtil.toString(dfConvertor.shardScanFragment).contains("OR"));
    }

    // ---- Interleaved AND/OR/NOT ----

    /** AND(native, OR(delegated, NOT(delegated))) — nested boolean structure with delegation. */
    public void testInterleavedAndOrNot() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        RecordingSerializer serializer = new RecordingSerializer();
        RexNode notFuzzy = rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT,
            makeFullTextCall(FUZZY_FUNCTION, 1, "wrld")
        );
        RexNode orClause = rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.OR,
            makeFullTextCall(MATCH_PHRASE_FUNCTION, 1, "timeout error"),
            notFuzzy
        );
        // amount is index=false → equals stays native; only MATCH_PHRASE + FUZZY get delegated.
        RexNode condition = makeAnd(makeEquals(2, SqlTypeName.INTEGER, 200), orClause);
        QueryDAG dag = buildTwoFieldDelegationDag(condition, dfConvertor, serializer);
        StagePlan plan = leafStage(dag).getPlanAlternatives().getFirst();
        assertDelegationResult(
            plan,
            dfConvertor,
            serializer,
            2,
            true,
            true,
            List.of("MATCH_PHRASE", "FUZZY"),
            FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION
        );
        String strippedPlan = RelOptUtil.toString(dfConvertor.shardScanFragment);
        assertTrue("AND structure should be preserved", strippedPlan.contains("AND"));
        assertTrue("OR structure should be preserved", strippedPlan.contains("OR"));
        assertTrue("NOT structure should be preserved", strippedPlan.contains("NOT"));
    }

    // ---- Error paths ----

    /** Delegated annotation with no serializer registered → IllegalStateException. */
    public void testMissingSerializerThrows() {
        RecordingConvertor dfConvertor = new RecordingConvertor();
        // Lucene mock accepts delegation but has NO serializers at all
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }
        };
        MockDataFusionBackend df = new MockDataFusionBackend() {
            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public FragmentConvertor getFragmentConvertor() {
                return dfConvertor;
            }
        };
        Map<String, Map<String, Object>> fields = Map.of("message", Map.of("type", "keyword", "index", true));
        PlannerContext context = buildContext("parquet", fields, List.of(df, lucene));
        LogicalFilter filter = LogicalFilter.create(
            stubScan(mockTable("test_index", new String[] { "message" }, new SqlTypeName[] { SqlTypeName.VARCHAR })),
            makeFullTextCall(MATCH_PHRASE_FUNCTION, 0, "hello world")
        );
        RelNode marked = runPlanner(filter, context);
        QueryDAG dag = DAGBuilder.build(marked, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> FragmentConversionDriver.convertAll(dag, context.getCapabilityRegistry())
        );
        assertTrue(exception.getMessage().contains("No DelegatedPredicateSerializer"));
        assertTrue(exception.getMessage().contains("MATCH_PHRASE"));
    }

    // ---- RecordingConvertor ----

    /** Records which convertor method was called and what was passed. */
    private static class RecordingConvertor implements FragmentConvertor {
        boolean shardScanCalled;
        boolean finalAggCalled;
        String shardScanTableName;
        RelNode shardScanFragment;
        RelNode reduceFragment;

        @Override
        public byte[] convertFragment(RelNode fragment) {
            // Distinguish shard-scan vs reduce/final by walking down the leftmost spine
            // to find a TableScan-shaped leaf (annotations are stripped before this is
            // called, so OpenSearchTableScan has been rewritten to LogicalTableScan).
            org.apache.calcite.rel.core.TableScan scan = org.opensearch.analytics.planner.RelNodeUtils.findNode(
                fragment,
                org.apache.calcite.rel.core.TableScan.class
            );
            if (scan != null) {
                this.shardScanCalled = true;
                this.shardScanTableName = scan.getTable().getQualifiedName().getLast();
                this.shardScanFragment = fragment;
                return ("shard:" + this.shardScanTableName).getBytes(StandardCharsets.UTF_8);
            }
            this.finalAggCalled = true;
            this.reduceFragment = fragment;
            return "reduce".getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
            return ("attach:" + new String(innerBytes, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
            return ("partialAgg:" + new String(innerBytes, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
        }
    }
}
