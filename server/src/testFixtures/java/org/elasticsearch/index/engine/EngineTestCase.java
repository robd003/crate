/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import static java.util.Collections.emptyList;
import static java.util.Collections.shuffle;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PEER_RECOVERY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.REPLICA;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToLongBiFunction;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.fieldvisitor.IDVisitor;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SequenceIDFields;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;

import io.crate.common.io.IOUtils;
import io.crate.common.unit.TimeValue;
import io.crate.execution.dml.StringIndexer;
import io.crate.metadata.doc.SysColumns;

public abstract class EngineTestCase extends ESTestCase {

    protected final ShardId shardId = new ShardId(new Index("index", "_na_"), 0);
    protected final AllocationId allocationId = AllocationId.newInitializing();
    protected static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);

    protected ThreadPool threadPool;
    protected TranslogHandler translogHandler;

    protected Store store;
    protected Store storeReplica;

    protected InternalEngine engine;
    protected InternalEngine replicaEngine;

    protected IndexSettings defaultSettings;
    protected String codecName;
    protected Path primaryTranslogDir;
    protected Path replicaTranslogDir;
    // A default primary term is used by engine instances created in this test.
    protected final PrimaryTermSupplier primaryTerm = new PrimaryTermSupplier(1L);

    protected Settings indexSettings() {
        // TODO randomize more settings
        return Settings.builder()
            .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), "1h") // make sure this doesn't kick in on us
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), codecName)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.getKey(),
                between(10, 10 * IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.get(Settings.EMPTY)))
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(),
                randomBoolean() ? IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.get(Settings.EMPTY) : between(0, 1000))
            .build();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        primaryTerm.set(randomLongBetween(1, Long.MAX_VALUE));
        CodecService codecService = new CodecService();
        String name = Codec.getDefault().getName();
        if (Arrays.asList(codecService.availableCodecs()).contains(name)) {
            // some codecs are read only so we only take the ones that we have in the service and randomly
            // selected by lucene test case.
            codecName = name;
        } else {
            codecName = "default";
        }
        defaultSettings = IndexSettingsModule.newIndexSettings("test", indexSettings());
        threadPool = new TestThreadPool(getClass().getName());
        store = createStore();
        storeReplica = createStore();
        Lucene.cleanLuceneIndex(store.directory());
        Lucene.cleanLuceneIndex(storeReplica.directory());
        primaryTranslogDir = createTempDir("translog-primary");
        translogHandler = createTranslogHandler(defaultSettings);
        engine = createEngine(store, primaryTranslogDir);
        LiveIndexWriterConfig currentIndexWriterConfig = engine.getCurrentIndexWriterConfig();

        assertThat(codecService.codec(codecName).getName()).isEqualTo(engine.config().getCodec().getName());
        assertThat(codecService.codec(codecName).getName()).isEqualTo(currentIndexWriterConfig.getCodec().getName());
        if (randomBoolean()) {
            engine.config().setEnableGcDeletes(false);
        }
        replicaTranslogDir = createTempDir("translog-replica");
        replicaEngine = createEngine(storeReplica, replicaTranslogDir);
        currentIndexWriterConfig = replicaEngine.getCurrentIndexWriterConfig();

        assertThat(codecService.codec(codecName).getName()).isEqualTo(replicaEngine.config().getCodec().getName());
        assertThat(codecService.codec(codecName).getName()).isEqualTo(currentIndexWriterConfig.getCodec().getName());
        if (randomBoolean()) {
            engine.config().setEnableGcDeletes(false);
        }
    }

    public EngineConfig copy(EngineConfig config, LongSupplier globalCheckpointSupplier) {
        return new EngineConfig(
            config.getShardId(),
            config.getThreadPool(),
            config.getIndexSettings(),
            config.getStore(),
            config.getMergePolicy(),
            config.getAnalyzer(),
            new CodecService(),
            config.getEventListener(),
            config.getQueryCache(),
            config.getQueryCachingPolicy(),
            config.getTranslogConfig(),
            config.getFlushMergesAfter(),
            config.getExternalRefreshListeners(),
            Collections.emptyList(),
            config.getCircuitBreakerService(),
            globalCheckpointSupplier,
            config.retentionLeasesSupplier(),
            config.getPrimaryTermSupplier(),
            tombstoneDocSupplier()
        );
    }

    public EngineConfig copy(EngineConfig config, Analyzer analyzer) {
        return new EngineConfig(
            config.getShardId(),
            config.getThreadPool(),
            config.getIndexSettings(),
            config.getStore(),
            config.getMergePolicy(),
            analyzer,
            new CodecService(),
            config.getEventListener(),
            config.getQueryCache(),
            config.getQueryCachingPolicy(),
            config.getTranslogConfig(),
            config.getFlushMergesAfter(),
            config.getExternalRefreshListeners(),
            Collections.emptyList(),
            config.getCircuitBreakerService(),
            config.getGlobalCheckpointSupplier(),
            config.retentionLeasesSupplier(),
            config.getPrimaryTermSupplier(),
            config.getTombstoneDocSupplier());
    }

    public EngineConfig copy(EngineConfig config, MergePolicy mergePolicy) {
        return new EngineConfig(
            config.getShardId(),
            config.getThreadPool(),
            config.getIndexSettings(),
            config.getStore(),
            mergePolicy,
            config.getAnalyzer(),
            new CodecService(),
            config.getEventListener(),
            config.getQueryCache(),
            config.getQueryCachingPolicy(),
            config.getTranslogConfig(),
            config.getFlushMergesAfter(),
            config.getExternalRefreshListeners(),
            Collections.emptyList(),
            config.getCircuitBreakerService(),
            config.getGlobalCheckpointSupplier(),
            config.retentionLeasesSupplier(),
            config.getPrimaryTermSupplier(),
            config.getTombstoneDocSupplier()
        );
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        try {
            if (engine != null && engine.isClosed.get() == false) {
                engine.getTranslog().getDeletionPolicy().assertNoOpenTranslogRefs();
                assertConsistentHistoryBetweenTranslogAndLuceneIndex(engine);
                assertNoInFlightDocuments(engine);
                assertMaxSeqNoInCommitUserData(engine);
                assertAtMostOneLuceneDocumentPerSequenceNumber(engine);
            }
            if (replicaEngine != null && replicaEngine.isClosed.get() == false) {
                replicaEngine.getTranslog().getDeletionPolicy().assertNoOpenTranslogRefs();
                assertConsistentHistoryBetweenTranslogAndLuceneIndex(replicaEngine);
                assertNoInFlightDocuments(replicaEngine);
                assertMaxSeqNoInCommitUserData(replicaEngine);
                assertAtMostOneLuceneDocumentPerSequenceNumber(replicaEngine);
            }
        } finally {
            IOUtils.close(replicaEngine, storeReplica, engine, store, () -> terminate(threadPool));
        }
    }


    protected static Document testDocumentWithTextField() {
        return testDocumentWithTextField("test");
    }

    protected static Document testDocumentWithTextField(String value) {
        Document document = testDocument();
        document.add(new TextField("value", value, Field.Store.YES));
        return document;
    }

    protected static Document testDocumentWithKeywordField(String value) {
        Document document = testDocument();
        var binaryValue = new BytesRef(value);
        document.add(new Field("value", binaryValue, StringIndexer.FIELD_TYPE));
        document.add(new SortedSetDocValuesField("value", binaryValue));
        return document;
    }

    protected static Document testDocument() {
        return new Document();
    }

    public static ParsedDocument createParsedDoc(String id) {
        return testParsedDocument(id, testDocumentWithKeywordField("test"), new BytesArray("{ \"value\" : \"test\" }"));
    }

    public static ParsedDocument createParsedDoc(String id, boolean recoverySource) {
        return testParsedDocument(id, testDocumentWithTextField(), new BytesArray("{ \"value\" : \"test\" }"),
            recoverySource);
    }

    protected static ParsedDocument testParsedDocument(
        String id, Document document, BytesReference source) {
        return testParsedDocument(id, document, source, false);
    }

    protected static ParsedDocument testParsedDocument(String id, Document document, BytesReference source,
                                                       boolean recoverySource) {
        Field uidField = new Field("_id", Uid.encodeId(id), SysColumns.ID.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        SequenceIDFields seqID = SequenceIDFields.emptySeqID();
        document.add(uidField);
        document.add(new BinaryDocValuesField("_id", Uid.encodeId(id)));
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        BytesRef ref = source.toBytesRef();
        if (recoverySource) {
            document.add(new StoredField(SysColumns.Source.RECOVERY_NAME, ref.bytes, ref.offset, ref.length));
            document.add(new NumericDocValuesField(SysColumns.Source.RECOVERY_NAME, 1));
        } else {
            document.add(new StoredField(SysColumns.Source.NAME, ref.bytes, ref.offset, ref.length));
        }
        return new ParsedDocument(versionField, seqID, id, document, source);
    }

    /**
     * Creates a tombstone document that only includes uid, seq#, term and version fields.
     */
    public static EngineConfig.TombstoneDocSupplier tombstoneDocSupplier() {
        return new EngineConfig.TombstoneDocSupplier() {
            @Override
            public ParsedDocument newDeleteTombstoneDoc(String id) {
                final Document doc = new Document();
                Field uidField = new Field(SysColumns.Names.ID, Uid.encodeId(id), SysColumns.ID.FIELD_TYPE);
                doc.add(uidField);
                doc.add(new BinaryDocValuesField(SysColumns.Names.ID, Uid.encodeId(id)));
                Field versionField = new NumericDocValuesField(SysColumns.VERSION.name(), 0);
                doc.add(versionField);
                SequenceIDFields seqID = SequenceIDFields.emptySeqID();
                doc.add(seqID.seqNo);
                doc.add(seqID.seqNoDocValue);
                doc.add(seqID.primaryTerm);
                seqID.tombstoneField.setLongValue(1);
                doc.add(seqID.tombstoneField);
                return new ParsedDocument(
                    versionField, seqID, id, doc, new BytesArray("{}"));
            }

            @Override
            public ParsedDocument newNoopTombstoneDoc(String reason) {
                final Document doc = new Document();
                SequenceIDFields seqID = SequenceIDFields.emptySeqID();
                doc.add(seqID.seqNo);
                doc.add(seqID.seqNoDocValue);
                doc.add(seqID.primaryTerm);
                seqID.tombstoneField.setLongValue(1);
                doc.add(seqID.tombstoneField);
                Field versionField = new NumericDocValuesField(SysColumns.VERSION.name(), 0);
                doc.add(versionField);
                BytesRef byteRef = new BytesRef(reason);
                doc.add(new StoredField(SysColumns.Source.NAME, byteRef.bytes, byteRef.offset, byteRef.length));
                return new ParsedDocument(
                    versionField, seqID, null, doc, null);
            }
        };
    }

    protected Store createStore() throws IOException {
        return createStore(newDirectory());
    }

    protected Store createStore(final Directory directory) throws IOException {
        return createStore(INDEX_SETTINGS, directory);
    }

    protected Store createStore(final IndexSettings indexSettings, final Directory directory) throws IOException {
        return new Store(shardId, indexSettings, directory, new DummyShardLock(shardId));
    }

    protected Translog createTranslog(LongSupplier primaryTermSupplier) throws IOException {
        return createTranslog(primaryTranslogDir, primaryTermSupplier);
    }

    protected Translog createTranslog(Path translogPath, LongSupplier primaryTermSupplier) throws IOException {
        TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE);
        String translogUUID = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId,
            primaryTermSupplier.getAsLong());
        return new Translog(translogConfig, translogUUID, createTranslogDeletionPolicy(INDEX_SETTINGS),
            () -> SequenceNumbers.NO_OPS_PERFORMED, primaryTermSupplier, seqNo -> {});
    }

    protected TranslogHandler createTranslogHandler(IndexSettings indexSettings) {
        return new TranslogHandler(indexSettings);
    }

    protected InternalEngine createEngine(Store store, Path translogPath) throws IOException {
        return createEngine(defaultSettings, store, translogPath, newMergePolicy(), null);
    }

    protected InternalEngine createEngine(Store store, Path translogPath, LongSupplier globalCheckpointSupplier) throws IOException {
        return createEngine(defaultSettings, store, translogPath, newMergePolicy(), null, null, globalCheckpointSupplier);
    }

    protected InternalEngine createEngine(
        Store store,
        Path translogPath,
        BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier) throws IOException {
        return createEngine(defaultSettings, store, translogPath, newMergePolicy(), null, localCheckpointTrackerSupplier, null);
    }

    protected InternalEngine createEngine(
        Store store,
        Path translogPath,
        BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
        ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation) throws IOException {
        return createEngine(
            defaultSettings, store, translogPath, newMergePolicy(), null, localCheckpointTrackerSupplier, null, seqNoForOperation);
    }

    protected InternalEngine createEngine(
        IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy) throws IOException {
        return createEngine(indexSettings, store, translogPath, mergePolicy, null);

    }

    protected InternalEngine createEngine(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy,
                                          @Nullable IndexWriterFactory indexWriterFactory) throws IOException {
        return createEngine(indexSettings, store, translogPath, mergePolicy, indexWriterFactory, null, null);
    }

    protected InternalEngine createEngine(
        IndexSettings indexSettings,
        Store store,
        Path translogPath,
        MergePolicy mergePolicy,
        @Nullable IndexWriterFactory indexWriterFactory,
        @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
        @Nullable LongSupplier globalCheckpointSupplier) throws IOException {
        return createEngine(
            indexSettings, store, translogPath, mergePolicy, indexWriterFactory, localCheckpointTrackerSupplier, null,
            globalCheckpointSupplier);
    }

    protected InternalEngine createEngine(
        IndexSettings indexSettings,
        Store store,
        Path translogPath,
        MergePolicy mergePolicy,
        @Nullable IndexWriterFactory indexWriterFactory,
        @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
        @Nullable LongSupplier globalCheckpointSupplier,
        @Nullable ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation) throws IOException {
        return createEngine(
            indexSettings,
            store,
            translogPath,
            mergePolicy,
            indexWriterFactory,
            localCheckpointTrackerSupplier,
            seqNoForOperation,
            globalCheckpointSupplier);
    }

    protected InternalEngine createEngine(
        IndexSettings indexSettings,
        Store store,
        Path translogPath,
        MergePolicy mergePolicy,
        @Nullable IndexWriterFactory indexWriterFactory,
        @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
        @Nullable ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation,
        @Nullable LongSupplier globalCheckpointSupplier) throws IOException {
        EngineConfig config = config(indexSettings, store, translogPath, mergePolicy, null, globalCheckpointSupplier);
        return createEngine(indexWriterFactory, localCheckpointTrackerSupplier, seqNoForOperation, config);
    }

    protected InternalEngine createEngine(EngineConfig config) throws IOException {
        return createEngine(null, null, null, config);
    }

    protected InternalEngine createEngine(@Nullable IndexWriterFactory indexWriterFactory,
                                          @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
                                          @Nullable ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation,
                                          EngineConfig config) throws IOException {
        final Store store = config.getStore();
        final Directory directory = store.directory();
        if (Lucene.indexExists(directory) == false) {
            store.createEmpty(config.getIndexSettings().getIndexVersionCreated().luceneVersion);
            final String translogUuid = Translog.createEmptyTranslog(config.getTranslogConfig().getTranslogPath(),
                SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
            store.associateIndexWithNewTranslog(translogUuid);

        }
        InternalEngine internalEngine = createInternalEngine(indexWriterFactory, localCheckpointTrackerSupplier, seqNoForOperation, config);
        internalEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
        return internalEngine;
    }

    public static InternalEngine createEngine(EngineConfig engineConfig, int maxDocs) {
        return new InternalEngine(engineConfig, maxDocs, LocalCheckpointTracker::new);
    }

    @FunctionalInterface
    public interface IndexWriterFactory {

        IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException;
    }

    /**
     * Generate a new sequence number and return it. Only works on InternalEngines
     */
    public static long generateNewSeqNo(final Engine engine) {
        assert engine instanceof InternalEngine : "expected InternalEngine, got: " + engine.getClass();
        InternalEngine internalEngine = (InternalEngine) engine;
        return internalEngine.getLocalCheckpointTracker().generateSeqNo();
    }

    public static InternalEngine createInternalEngine(
        @Nullable final IndexWriterFactory indexWriterFactory,
        @Nullable final BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
        @Nullable final ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation,
        final EngineConfig config) {
        if (localCheckpointTrackerSupplier == null) {
            return new InternalTestEngine(config) {
                @Override
                IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
                    return (indexWriterFactory != null) ?
                        indexWriterFactory.createWriter(directory, iwc) :
                        super.createWriter(directory, iwc);
                }

                @Override
                protected long doGenerateSeqNoForOperation(final Operation operation) {
                    return seqNoForOperation != null
                        ? seqNoForOperation.applyAsLong(this, operation)
                        : super.doGenerateSeqNoForOperation(operation);
                }
            };
        } else {
            return new InternalTestEngine(config, IndexWriter.MAX_DOCS, localCheckpointTrackerSupplier) {
                @Override
                IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
                    return (indexWriterFactory != null) ?
                        indexWriterFactory.createWriter(directory, iwc) :
                        super.createWriter(directory, iwc);
                }

                @Override
                protected long doGenerateSeqNoForOperation(final Operation operation) {
                    return seqNoForOperation != null
                        ? seqNoForOperation.applyAsLong(this, operation)
                        : super.doGenerateSeqNoForOperation(operation);
                }
            };
        }

    }

    public EngineConfig config(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy,
                               ReferenceManager.RefreshListener refreshListener) {
        return config(indexSettings, store, translogPath, mergePolicy, refreshListener, () -> SequenceNumbers.NO_OPS_PERFORMED);
    }

    public EngineConfig config(IndexSettings indexSettings, Store store, Path translogPath, MergePolicy mergePolicy,
                               ReferenceManager.RefreshListener refreshListener, LongSupplier globalCheckpointSupplier) {
        return config(
            indexSettings,
            store,
            translogPath,
            mergePolicy,
            refreshListener,
            globalCheckpointSupplier,
            globalCheckpointSupplier == null ? null : () -> RetentionLeases.EMPTY
        );
    }


    public EngineConfig config(
            final IndexSettings indexSettings,
            final Store store,
            final Path translogPath,
            final MergePolicy mergePolicy,
            final ReferenceManager.RefreshListener refreshListener,
            final LongSupplier globalCheckpointSupplier,
            final Supplier<RetentionLeases> retentionLeasesSupplier) {
        return config(
            indexSettings,
            store,
            translogPath,
            mergePolicy,
            refreshListener,
            null,
            globalCheckpointSupplier,
            retentionLeasesSupplier
        );
     }

    public EngineConfig config(IndexSettings indexSettings,
                               Store store,
                               Path translogPath,
                               MergePolicy mergePolicy,
                               ReferenceManager.RefreshListener externalRefreshListener,
                               ReferenceManager.RefreshListener internalRefreshListener,
                               @Nullable LongSupplier maybeGlobalCheckpointSupplier) {
        return config(
            indexSettings,
            store,
            translogPath,
            mergePolicy,
            externalRefreshListener,
            internalRefreshListener,
            maybeGlobalCheckpointSupplier,
            maybeGlobalCheckpointSupplier == null ? null : () -> RetentionLeases.EMPTY);
    }

    public EngineConfig config(IndexSettings indexSettings,
                               Store store,
                               Path translogPath,
                               MergePolicy mergePolicy,
                               ReferenceManager.RefreshListener externalRefreshListener,
                               ReferenceManager.RefreshListener internalRefreshListener,
                               @Nullable LongSupplier maybeGlobalCheckpointSupplier,
                               @Nullable Supplier<RetentionLeases> maybeRetentionLeasesSupplier) {
        IndexWriterConfig iwc = newIndexWriterConfig();
        TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
        Engine.EventListener eventListener = new Engine.EventListener() {
            @Override
            public void onFailedEngine(String reason, @Nullable Exception e) {
                // we don't need to notify anybody in this test
            }
        };
        final List<ReferenceManager.RefreshListener> extRefreshListenerList =
            externalRefreshListener == null ? emptyList() : Collections.singletonList(externalRefreshListener);
        final List<ReferenceManager.RefreshListener> intRefreshListenerList =
            internalRefreshListener == null ? emptyList() : Collections.singletonList(internalRefreshListener);


        final LongSupplier globalCheckpointSupplier;
        final Supplier<RetentionLeases> retentionLeasesSupplier;
        if (maybeGlobalCheckpointSupplier == null) {
            assert maybeRetentionLeasesSupplier == null;
            final ReplicationTracker replicationTracker = new ReplicationTracker(
                shardId,
                allocationId.getId(),
                indexSettings,
                randomNonNegativeLong(),
                SequenceNumbers.NO_OPS_PERFORMED,
                update -> {},
                () -> 0L,
                (leases, listener) -> {},
                () -> SafeCommitInfo.EMPTY
            );
            globalCheckpointSupplier = replicationTracker;
            retentionLeasesSupplier = replicationTracker::getRetentionLeases;
        } else {
            assert maybeRetentionLeasesSupplier != null;
            globalCheckpointSupplier = maybeGlobalCheckpointSupplier;
            retentionLeasesSupplier = maybeRetentionLeasesSupplier;
        }
        return new EngineConfig(
            shardId,
            threadPool,
            indexSettings,
            store,
            mergePolicy,
            iwc.getAnalyzer(),
            new CodecService(),
            eventListener,
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            translogConfig,
            TimeValue.timeValueMinutes(5),
            extRefreshListenerList,
            intRefreshListenerList,
            new NoneCircuitBreakerService(),
            globalCheckpointSupplier,
            retentionLeasesSupplier,
            primaryTerm,
            tombstoneDocSupplier());
    }

    protected EngineConfig config(EngineConfig config,
                                  Store store,
                                  Path translogPath,
                                  EngineConfig.TombstoneDocSupplier tombstoneDocSupplier) {
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            config.getIndexSettings().getSettings()
        );
        TranslogConfig translogConfig = new TranslogConfig(
            shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
        return new EngineConfig(
            config.getShardId(),
            config.getThreadPool(),
            indexSettings,
            store,
            config.getMergePolicy(),
            config.getAnalyzer(),
            new CodecService(),
            config.getEventListener(),
            config.getQueryCache(),
            config.getQueryCachingPolicy(),
            translogConfig,
            config.getFlushMergesAfter(),
            config.getExternalRefreshListeners(),
            config.getInternalRefreshListeners(),
            config.getCircuitBreakerService(),
            config.getGlobalCheckpointSupplier(),
            config.retentionLeasesSupplier(),
            config.getPrimaryTermSupplier(),
            tombstoneDocSupplier);
    }

    protected EngineConfig noOpConfig(IndexSettings indexSettings, Store store, Path translogPath) {
        return noOpConfig(indexSettings, store, translogPath, null);
    }

    protected EngineConfig noOpConfig(IndexSettings indexSettings, Store store, Path translogPath, LongSupplier globalCheckpointSupplier) {
        return config(indexSettings, store, translogPath, newMergePolicy(), null, null, globalCheckpointSupplier);
    }

    protected static final BytesReference B_1 = new BytesArray(new byte[]{1});
    protected static final BytesReference B_2 = new BytesArray(new byte[]{2});
    protected static final BytesReference B_3 = new BytesArray(new byte[]{3});
    protected static final BytesArray SOURCE = bytesArray("{}");

    protected static BytesArray bytesArray(String string) {
        return new BytesArray(string.getBytes(Charset.defaultCharset()));
    }

    public static Term newUid(String id) {
        return new Term("_id", Uid.encodeId(id));
    }

    public static Term newUid(ParsedDocument doc) {
        return newUid(doc.id());
    }

    protected Engine.Get newGet(ParsedDocument doc) {
        return new Engine.Get(doc.id(), newUid(doc));
    }

    protected Engine.Index indexForDoc(ParsedDocument doc) {
        return new Engine.Index(
            newUid(doc),
            doc,
            UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            -1,
            false,
            UNASSIGNED_SEQ_NO,
            0
        );
    }

    protected Engine.Index replicaIndexForDoc(ParsedDocument doc,
                                              long version,
                                              long seqNo,
                                              boolean isRetry) {
        return new Engine.Index(
            newUid(doc),
            doc,
            seqNo,
            primaryTerm.get(),
            version,
            null,
            Engine.Operation.Origin.REPLICA,
            System.nanoTime(),
            Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
            isRetry,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }

    protected Engine.Delete replicaDeleteForDoc(String id, long version, long seqNo, long startTime) {
        return new Engine.Delete(
            id,
            newUid(id),
            seqNo,
            1,
            version,
            null,
            Engine.Operation.Origin.REPLICA,
            startTime,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }

    protected static void assertVisibleCount(InternalEngine engine, int numDocs) throws IOException {
        assertVisibleCount(engine, numDocs, true);
    }

    protected static void assertVisibleCount(InternalEngine engine, int numDocs, boolean refresh) throws IOException {
        if (refresh) {
            engine.refresh("test");
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            int totalHits = searcher.count(new MatchAllDocsQuery());
            assertThat(totalHits).isEqualTo(numDocs);
        }
    }

    public static List<Engine.Operation> generateSingleDocHistory(boolean forReplica,
                                                                  VersionType versionType,
                                                                  long primaryTerm,
                                                                  int minOpCount,
                                                                  int maxOpCount,
                                                                  String docId) {
        final int numOfOps = randomIntBetween(minOpCount, maxOpCount);
        final List<Engine.Operation> ops = new ArrayList<>();
        final Term id = newUid(docId);
        final int startWithSeqNo = 0;
        final String valuePrefix = (forReplica ? "r_" : "p_") + docId + "_";
        final boolean incrementTermWhenIntroducingSeqNo = randomBoolean();
        for (int i = 0; i < numOfOps; i++) {
            final Engine.Operation op;
            final long version;
            switch (versionType) {
                case INTERNAL:
                    version = forReplica ? i : Versions.MATCH_ANY;
                    break;
                case EXTERNAL:
                    version = i;
                    break;
                case EXTERNAL_GTE:
                    version = randomBoolean() ? Math.max(i - 1, 0) : i;
                    break;
                default:
                    throw new UnsupportedOperationException("unknown version type: " + versionType);
            }
            if (randomBoolean()) {
                op = new Engine.Index(
                    id,
                    testParsedDocument(docId, testDocumentWithTextField(valuePrefix + i), SOURCE),
                    forReplica && i >= startWithSeqNo ? i * 2 : UNASSIGNED_SEQ_NO,
                    forReplica && i >= startWithSeqNo && incrementTermWhenIntroducingSeqNo ? primaryTerm + 1 : primaryTerm,
                    version,
                    forReplica ? null : versionType,
                    forReplica ? REPLICA : PRIMARY,
                    System.currentTimeMillis(), -1, false,
                    UNASSIGNED_SEQ_NO,
                    0
                );
            } else {
                op = new Engine.Delete(
                    docId,
                    id,
                    forReplica && i >= startWithSeqNo ? i * 2 : UNASSIGNED_SEQ_NO,
                    forReplica && i >= startWithSeqNo && incrementTermWhenIntroducingSeqNo ? primaryTerm + 1 : primaryTerm,
                    version,
                    forReplica ? null : versionType,
                    forReplica ? REPLICA : PRIMARY,
                    System.currentTimeMillis(),
                    UNASSIGNED_SEQ_NO,
                    0
                );
            }
            ops.add(op);
        }
        return ops;
    }

    public List<Engine.Operation> generateHistoryOnReplica(int numOps, boolean allowGapInSeqNo, boolean allowDuplicate) throws Exception {
        long seqNo = 0;
        final int maxIdValue = randomInt(numOps * 2);
        final List<Engine.Operation> operations = new ArrayList<>(numOps);
        for (int i = 0; i < numOps; i++) {
            final String id = Integer.toString(randomInt(maxIdValue));
            final Engine.Operation.TYPE opType = randomFrom(Engine.Operation.TYPE.values());
            final long startTime = threadPool.relativeTimeInMillis();
            final int copies = allowDuplicate && rarely() ? between(2, 4) : 1;
            for (int copy = 0; copy < copies; copy++) {
                final ParsedDocument doc = createParsedDoc(id);
                switch (opType) {
                    case INDEX:
                        operations.add(new Engine.Index(
                            EngineTestCase.newUid(doc),
                            doc,
                            seqNo,
                            primaryTerm.get(),
                            i,
                            null,
                            randomFrom(REPLICA, PEER_RECOVERY),
                            startTime,
                            -1,
                            true,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        ));
                        break;
                    case DELETE:
                        operations.add(new Engine.Delete(
                            doc.id(),
                            EngineTestCase.newUid(doc),
                            seqNo,
                            primaryTerm.get(),
                            i,
                            null,
                            randomFrom(REPLICA, PEER_RECOVERY),
                            startTime,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        ));
                        break;
                    case NO_OP:
                        operations.add(new Engine.NoOp(
                            seqNo,
                            primaryTerm.get(),
                            randomFrom(REPLICA, PEER_RECOVERY),
                            startTime,
                            "test-" + i
                        ));
                        break;
                    default:
                        throw new IllegalStateException("Unknown operation type [" + opType + "]");
                }
            }
            seqNo++;
            if (allowGapInSeqNo && rarely()) {
                seqNo++;
            }
        }
        Randomness.shuffle(operations);
        return operations;
    }

    public static void assertOpsOnReplica(
        final List<Engine.Operation> ops,
        final InternalEngine replicaEngine,
        boolean shuffleOps,
        final Logger logger) throws IOException {
        final Engine.Operation lastOp = ops.get(ops.size() - 1);
        final String lastFieldValue;
        if (lastOp instanceof Engine.Index) {
            Engine.Index index = (Engine.Index) lastOp;
            lastFieldValue = index.document().get("value");
        } else {
            // delete
            lastFieldValue = null;
        }
        if (shuffleOps) {
            int firstOpWithSeqNo = 0;
            while (firstOpWithSeqNo < ops.size() && ops.get(firstOpWithSeqNo).seqNo() < 0) {
                firstOpWithSeqNo++;
            }
            // shuffle ops but make sure legacy ops are first
            shuffle(ops.subList(0, firstOpWithSeqNo), random());
            shuffle(ops.subList(firstOpWithSeqNo, ops.size()), random());
        }
        boolean firstOp = true;
        for (Engine.Operation op : ops) {
            logger.info("performing [{}], v [{}], seq# [{}], term [{}]",
                op.operationType().name().charAt(0), op.version(), op.seqNo(), op.primaryTerm());
            if (op instanceof Engine.Index) {
                Engine.IndexResult result = replicaEngine.index((Engine.Index) op);
                // replicas don't really care to about creation status of documents
                // this allows to ignore the case where a document was found in the live version maps in
                // a delete state and return false for the created flag in favor of code simplicity
                // as deleted or not. This check is just signal regression so a decision can be made if it's
                // intentional
                assertThat(result.isCreated()).isEqualTo(firstOp);
                assertThat(result.getVersion()).isEqualTo(op.version());
                assertThat(result.getResultType()).isEqualTo(Engine.Result.Type.SUCCESS);

            } else {
                Engine.DeleteResult result = replicaEngine.delete((Engine.Delete) op);
                // Replicas don't really care to about found status of documents
                // this allows to ignore the case where a document was found in the live version maps in
                // a delete state and return true for the found flag in favor of code simplicity
                // his check is just signal regression so a decision can be made if it's
                // intentional
                assertThat(result.isFound()).isEqualTo(firstOp == false);
                assertThat(result.getVersion()).isEqualTo(op.version());
                assertThat(result.getResultType()).isEqualTo(Engine.Result.Type.SUCCESS);
            }
            if (randomBoolean()) {
                replicaEngine.refresh("test");
            }
            if (randomBoolean()) {
                replicaEngine.flush();
                replicaEngine.refresh("test");
            }
            firstOp = false;
        }

        assertVisibleCount(replicaEngine, lastFieldValue == null ? 0 : 1);
        if (lastFieldValue != null) {
            try (Engine.Searcher searcher = replicaEngine.acquireSearcher("test")) {
                int totalHits = searcher.count(new TermQuery(new Term("value", lastFieldValue)));
                assertThat(totalHits).isEqualTo(1);
            }
        }
    }

    public static void concurrentlyApplyOps(List<Engine.Operation> ops, InternalEngine engine) throws InterruptedException {
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        CountDownLatch startGun = new CountDownLatch(thread.length);
        AtomicInteger offset = new AtomicInteger(-1);
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread(() -> {
                startGun.countDown();
                try {
                    startGun.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                int docOffset;
                while ((docOffset = offset.incrementAndGet()) < ops.size()) {
                    try {
                        final Engine.Operation op = ops.get(docOffset);
                        if (op instanceof Engine.Index) {
                            engine.index((Engine.Index) op);
                        } else if (op instanceof Engine.Delete) {
                            engine.delete((Engine.Delete) op);
                        } else {
                            engine.noOp((Engine.NoOp) op);
                        }
                        if ((docOffset + 1) % 4 == 0) {
                            engine.refresh("test");
                        }
                        if (rarely()) {
                            engine.flush();
                        }
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
            });
            thread[i].start();
        }
        for (int i = 0; i < thread.length; i++) {
            thread[i].join();
        }
    }

    public static void applyOperations(Engine engine, List<Engine.Operation> operations) throws IOException {
        for (Engine.Operation operation : operations) {
            applyOperation(engine, operation);
            if (randomInt(100) < 10) {
                engine.refresh("test");
            }
            if (rarely()) {
                engine.flush();
            }
        }
    }

    public static Engine.Result applyOperation(Engine engine, Engine.Operation operation) throws IOException {
        final Engine.Result result;
        switch (operation.operationType()) {
            case INDEX:
                result = engine.index((Engine.Index) operation);
                break;
            case DELETE:
                result = engine.delete((Engine.Delete) operation);
                break;
            case NO_OP:
                result = engine.noOp((Engine.NoOp) operation);
                break;
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
        return result;
    }

    /**
     * Gets a collection of tuples of docId, sequence number, and primary term of all live documents in the provided engine.
     */
    public static List<DocIdAndSeqNo> getDocIds(Engine engine, boolean refresh) throws IOException {
        if (refresh) {
            engine.refresh("test_get_doc_ids");
        }
        try (Engine.Searcher searcher = engine.acquireSearcher("test_get_doc_ids", Engine.SearcherScope.INTERNAL)) {
            List<DocIdAndSeqNo> docs = new ArrayList<>();
            for (LeafReaderContext leafContext : searcher.getIndexReader().leaves()) {
                LeafReader reader = leafContext.reader();
                NumericDocValues seqNoDocValues = reader.getNumericDocValues(SysColumns.Names.SEQ_NO);
                NumericDocValues primaryTermDocValues = reader.getNumericDocValues(SysColumns.Names.PRIMARY_TERM);
                NumericDocValues versionDocValues = reader.getNumericDocValues(SysColumns.VERSION.name());
                Bits liveDocs = reader.getLiveDocs();
                for (int i = 0; i < reader.maxDoc(); i++) {
                    if (liveDocs == null || liveDocs.get(i)) {
                        if (primaryTermDocValues.advanceExact(i) == false) {
                            // We have to skip non-root docs because its _id field is not stored (indexed only).
                            continue;
                        }
                        final long primaryTerm = primaryTermDocValues.longValue();
                        Document doc = reader.storedFields().document(i, Set.of(SysColumns.Names.ID, SysColumns.Source.NAME));
                        BytesRef binaryID = doc.getBinaryValue(SysColumns.Names.ID);
                        String id = Uid.decodeId(Arrays.copyOfRange(binaryID.bytes, binaryID.offset, binaryID.offset + binaryID.length));
                        if (seqNoDocValues.advanceExact(i) == false) {
                            throw new AssertionError("seqNoDocValues not found for doc[" + i + "] id[" + id + "]");
                        }
                        final long seqNo = seqNoDocValues.longValue();
                        if (versionDocValues.advanceExact(i) == false) {
                            throw new AssertionError("versionDocValues not found for doc[" + i + "] id[" + id + "]");
                        }
                        final long version = versionDocValues.longValue();
                        docs.add(new DocIdAndSeqNo(id, seqNo, primaryTerm, version));
                    }
                }
            }
            docs.sort(Comparator.comparingLong(DocIdAndSeqNo::seqNo)
                .thenComparingLong(DocIdAndSeqNo::primaryTerm)
                .thenComparing((DocIdAndSeqNo::id)));
            return docs;
        }
    }

    /**
     * Reads all engine operations that have been processed by the engine from Lucene index.
     * The returned operations are sorted and de-duplicated, thus each sequence number will be have at most one operation.
     */
    public static List<Translog.Operation> readAllOperationsInLucene(Engine engine) throws IOException {
        final List<Translog.Operation> operations = new ArrayList<>();
        long maxSeqNo = Math.max(0, ((InternalEngine) engine).getLocalCheckpointTracker().getMaxSeqNo());
        try (Translog.Snapshot snapshot = engine.newChangesSnapshot("test", 0, maxSeqNo, false)) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                operations.add(op);
            }
        }
        return operations;
    }

    /**
     * Asserts the provided engine has a consistent document history between translog and Lucene index.
     */
    public static void assertConsistentHistoryBetweenTranslogAndLuceneIndex(Engine engine) throws IOException {
        if ((engine instanceof InternalEngine) == false) {
            return;
        }
        final long maxSeqNo = ((InternalEngine) engine).getLocalCheckpointTracker().getMaxSeqNo();
        if (maxSeqNo < 0) {
            return; // nothing to check
        }
        final Map<Long, Translog.Operation> translogOps = new HashMap<>();
        try (Translog.Snapshot snapshot = EngineTestCase.getTranslog(engine).newSnapshot()) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                translogOps.put(op.seqNo(), op);
            }
        }
        final Map<Long, Translog.Operation> luceneOps = readAllOperationsInLucene(engine).stream()
            .collect(Collectors.toMap(Translog.Operation::seqNo, Function.identity()));
        final long globalCheckpoint = EngineTestCase.getTranslog(engine).getLastSyncedGlobalCheckpoint();
        final long retainedOps = engine.config().getIndexSettings().getSoftDeleteRetentionOperations();
        final long seqNoForRecovery;
        try (Engine.IndexCommitRef safeCommit = engine.acquireSafeIndexCommit()) {
            seqNoForRecovery =
                Long.parseLong(safeCommit.getIndexCommit().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) + 1;
        }
        final long minSeqNoToRetain = Math.min(seqNoForRecovery, globalCheckpoint + 1 - retainedOps);
        for (Translog.Operation translogOp : translogOps.values()) {
            final Translog.Operation luceneOp = luceneOps.get(translogOp.seqNo());
            if (luceneOp == null) {
                if (minSeqNoToRetain <= translogOp.seqNo() && translogOp.seqNo() <= maxSeqNo) {
                    fail("Operation not found seq# [" + translogOp.seqNo() + "], global checkpoint [" +
                         globalCheckpoint + "], " +
                         "retention policy [" + retainedOps + "], maxSeqNo [" + maxSeqNo + "], translog op [" +
                         translogOp + "]");
                } else {
                    continue;
                }
            }
            assertThat(luceneOp).isNotNull();
            assertThat(luceneOp.primaryTerm()).as(luceneOp.toString()).isEqualTo(translogOp.primaryTerm());
            assertThat(luceneOp.opType()).isEqualTo(translogOp.opType());
            if (luceneOp.opType() == Translog.Operation.Type.INDEX) {
                assertThat(luceneOp.getSource()).isEqualTo(translogOp.getSource());
            }
        }
    }

    /**
     * Asserts that the max_seq_no stored in the commit's user_data is never smaller than seq_no of any document in the commit.
     */
    public static void assertMaxSeqNoInCommitUserData(Engine engine) throws Exception {
        List<IndexCommit> commits = DirectoryReader.listCommits(engine.store.directory());
        for (IndexCommit commit : commits) {
            try (DirectoryReader reader = DirectoryReader.open(commit)) {
                assertThat(Long.parseLong(commit.getUserData().get(SequenceNumbers.MAX_SEQ_NO)))
                    .isGreaterThanOrEqualTo(maxSeqNosInReader(reader));
            }
        }
    }

    public static void assertAtMostOneLuceneDocumentPerSequenceNumber(Engine engine) throws IOException {
        if (engine instanceof InternalEngine) {
            try {
                engine.refresh("test");
                try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
                    assertAtMostOneLuceneDocumentPerSequenceNumber(engine.config().getIndexSettings(), searcher.getDirectoryReader());
                }
            } catch (AlreadyClosedException ignored) {
                // engine was closed
            }
        }
    }

    public static void assertAtMostOneLuceneDocumentPerSequenceNumber(IndexSettings indexSettings,
                                                                      DirectoryReader reader) throws IOException {
        Set<Long> seqNos = new HashSet<>();
        final DirectoryReader wrappedReader = Lucene.wrapAllDocsLive(reader);
        for (LeafReaderContext leaf : wrappedReader.leaves()) {
            NumericDocValues primaryTermDocValues = leaf.reader().getNumericDocValues(SysColumns.Names.PRIMARY_TERM);
            NumericDocValues seqNoDocValues = leaf.reader().getNumericDocValues(SysColumns.Names.SEQ_NO);
            int docId;
            while ((docId = seqNoDocValues.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                assertThat(seqNoDocValues.advanceExact(docId)).isTrue();
                long seqNo = seqNoDocValues.longValue();
                assertThat(seqNo).isGreaterThanOrEqualTo(0L);
                if (primaryTermDocValues.advanceExact(docId)) {
                    if (seqNos.add(seqNo) == false) {
                        IDVisitor idFieldVisitor = new IDVisitor(SysColumns.Names.ID);
                        leaf.reader().storedFields().document(docId, idFieldVisitor);
                        throw new AssertionError("found multiple documents for seq=" + seqNo + " id=" + idFieldVisitor.getId());
                    }
                }
            }
        }
    }

    /**
     * Exposes a translog associated with the given engine for testing purpose.
     */
    public static Translog getTranslog(Engine engine) {
        assert engine instanceof InternalEngine : "only InternalEngines have translogs, got: " + engine.getClass();
        InternalEngine internalEngine = (InternalEngine) engine;
        return internalEngine.getTranslog();
    }

    public static final class PrimaryTermSupplier implements LongSupplier {
        private final AtomicLong term;

        PrimaryTermSupplier(long initialTerm) {
            this.term = new AtomicLong(initialTerm);
        }

        public long get() {
            return term.get();
        }

        public void set(long newTerm) {
            this.term.set(newTerm);
        }

        @Override
        public long getAsLong() {
            return get();
        }
    }

    static long maxSeqNosInReader(DirectoryReader reader) throws IOException {
        long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        for (LeafReaderContext leaf : reader.leaves()) {
            final NumericDocValues seqNoDocValues = leaf.reader().getNumericDocValues(SysColumns.Names.SEQ_NO);
            while (seqNoDocValues.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNoDocValues.longValue());
            }
        }
        return maxSeqNo;
    }

    public static long getInFlightDocCount(Engine engine) {
        if (engine instanceof InternalEngine internalEngine) {
            return internalEngine.getInFlightDocCount();
        } else {
            return 0;
        }
    }

    public static void assertNoInFlightDocuments(Engine engine) throws Exception {
        assertBusy(() -> assertThat(getInFlightDocCount(engine)).isEqualTo(0L));
    }
}
