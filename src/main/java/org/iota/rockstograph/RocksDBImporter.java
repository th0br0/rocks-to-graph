package org.iota.rockstograph;

import com.google.common.base.Strings;
import jota.model.Transaction;
import jota.utils.Converter;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jota.model.Transaction;

/**
 * @author Andreas C. Osowski
 */
public class RocksDBImporter implements Runnable {

  private final static Logger LOG = LoggerFactory.getLogger(RocksDBImporter.class);
  private final Config config;

  private Map<String, ColumnFamilyHandle> rdbCFHandles = new HashMap<>();
  private JanusGraph graphInst;
  private RocksDB rdbInst;
  private GraphTraversalSource graph;

  public RocksDBImporter(Config config) {
    this.config = config;
  }


  void openRocksDB() throws RocksDBException {
    LOG.info("Opening RocksDB from: " + config.rocksDbPath);
    List<ColumnFamilyDescriptor> cfs = new ArrayList<>();
    List<ColumnFamilyHandle> handles = new ArrayList<>();
    DBOptions dbOptions = new DBOptions();

    OptionsUtil.loadLatestOptions(config.rocksDbPath, Env.getDefault(), dbOptions, cfs);

    rdbInst = RocksDB.open(dbOptions, config.rocksDbPath, cfs, handles);

    for (ColumnFamilyHandle handle : handles) {
      rdbCFHandles.put(new String(handle.getName()), handle);
    }
  }

  void closeRocksDB() {
    rdbInst.close();
    rdbInst = null;
  }

  void openGraphDB() {
    graphInst = JanusGraphFactory.open(config.janusConfig);
    setupGraphSchema();
    graph = graphInst.traversal();
  }

  void closeGraphDB() {
    try {
      graph.close();
    } catch (Exception e) {
      LOG.error("graph.close() failed", e);
    }
    graphInst.close();

    graph = null;
    graphInst = null;
  }

  void setupGraphSchema() {
    final JanusGraphManagement management = graphInst.openManagement();

    if (management.getRelationTypes(RelationType.class).iterator().hasNext()) {
      LOG.info("Graph schema already setup");
      management.rollback();
      return;
    }

    LOG.info("Creating graph schema");

    management.makeVertexLabel("transaction").make();

    management.makePropertyKey("address").dataType(String.class).make();
    management.makePropertyKey("bundle").dataType(String.class).make();
    management.makePropertyKey("tag").dataType(String.class).make();
    management.makePropertyKey("value").dataType(Long.class).make();
    management.makePropertyKey("timestamp").dataType(Long.class).make();
    management.makePropertyKey("attachmentTimestamp").dataType(Long.class).make();
    management.makePropertyKey("attachmentTag").dataType(String.class).make();

    management.makeEdgeLabel("ref").directed().multiplicity(Multiplicity.MULTI).make();
    management.makePropertyKey("refKind").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();

    management.commit();
  }


  void performImport() throws RocksDBException {
    long count = 0;
    int[] txTrits = new int[8019];

    try (RocksIterator iter = rdbInst.newIterator(rdbCFHandles.get("transaction"))) {
      long txCount = rdbInst.getLongProperty(rdbCFHandles.get("transaction"), "rocksdb.estimate-num-keys");
      LOG.info("Estimated transaction count: " + txCount);

      // Genesis
      {
        String all9 = Strings.repeat("9", 81);
        graph.addV(all9)
            .property("address", all9)
            .property("bundle", all9)
            .property("tag", all9.substring(0, 27))
            .property("attachmentTag", all9.substring(0, 27))
            .property("value", 0)
            .property("timestamp", 0)
            .property("attachmentTimestamp", 0).next();
      }

      // Vertices
      for (iter.seekToFirst(); iter.isValid(); iter.next()) {
        count++;

        byte[] transactionData = iter.value();
        Converter.getTrits(transactionData, txTrits);
        Transaction iotaTx = Transaction.asTransactionObject(Converter.trytes(txTrits));

        graph.addV(iotaTx.getHash())
            .property("address", iotaTx.getAddress())
            .property("bundle", iotaTx.getBundle())
            .property("tag", iotaTx.getObsoleteTag())
            .property("attachmentTag", iotaTx.getTag())
            .property("value", iotaTx.getValue())
            .property("timestamp", iotaTx.getTimestamp())
            .property("attachmentTimestamp", iotaTx.getAttachmentTimestamp()).next();


        if (count % 1000L == 0) {
          graph.tx().commit();
          LOG.info("Persisted " + count + " vertices.");
        }
      }
      graph.tx().commit();
      LOG.info("Persisted " + count + " vertices.");

      // Edges
      count = 0;
      for (iter.seekToFirst(); iter.isValid(); iter.next()) {
        count++;

        byte[] transactionData = iter.value();
        Converter.getTrits(transactionData, txTrits);
        Transaction iotaTx = Transaction.asTransactionObject(Converter.trytes(txTrits));

        graph
            .V().has(T.label, iotaTx.getHash())
            .as("a")
            .V().has(T.label, iotaTx.getTrunkTransaction())
            .addE("ref")
            .property("refKind", 0)
            .from("a").next();

        graph
            .V().has(T.label, iotaTx.getHash())
            .as("a")
            .V().has(T.label, iotaTx.getBranchTransaction())
            .addE("ref")
            .property("refKind", 1)
            .from("a").next();

        if (count % 1000L == 0) {
          graph.tx().commit();
          LOG.info("Persisted " + 2 * count + " edges.");
        }
      }

      graph.tx().commit();
      LOG.info("Persisted " + 2 * count + " edges.");
    }
  }


  @Override
  public void run() {
    try {
      openRocksDB();
    } catch (Exception e) {
      LOG.error("Opening RocksDB failed.", e);
      return;
    }

    openGraphDB();
    LOG.info("Databases opened.");

    try {
      if (graph.V().count().next() != 0) {
        throw new RuntimeException("Graph already populated! Aborting.");
      }

      performImport();
    } catch (Exception e) {
      LOG.error("Import failed.", e);
    }

    closeGraphDB();
    closeRocksDB();
    LOG.info("Databases closed.");
  }
}
