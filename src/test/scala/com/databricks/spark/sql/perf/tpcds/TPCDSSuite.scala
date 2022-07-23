package com.databricks.spark.sql.perf.tpcds

import com.datastax.spark.connector._
import com.datastax.spark.connector.datasource.{CassandraCatalog, CassandraScan}
import io.fabric8.kubernetes.api.model.NodeBuilder
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient}
import org.apache.commons.io.IOUtils
import org.apache.hive.jdbc.HiveDriver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.optimizer.{ColumnPruning, RewritePredicateSubquery}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.BATCH_SCAN
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits.TableHelper
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2Relation, DataSourceV2ScanRelation, ScanBuilderHolder}
import org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown.applyColumnPruning
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel
import org.xbill.DNS.Address

import java.nio.charset.Charset
import java.sql.{DriverManager, ResultSet}
import java.util.Properties
import java.util.stream.Collectors
import scala.collection.{JavaConverters, mutable}

class TPCDSSuite extends FunSuite with BeforeAndAfterAll { self =>
  private object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.sparkSession.sqlContext
  }
  import testImplicits._

  private var sparkSession: SparkSession = _
  var savedLevels: Map[String, Level] = _
  private final val cassandraCatalog = "cassandra"
  private final val databaseName = "tpcds"
  private final val rootDir = "/Users/awfover/Projects/spark-sql-perf/perf-data"
  //  "/opt/spark-perf-data"

  def resultSetToList(resultSet: ResultSet): List[Seq[Object]] = {
    val columnCount = resultSet.getMetaData.getColumnCount

    new Iterator[Seq[Object]] {
      override def hasNext: Boolean = resultSet.next()

      override def next(): Seq[Object] = {
        // can also use column-label instead of column-index
        Seq.range(1, columnCount + 1).map(resultSet.getObject)
      }
    }.toList
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession
      .builder
      .master("local[3]")
      .appName("TPCDS QA")
      .config("spark.local.dir", "/Users/awfover/Projects/spark-sql-perf/spark-warehouse")
      .config(s"spark.sql.catalog.$cassandraCatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
//      .config("spark.cassandra.connection.host", "127.0.0.1:9042")
      .config("spark.cassandra.connection.host", "192.168.0.101:9042")
      .config("confirm.truncate", "true")
//      .config("spark.cassandra.auth.username", "demo-superuser")
//      .config("spark.cassandra.auth.password", "nYuTT6QqvXsRReDONbVX")
      .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "/Users/awfover/Projects/spark-sql-perf/spark-log")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.cassandra.sql.execution.batchscan.reuse", "true")
      .config("spark.cassandra.sql.execution.batchscan.persist", "NONE")
      .config("spark.sql.exchange.reuse", "true")
      .config("spark.cassandra.sql.fix.scan.equality", "true")
      .config("spark.sql.postPruneColumns", "false")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("INFO")

    // Travis limits the size of the log file produced by a build. Because we do run a small
    // version of all the ML benchmarks in this suite, we produce a ton of logs. Here we set the
    // log level to ERROR, just for this suite, to avoid displeasing travis.
    savedLevels = Seq("akka", "org", "com.databricks").map { name =>
      val logger = Logger.getLogger(name)
      val curLevel = logger.getLevel
      logger.setLevel(Level.ERROR)
      name -> curLevel
    }.toMap
  }

  override def afterAll(): Unit = {
    savedLevels.foreach { case (name, level) =>
      Logger.getLogger(name).setLevel(level)
    }
    try {
      if (sparkSession != null) {
        sparkSession.stop()
      }
      // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.driver.port")
      sparkSession = null
    } finally {
      super.afterAll()
    }
  }

  test("test cassandra q2.sql") {
    assert(sparkSession.sessionState.catalogManager.isCatalogRegistered(cassandraCatalog))

    sparkSession.sessionState.catalogManager.setCurrentCatalog(cassandraCatalog)
    assert(cassandraCatalog.equals(sparkSession.sessionState.catalogManager.currentCatalog.name()))

    val tpcds = new TPCDS(sqlContext = sparkSession.sqlContext)
    val queries = tpcds.tpcds2_4Queries // queries to run.
    sparkSession.sql(s"USE ${databaseName}_1")

    val df1 = sparkSession.sql(queries(1).sqlText.get)
    val plan = df1.queryExecution.executedPlan
    val dt1 = df1.collect()
  }

  test("test cassandra q10.sql") {
    assert(sparkSession.sessionState.catalogManager.isCatalogRegistered(cassandraCatalog))

    sparkSession.sessionState.catalogManager.setCurrentCatalog(cassandraCatalog)
    assert(cassandraCatalog.equals(sparkSession.sessionState.catalogManager.currentCatalog.name()))

    val tpcds = new TPCDS(sqlContext = sparkSession.sqlContext)
    val queries = tpcds.tpcds2_4Queries // queries to run.
    sparkSession.sql(s"USE ${databaseName}_1")

    val df1 = sparkSession.sql(queries(9).sqlText.get)
    val sparkPlan = df1.queryExecution.sparkPlan
    val executedPlan = df1.queryExecution.executedPlan
    val dt1 = df1.collect()
  }

  test("test hive jdbc") {

    try Class.forName("org.apache.hive.jdbc.HiveDriver")
    catch {
      case e: ClassNotFoundException =>
        e.printStackTrace()
        System.exit(1)
    }

    val queryContent: String = IOUtils.toString(
      getClass.getClassLoader.getResourceAsStream("tpcds_2_4/q2.sql"))

    val con = DriverManager.getConnection(
      "jdbc:hive2://spark.local:10000/cassandra.tpcds_1"
    )
    val stmt = con.createStatement()
    val resultSet = stmt.executeQuery(queryContent)

    val resultList = resultSetToList(resultSet)
    println(
      Seq.range(1, resultSet.getMetaData.getColumnCount + 1)
        .map(resultSet.getMetaData.getColumnName)
        .mkString("\t")
    )
    println(resultList.take(10).map(_.mkString("\t")).mkString("\n"))
    println()
  }

  test("test k8s client") {
    val config = new ConfigBuilder()
      .withMasterUrl("https://127.0.0.1:6666")
      .withNamespace("spark")
      .build
    val client = new DefaultKubernetesClient(config)
    val pods = client.pods().inAnyNamespace().withField("status.podIP", "10.244.3.6").list()
    val execs = client
      .pods()
//      .withLabel("spark-app-selector", applicationId)
      .withLabel("spark-role", "executor")
      .withoutLabel("spark-exec-inactive", "true")
      .list()
      .getItems
    val nodes = client.nodes().withName("kind-0-worker2").get()
//    val node = new NodeBuilder().withNewMetadata().endMetadata().withNewSpec().endSpec().build()
//    val labels = node.getMetadata.getLabels

    val i = 1
  }

  test("test cache performance") {
    for (_ <- 1 to 12) {
      for ((_, v) <- sparkSession.sparkContext.getPersistentRDDs) {
        v.unpersist(blocking = true)
      }
      sparkSession.sharedState.cacheManager.clearCache()
      val df = sparkSession.read.text("data.dat")
      df.persist(StorageLevel.DISK_ONLY)
      val dt = df.collect()
    }
  }
}
