package geotrellis.spark.io.geowave

import geotrellis.geotools._
import geotrellis.proj4.LatLng
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.index.KeyIndex
import geotrellis.spark.tiling.{LayoutDefinition, MapKeyTransform}
import geotrellis.util._
import geotrellis.vector.Extent

import com.vividsolutions.jts.geom._
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter
import mil.nga.giat.geowave.core.geotime.store.query.IndexOnlySpatialQuery
import mil.nga.giat.geowave.core.geotime.ingest._
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics
import mil.nga.giat.geowave.core.index.ByteArrayId
import mil.nga.giat.geowave.core.store._
import mil.nga.giat.geowave.core.store.index.CustomIdIndex
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions
import mil.nga.giat.geowave.core.store.query.QueryOptions
import mil.nga.giat.geowave.datastore.accumulo._
import mil.nga.giat.geowave.datastore.accumulo.metadata._
import mil.nga.giat.geowave.mapreduce.input.{GeoWaveInputKey, GeoWaveInputFormat}
import org.apache.avro.Schema
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.geotools.coverage.grid._

import spray.json._

import scala.reflect._


object GeowaveLayerReader {
  val geometryFactory = new GeometryFactory
  val p = geometryFactory.createPoint(new Coordinate(31.137720, 29.975307))
  val emptySet = p.difference(p)

  /**
    * Given a map transform and a keybounds, produce a corresponding
    * jts.Geometry.
    *
    * @param  mt  The map transform
    * @param  kb  The KeyBounds
    */
  def keyBoundsToGeometry(mt: MapKeyTransform, kb: KeyBounds[SpatialKey]) = {
    val KeyBounds(minKey, maxKey) = kb
    val Extent(lng1, lat1, lng2, lat2) = mt(minKey)
    val Extent(lng3, lat3, lng4, lat4) = mt(maxKey)
    val lngs = List(lng1, lng2, lng3, lng4)
    val lats = List(lat1, lat2, lat3, lat4)
    val width = math.abs(lng1 - lng2)
    val height = math.abs(lat1 - lat2)
    val minLng = lngs.min
    val maxLng = lngs.max
    val minLat = lats.min
    val maxLat = lats.max
    val envelope = new Envelope(
      minLng + width/3,
      maxLng - width/3,
      minLat + height/3,
      maxLat - height/3)

    geometryFactory.toGeometry(envelope)
  }
}

class GeowaveLayerReader(val attributeStore: AttributeStore)(implicit sc: SparkContext)
    extends FilteringLayerReader[LayerId] {

  val defaultNumPartitions = sc.defaultParallelism

  val GeowaveAttributeStore(
    basicOperations,
    requiredOptions,
    adapters,
    index,
    substrats,
    bboxMap
  ) = attributeStore

  /**
    * Compute the common part of the
    * org.apache.hadoop.conf.Configuration associated with this layer.
    * This result can be reused by changing the Query and QueryOptions
    * as desired.
    */
  def computeConfiguration()(implicit sc: SparkContext) = {
    val pluginOptions = new DataStorePluginOptions
    pluginOptions.setFactoryOptions(requiredOptions)
    val job = Job.getInstance(sc.hadoopConfiguration)
    val config = job.getConfiguration
    GeoWaveInputFormat.setStoreOptions(config, pluginOptions)

    config
  }

  /**
    * Compute the metadata associated with this layer.
    *
    * @param  adapter  The RasterDataAdapter associated with the chosen layer
    * @param  ranges   The ranges in degree of longitude and latitude associated with the chosen zoom level
    */
  def computeSpatialMetadata(
    adapter: RasterDataAdapter,
    ranges: Array[Double]
  ): (TileLayerMetadata[SpatialKey], Int, Int) = {
    val adapterId = adapter.getAdapterId

    val metadata = adapter.getMetadata

    val bbox = bboxMap.getOrElse(adapterId, throw new Exception(s"Unknown Adapter Id $adapterId"))

    val minX = bbox.getMinX
    val minY = bbox.getMinY
    val maxX = bbox.getMaxX
    val maxY = bbox.getMaxY
    val minCol = (minX / ranges(0)).toInt
    val minRow = (minY / ranges(1)).toInt
    val maxCol = (maxX / ranges(0)).toInt
    val maxRow = (maxY / ranges(1)).toInt

    val extent = Extent(
      minCol * ranges(0),
      minRow * ranges(1),
      (maxCol + 1) * ranges(0),
      (maxRow + 1) * ranges(1)
    )

    val layout = {
      val tileSize = adapter.getTileSize
      val tileLayout = TileLayout(maxCol - minCol + 1, maxRow - minRow + 1, tileSize, tileSize)
      LayoutDefinition(extent, tileLayout)
    }

    val cellType = metadata.get("cellType") match {
      case null => {
        val geom = (new GeometryFactory).createPoint(new Coordinate((minX + maxX) / 2.0, (minY + maxY) / 2.0))
        val queryOptions = new QueryOptions(adapter, index)
        val query = new IndexOnlySpatialQuery(geom)
        val config = computeConfiguration
        GeoWaveInputFormat.setQuery(config, query)
        GeoWaveInputFormat.setQueryOptions(config, queryOptions)

        val gc = sc.newAPIHadoopRDD(
          config,
          classOf[GeoWaveInputFormat[GridCoverage2D]],
          classOf[GeoWaveInputKey],
          classOf[GridCoverage2D])
          .map({ case (_, gc) => gc })
          .collect.head

        GridCoverage2DConverters.getCellType(gc)
      }
      case s: String => CellType.fromString(s)
    }

    val bounds = KeyBounds(
      SpatialKey(0, 0),
      SpatialKey(maxCol - minCol, maxRow - minRow)
    )

    (TileLayerMetadata(cellType, layout, extent, LatLng, bounds), minCol, maxRow)
  }

  /**
    * Read particular rasters out of the GeoWave database.  The
    * particular rasters to read are given by the result running the
    * provided LayerQuery.
    *
    * @param  id               The LayerId specifying the name and zoom level to query
    * @param  rasterQuery      Produces a list of rasters to read
    * @param  numPartitions    The number of Spark partitions to use
    * @param  filterIndexOnly  ?
    */
  def read[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]
  ](id: LayerId, rasterQuery: LayerQuery[K, M], numPartitions: Int, filterIndexOnly: Boolean) = {
    import GeowaveLayerReader._

    /* Perform checks */
    if (!attributeStore.layerExists(id))
      throw new LayerNotFoundError(id)
    implicitly[ClassTag[K]].toString match {
      case "geotrellis.spark.SpatialKey" =>
      case t: String => throw new Exception("Unsupported Key Type: $t")
    }

    /* Boilerplate */
    val LayerId(name, zoom) = id
    val adapter = adapters.filter(_.getCoverageName == name).head
    val strategy = substrats(zoom)
    val ranges = strategy.getIndexStrategy.getHighestPrecisionIdRangePerDimension
    val customIndex = new CustomIdIndex(strategy.getIndexStrategy, index.getIndexModel, index.getId)

    /* GeoTrellis metadata */
    val (_md, minCol, maxRow) = computeSpatialMetadata(adapter, ranges)
    val md = _md.asInstanceOf[M]

    /* GeoWave Query and Query Options */
    val queryOptions = new QueryOptions(adapter, customIndex)
    val query = {
      val fn = keyBoundsToGeometry(_md.mapTransform, _: KeyBounds[SpatialKey])
      val kbs = rasterQuery(md)

      implicitly[ClassTag[K]].toString match {
        case "geotrellis.spark.SpatialKey" => { // Spatial Query
          val geom = kbs
            .map({ kb => fn(kb.asInstanceOf[KeyBounds[SpatialKey]]) })
            .foldLeft(emptySet)({ (l, r) => l.union(r) })
          new IndexOnlySpatialQuery(geom)
        }
      }
    }

    /* Construct org.apache.hadoop.conf.Configuration */
    val config = computeConfiguration
    GeoWaveInputFormat.setQuery(config, query)
    GeoWaveInputFormat.setQueryOptions(config, queryOptions)

    /* Submit query */
    val rdd =
      implicitly[ClassTag[K]].toString match {
        case "geotrellis.spark.SpatialKey" => { // Spatial Query
          sc.newAPIHadoopRDD(
            config,
            classOf[GeoWaveInputFormat[GridCoverage2D]],
            classOf[GeoWaveInputKey],
            classOf[GridCoverage2D])
            .map({ case (_, gc) =>
              val Extent(lng, lat, _, _) = GridCoverage2DConverters.getExtent(gc)
              val key = SpatialKey(
                (lng / ranges(0)).toInt - minCol,
                maxRow - (lat / ranges(1)).toInt
              ).asInstanceOf[K]
              val value = implicitly[ClassTag[V]].toString match {
                case "geotrellis.raster.Tile" => gc.toTile(0).asInstanceOf[V]
                case "geotrellis.raster.MultibandTile" => gc.toMultibandTile.asInstanceOf[V]
                case t: String => throw new Exception("Unsupported Value Type: $t")
              }
              (key, value)
            })
        }
      }

    new ContextRDD(rdd, md)
  }
}
