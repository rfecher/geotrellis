package geotrellis.spark.merge

import geotrellis.raster._
import geotrellis.spark._

import org.scalatest.FunSpec


class TileFeatureRDDMergeMethodsSpec extends FunSpec with TestEnvironment {
  val spatialKey = SpatialKey(0, 0)
  val ones = IntArrayTile.fill(1, 10, 10)
  val twos = IntArrayTile.fill(2, 10, 10)
  val threes = IntArrayTile.fill(3, 10, 10)
  val sevens = IntArrayTile.fill(7, 10, 10)
  val featureTile = TileFeature(ones, twos)
  val order = {(left: Seq[Int], right: Seq[Int]) => left.head < right.head}

  describe("TileFeature RDD Methods") {

    it("should give one FeatureTile per unique key (unary)") {
      val rdd = sc.parallelize(List((SpatialKey(0,0), featureTile), (SpatialKey(0,0), featureTile), (SpatialKey(0,1), featureTile)))
      val merged = rdd.merge(order).collect()
      merged.size should be (2)
    }

    it("should give one FeatureTile per unique key (binary)") {
      val rdd1 = sc.parallelize(List((SpatialKey(0,0), featureTile), (SpatialKey(0,0), featureTile), (SpatialKey(0,1), featureTile), (SpatialKey(1,0), featureTile)))
      val rdd2 = sc.parallelize(List((SpatialKey(0,0), featureTile), (SpatialKey(0,0), featureTile), (SpatialKey(0,1), featureTile), (SpatialKey(1,1), featureTile)))
      val merged = rdd1.merge(order, rdd2).collect()
      merged.size should be (4)
    }

    it("should respect the order function (Part 1)") {
      val rdd1 = sc.parallelize(List(
        (SpatialKey(0,0), TileFeature(ones, ones)),
        (SpatialKey(0,0), TileFeature(twos, twos))))
      val rdd2 = sc.parallelize(List(
        (SpatialKey(0,0), TileFeature(threes, threes)),
        (SpatialKey(0,0), TileFeature(sevens, sevens))))
      val merged = rdd1.merge(order, rdd2).first()
      merged._2.tile.get(0,0) should be (7)
    }

    it("should respect the order function (Part 2)") {
      val rdd1 = sc.parallelize(List(
        (SpatialKey(0,0), TileFeature(sevens, ones)),
        (SpatialKey(0,0), TileFeature(threes, twos))))
      val rdd2 = sc.parallelize(List(
        (SpatialKey(0,0), TileFeature(twos, threes)),
        (SpatialKey(0,0), TileFeature(ones, sevens))))
      val merged = rdd1.merge(order, rdd2).first()
      merged._2.tile.get(0,0) should be (1)
    }

    it("should respect the order function (Part 3)") {
      val rdd2 = sc.parallelize(List(
        (SpatialKey(0,0), TileFeature(sevens, ones)),
        (SpatialKey(0,0), TileFeature(threes, twos))))
      val rdd1 = sc.parallelize(List(
        (SpatialKey(0,0), TileFeature(twos, threes)),
        (SpatialKey(0,0), TileFeature(ones, sevens))))
      val merged = rdd1.merge(order, rdd2).first()
      merged._2.tile.get(0,0) should be (1)
    }

  }
}
