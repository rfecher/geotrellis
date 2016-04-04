package geotrellis.spark.merge

import geotrellis.raster._
import geotrellis.raster.bundle._
import geotrellis.raster.merge._
import geotrellis.util.MethodExtensions

import org.apache.spark._
import org.apache.spark.rdd._

import scala.reflect.ClassTag


class TileFeatureRDDMergeMethods[
  K: ClassTag,
  T <: Tile: ClassTag,
  D: ClassTag: ? => BundleMethods[D]
](val self: RDD[(K,TileFeature[T,D])]) extends MethodExtensions[RDD[(K,TileFeature[T,D])]] {

  def merge(
    fn: (Seq[Int], Seq[Int]) => Boolean,
    other: RDD[(K,TileFeature[T,D])]
  ): RDD[(K,TileFeature[Tile,D])] =
    TileFeatureRDDMerge(fn, self, other)

  def merge(fn: (Seq[Int], Seq[Int]) => Boolean): RDD[(K,TileFeature[Tile,D])] =
    TileFeatureRDDMerge(fn, self, None)

  def merge(
    fn: (Seq[Int], Seq[Int]) => Boolean,
    partitioner: Option[Partitioner]
  ): RDD[(K,TileFeature[Tile,D])] =
    TileFeatureRDDMerge(fn, self, partitioner)

}
