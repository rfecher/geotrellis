package geotrellis.spark.merge

import geotrellis.raster._
import geotrellis.raster.bundle._
import geotrellis.raster.merge._

import org.apache.spark._
import org.apache.spark.rdd._

import scala.reflect.ClassTag


object TileFeatureRDDMerge {

  def apply[
    K: ClassTag,
    T <: Tile: ClassTag,
    D: ClassTag: ? => BundleMethods[D]
  ](
    fn: (Seq[Int], Seq[Int]) => Boolean,
    rdd: RDD[(K, TileFeature[T,D])],
    other: RDD[(K, TileFeature[T,D])]
  ): RDD[(K,TileFeature[Tile,D])] = {
    rdd
      .cogroup(other)
      .map { case (key, (myTiles, otherTiles)) =>
        if (myTiles.nonEmpty && otherTiles.nonEmpty) {
          val a = myTiles.reduce({ (left: TileFeature[Tile,D], right: TileFeature[Tile,D]) => left.merge(right, fn) })
          val b = otherTiles.reduce({ (left: TileFeature[Tile,D], right: TileFeature[Tile,D]) => left.merge(right, fn) })
          (key, a.merge(b, fn))
        } else if (myTiles.nonEmpty) {
          (key, myTiles.reduce({ (left: TileFeature[Tile,D], right: TileFeature[Tile,D]) => left.merge(right, fn) }))
        } else {
          (key, otherTiles.reduce({ (left: TileFeature[Tile,D], right: TileFeature[Tile,D]) => left.merge(right, fn) }))
        }
    }
  }

  def apply[
    K: ClassTag,
    T <: Tile: ClassTag,
    D: ClassTag: ? => BundleMethods[D]
  ](
    fn: (Seq[Int], Seq[Int]) => Boolean,
    rdd: RDD[(K,TileFeature[T,D])],
    partitioner: Option[Partitioner]
  ): RDD[(K,TileFeature[Tile,D])] = {
    val rdd2 = rdd.mapValues({ v: TileFeature[Tile,D] =>  v})

    partitioner match {
      case Some(p) =>
        rdd2
          .reduceByKey(p, {(left, right) => left.merge(right, fn) })
      case None =>
        rdd2
          .reduceByKey({ (left, right) => left.merge(right, fn) })
    }
  }

}
