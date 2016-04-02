/*
 * Copyright (c) 2016 Azavea.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.raster.merge

import geotrellis.raster._
import geotrellis.raster.bundle._
import geotrellis.util.MethodExtensions


class TileFeatureMergeMethods[T <: Tile, D: ? => BundleMethods[D]](val self: TileFeature[T,D])
    extends MethodExtensions[TileFeature[T,D]] {

  def merge[T2 <: Tile, D2: ? => BundleMethods[D2]](
    other: TileFeature[T2,D2],
    fn: (Seq[Int], Seq[Int]) => Boolean
  ): TileFeature[Tile,D] = { // XXX

    require(self.data.cols >= self.tile.cols && self.data.rows >= self.tile.rows, "Self-data must cover self-tile.")
    require(other.data.cols >= other.tile.cols && other.data.rows >= other.tile.rows, "Other-data must cover other-tile.")

    val typeUnion = self.tile.cellType.union(other.tile.cellType)
    val newTile = ArrayTile.alloc(typeUnion, self.tile.cols, self.tile.rows)
    val mixingFn = {(col: Int, row: Int, thisFiber: Seq[Int], otherFiber: Seq[Int]) =>
      fn(thisFiber, otherFiber) match {
        case false =>
          val selfPixel = self.tile.get(col, row)
          val pixel = if (!isNoData(selfPixel)) selfPixel; else other.tile.get(col, row)
          newTile.set(col, row, pixel)
          thisFiber
        case true =>
          val otherPixel = other.tile.get(col, row)
          val pixel = if (!isNoData(otherPixel)) otherPixel; else self.tile.get(col, row)
          newTile.set(col, row, pixel)
          otherFiber
      }
    }
    val newData = self.data.mix(other.data, mixingFn)

    TileFeature(newTile, newData)
  }

}
