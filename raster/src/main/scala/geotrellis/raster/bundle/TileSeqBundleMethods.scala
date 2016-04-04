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

package geotrellis.raster.bundle

import geotrellis.raster._


trait TileSeqBundleMethods extends BundleMethods[Traversable[Tile]] {

  def cols = self.head.cols

  def rows = self.head.rows

  def fiber(col: Int, row: Int): Seq[Int] =
    self.map({ tile => tile.get(col, row) }).toVector

  def mix[D: ? => BundleMethods[D]](other: D, fn: (Int, Int, Seq[Int], Seq[Int]) => Seq[Int]): Traversable[Tile] = {
    require(cols == other.cols && rows == other.rows, "Dimensions must match")

    val newBundleTileCount = math.min(self.size, other.fiber(0,0).length)
    val newBundleTiles = {
      val bands = Array.ofDim[MutableArrayTile](newBundleTileCount)
      val selfVector = self.toVector
      var b = 0; while (b < newBundleTileCount) {
        bands(b) = ArrayTile.alloc(selfVector(b).cellType, cols, rows)
        b += 1
      }
      bands
    }

    var col = 0; while (col < cols) {
      var row = 0; while (row < rows) {
        val newFiber = fn(col, row, self.fiber(col, row), other.fiber(col, row))
        var b = 0; newFiber.take(newBundleTileCount).foreach({ z =>
          newBundleTiles(b).set(col, row, z)
          b += 1
        })
        row += 1
      }
      col += 1
    }
    newBundleTiles
  }

}
