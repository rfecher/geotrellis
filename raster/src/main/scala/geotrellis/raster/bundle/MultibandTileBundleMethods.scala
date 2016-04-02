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


trait MultibandTileBundleMethods extends BundleMethods[MultibandTile] {

  def cols = self.cols

  def rows = self.rows

  def fiber(col: Int, row: Int): Seq[Int] =
    (0 until self.bandCount).map({ b => self.band(b).get(col, row)})

  def mix[D: ? => BundleMethods[D]](other: D, fn: (Int, Int, Seq[Int], Seq[Int]) => Seq[Int]): ArrayMultibandTile = {
    require(cols == other.cols && rows == other.rows, "Dimensions must match")

    val newBundleBandCount = math.min(self.bandCount, other.fiber(0,0).length)
    val newBundleBands = {
      val bands = Array.ofDim[MutableArrayTile](newBundleBandCount)
      var b = 0; while (b < newBundleBandCount) {
        bands(b) = ArrayTile.alloc(self.cellType, cols, rows)
        b += 1
      }
      bands
    }
    val newBundle = ArrayMultibandTile(newBundleBands)

    var col = 0; while (col < cols) {
      var row = 0; while (row < rows) {
        val newFiber = fn(col, row, self.fiber(col, row), other.fiber(col, row))
        var b = 0; newFiber.take(newBundleBandCount).foreach({ z =>
          newBundleBands(b).set(col, row, z)
          b += 1
        })
        row += 1
      }
      col += 1
    }
    newBundle
  }

}
