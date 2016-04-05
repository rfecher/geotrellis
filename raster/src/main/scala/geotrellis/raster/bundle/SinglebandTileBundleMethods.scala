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


trait SinglebandTileBundleMethods extends BundleMethods[Tile] {

  def cols = self.cols

  def rows = self.rows

  def fiber(col: Int, row: Int): Seq[Int] =
    List(self.get(col, row))

  def mix[D: ? => BundleMethods[D]](other: D, fn: (Int, Int, Seq[Int], Seq[Int]) => Seq[Int]): Tile = {
    require(cols == other.cols && rows == other.rows, "Dimensions must match")

    val newBundle = ArrayTile.alloc(self.cellType, cols, rows)

    var col = 0; while (col < cols) {
      var row = 0; while (row < rows) {
        val newFiber = fn(col, row, self.fiber(col, row), other.fiber(col, row))
        newBundle.set(col, row, newFiber.head)
        row += 1
      }
      col += 1
    }
    newBundle
  }

}
