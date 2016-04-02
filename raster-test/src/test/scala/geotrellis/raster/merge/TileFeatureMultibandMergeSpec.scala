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

package geotrellis.raster

import org.scalatest._


class TileFeatureMultibandMergeSpec
    extends FunSpec
    with Matchers {

  val ones = IntArrayTile.fill(1, 10, 10)
  val twos = IntArrayTile.fill(2, 10, 10)
  val threes = ArrayMultibandTile(IntArrayTile.fill(3, 10, 10), IntArrayTile.fill(3, 10, 10))
  val sevens = ArrayMultibandTile(IntArrayTile.fill(7, 10, 10), IntArrayTile.fill(7, 10, 10))
  val order = {(left: Seq[Int], right: Seq[Int]) =>
    val diff = left.zip(right).map({ case (a,b) => a - b }).dropWhile(_ == 0)
    if (diff.nonEmpty) {
      if (diff.head < 0) true; else false
    } else true
  }

  describe("The merge method") {
    it("should take values from higher-priority multi-pixels") {
      val ot1 = TileFeature(ones, threes)
      val ot2 = TileFeature(twos, sevens)
      val ot3 = ot1.merge(ot2, order)
      val ot4 = ot2.merge(ot1, order)

      var onesCount = 0
      var twosCount = 0

      ot3.tile.foreach({ z: Int =>
        if (z == 1) onesCount += 1 // from ot1
        else if (z == 2) twosCount += 1 // from ot2
      })
      onesCount should be (0) // from ot1
      twosCount should be (100) // from ot2

      onesCount = 0; twosCount = 0
      ot4.tile.foreach({ z: Int =>
        if (z == 1) onesCount += 1 // from ot1
        else if (z == 2) twosCount += 1 // from ot2
      })
      onesCount should be (0) // from ot1
      twosCount should be (100) // from ot2
    }

    it("should consider NODATA to be transparent") {
      val ot1 = TileFeature(ones, threes)
      val ar = IntArrayTile.fill(2, 10, 10)
      val ot2 = TileFeature(ar, sevens)

      // Poke a hole in the higher priority TileFeature, ot2
      ar.set(5, 5, NODATA)

      val ot3 = ot1.merge(ot2, order)

      var onesCount = 0
      var twosCount = 0

      ot3.tile.foreach({ z: Int =>
        if (z == 1) onesCount += 1 // from ot1
        else if (z == 2) twosCount += 1 // from ot2
      })
      onesCount should be (1) // from ot1
      twosCount should be (99) // from ot2
    }

    it("data should have as many bands as the least of the inputs") {
      val ot1 = TileFeature(ones, threes) // two bands
      val ot2 = TileFeature(ones,
        MultibandTile( // seven bands
          IntArrayTile.empty(10, 10),
          IntArrayTile.empty(10, 10),
          IntArrayTile.empty(10, 10),
          IntArrayTile.empty(10, 10),
          IntArrayTile.empty(10, 10),
          IntArrayTile.empty(10, 10),
          IntArrayTile.empty(10, 10)
        ))
      val ot3 = ot1.merge(ot2, order)
      val ot4 = ot2.merge(ot1, order)

      ot3.data.bandCount should be (2)
      ot4.data.bandCount should be (2)
    }

    it("data should interoperate with TileFeature[Tile,Tile]") {
      val ot1 = TileFeature(ones, threes) // two bands
      val ot2 = TileFeature(ones, IntArrayTile.empty(10, 10)) // one "band"
      val ot3 = ot1.merge(ot2, order)
      val ot4 = ot2.merge(ot1, order)

      ot3.data.bandCount should be (1)
      ot4.data shouldBe a [Tile]
    }

    it("should throw when self and other are dimensionally mismatched") {
      val ot1 = TileFeature(ones, IntArrayTile.empty(10, 10))
      val ot2 = TileFeature(IntArrayTile.empty(11, 11), IntArrayTile.empty(11, 11))
      intercept[IllegalArgumentException] {
        ot1.merge(ot2, order)
      }
    }

    it("should throw when self tile and data are dimensionally mismatched") {
      val ot1 = TileFeature(ones, IntArrayTile.empty(11, 11))
      val ot2 = TileFeature(ones, ones)
      intercept[IllegalArgumentException] {
        ot1.merge(ot2, order)
      }
    }

    it("should throw when other tile and data are dimensionally mismatched") {
      val ot1 = TileFeature(ones, ones)
      val ot2 = TileFeature(ones, IntArrayTile.empty(11, 11))
      intercept[IllegalArgumentException] {
        ot1.merge(ot2, order)
      }
    }

  }
}
