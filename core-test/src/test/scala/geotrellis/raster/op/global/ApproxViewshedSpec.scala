package geotrellis.raster.op.global

import geotrellis._
import geotrellis.source._
import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

import geotrellis.testkit._
import geotrellis.raster.BitConstant

/**
 * Created by jchien on 5/1/14.
 */
class ApproxViewshedSpec extends FunSpec
                            with ShouldMatchers
                            with TestServer
                            with RasterBuilders {
  describe("Viewshed") {
    //    it("computes the viewshed of a flat int plane") {
    //      val r = createRaster(Array.fill(7*8)(1), 7, 8)
    //      assertEqual(Raster(BitConstant(true, 7, 8), r.rasterExtent), ApproxViewshed.approxComputeMinHeightViewable(3, 4, r))
    //    }
    //
    //    it("computes the viewshed of a flat double plane") {
    //      val r = createRaster(Array.fill(7*8)(1.5), 7, 8)
    //      assertEqual(Raster(BitConstant(true, 7, 8), r.rasterExtent), ApproxViewshed.approxComputeMinHeightViewable(3, 4, r))
    //    }
    //
    //    it("computes the viewshed of a double line") {
    //      val rasterData = Array (
    //        300.0, 1.0, 99.0, 0.0, 10.0, 200.0, 137.0
    //      )
    //      val viewable = Array (
    //        1, 0, 1, 1, 1, 1, 0
    //      )
    //      val r = createRaster(rasterData, 7, 1)
    //      val viewRaster = createRaster(viewable, 7, 1)
    //      assertEqual(viewRaster, ApproxViewshed.approxComputeMinHeightViewable(0, 3, r))
    //    }
    //
    //    it("computes the viewshed of a double plane") {
    //      val rasterData = Array (
    //        999.0, 1.0,   1.0,   1.0,   1.0,   1.0,   999.0,
    //        1.0,   1.0,   1.0,   1.0,   1.0,   499.0, 1.0,
    //        1.0,   1.0,   1.0,   1.0,   99.0,  1.0,   1.0,
    //        1.0,   1.0,   999.0, 1.0,   1.0,   1.0,   1.0,
    //        1.0,   1.0,   1.0,   1.0,   100.0, 1.0,   1.0,
    //        1.0,   1.0,   1.0,   1.0,   1.0,   101.0, 1.0,
    //        1.0,   1.0,   1.0,   1.0,   1.0,   1.0,   102.0
    //      )
    //      val viewable = Array (
    //          1,     1,     1,     1,     0,     0,     1,
    //          0,     1,     1,     1,     0,     1,     0,
    //          0,     0,     1,     1,     1,     0,     0,
    //          0,     0,     1,     1,     1,     1,     1,
    //          0,     0,     1,     1,     1,     0,     0,
    //          0,     1,     1,     1,     0,     0,     0,
    //          1,     1,     1,     1,     0,     0,     0
    //      )
    //      val r = createRaster(rasterData, 7, 7)
    //      val viewRaster = createRaster(viewable, 7, 7)
    //      assertEqual(viewRaster, ApproxViewshed.approxComputeMinHeightViewable(3, 3, r))
    //    }
    //
    //    it("computes the viewshed of a int plane") {
    //      val rasterData = Array (
    //        999, 1,   1,   1,   1,   499, 999,
    //        1,   1,   1,   1,   1,   499, 250,
    //        1,   1,   1,   1,   99,  1,   1,
    //        1,   999, 1,   1,   1,   1,   1,
    //        1,   1,   1,   1,   1,   1,   1,
    //        1,   1,   1,   0,   1,   1,   1,
    //        1,   1,   1,   1,   1,   1,   1
    //      )
    //      val viewable = Array (
    //        1,     1,     1,     1,     0,     1,     1,
    //        1,     1,     1,     1,     0,     1,     1,
    //        0,     1,     1,     1,     1,     0,     0,
    //        0,     1,     1,     1,     1,     1,     1,
    //        0,     1,     1,     1,     1,     1,     1,
    //        1,     1,     1,     0,     1,     1,     1,
    //        1,     1,     1,     1,     1,     1,     1
    //      )
    //      val r = createRaster(rasterData, 7, 7)
    //      val viewRaster = createRaster(viewable, 7, 7)
    //      assertEqual(viewRaster, ApproxViewshed.approxComputeMinHeightViewable(3, 3, r))
    //    }

    it("computes the approx viewshed of a int plane") {
      val rasterData = Array (
        999, 1,   1,   1,   1,   499, 999,
        1,   1,   1,   1,   1,   499, 250,
        1,   1,   1,   1,   99,  1,   1,
        1,   999, 1,   1,   1,   1,   1,
        1,   1,   1,   1,   1,   1,   1,
        1,   1,   1,   0,   1,   1,   1,
        1,   1,   1,   1,   1,   1,   1
      )
      val viewable = Array (
        1,     1,     1,     1,     0,     1,     1,
        1,     1,     1,     1,     0,     1,     0, // this cell will be incorrectly computed due to the algorithm
        0,     1,     1,     1,     1,     0,     0,
        0,     1,     1,     1,     1,     1,     1,
        0,     1,     1,     1,     1,     1,     1,
        1,     1,     1,     0,     1,     1,     1,
        1,     1,     1,     1,     1,     1,     1
      )
      val r = createRaster(rasterData, 7, 7)
      val viewRaster = createRaster(viewable, 7, 7)
      assertEqual(viewRaster.localEqual(createRaster(Array.fill(7*7)(1), 7, 7)), ApproxViewshed.approxComputeViewable(3, 3, r))
    }

    it("should match viewshed generated by ArgGIS") { // discrepancy of 6.7% with ESRI, 6.6% with exact algo
      val elevation = RasterSource("viewshed-elevation").get
      val expected = RasterSource("viewshed-expected").get

      val (x,y) = (-93.63300872055451407, 30.54649743277299123) // create overload
      val (col, row) = elevation.rasterExtent.mapToGrid(x, y)
      val actual = ApproxViewshed.approxComputeViewable(row, col, elevation)

      def countDiff(a:Raster, b:Raster):Int = {
        var ans = 0
        for(col <- 0 until 256) {
          for(row <- 0 until 256) {
            if (a.get(col, row) != b.get(col,row)) ans += 1;
          }
        }
        ans
      }
      System.out.println(countDiff(expected,actual))
      System.out.println(countDiff(Viewshed.computeViewable(row,col,elevation),actual))
     // assertEqual(ApproxViewshed.approxComputeViewable(row, col, elevation), expected)
    }
  }
}
