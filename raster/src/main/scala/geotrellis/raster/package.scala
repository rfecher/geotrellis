/*
 * Copyright (c) 2014 Azavea.
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

package geotrellis

import geotrellis.vector.Point
import geotrellis.macros.{ NoDataMacros, TypeConversionMacros }
import geotrellis.vector.{Geometry, Feature}
import geotrellis.util.MethodExtensions


package object raster
    extends crop.Implicits
    with bundle.Implicits
    with geotrellis.raster.mapalgebra.focal.hillshade.Implicits
    with mask.Implicits
    with merge.Implicits
    with reproject.Implicits
    with split.Implicits {
  type CellType = DataType with NoDataHandling
  type SinglebandRaster = Raster[Tile]
  type MultibandRaster = Raster[MultibandTile]

  // Implicit method extension for core types

  implicit class withRasterExtentRasterizeMethods(val self: RasterExtent) extends MethodExtensions[RasterExtent]
      with rasterize.RasterExtentRasterizeMethods[RasterExtent]

  implicit class withGeometryRasterizeMethods(val self : Geometry) extends MethodExtensions[Geometry]
      with rasterize.GeometryRasterizeMethods

  implicit class withFeatureIntRasterizeMethods(val self : Feature[Geometry, Int]) extends MethodExtensions[Feature[Geometry, Int]]
      with rasterize.FeatureIntRasterizeMethods[Geometry]

  implicit class withFeatureDoubleRasterizeMethods(val self : Feature[Geometry, Double]) extends MethodExtensions[Feature[Geometry, Double]]
      with rasterize.FeatureDoubleRasterizeMethods[Geometry]

  implicit class withTileMethods(val self: Tile) extends MethodExtensions[Tile]
      with bundle.SinglebandTileBundleMethods
      with costdistance.CostDistanceMethods
      with crop.SinglebandTileCropMethods
      with hydrology.HydrologyMethods
      with mask.SinglebandTileMaskMethods
      with merge.SinglebandTileMergeMethods
      with mapalgebra.local.LocalMethods
      with mapalgebra.focal.FocalMethods
      with mapalgebra.zonal.ZonalMethods
      with mapalgebra.focal.hillshade.HillshadeMethods
      with prototype.SinglebandTilePrototypeMethods
      with regiongroup.RegionGroupMethods
      with render.ColorMethods
      with render.JpgRenderMethods
      with render.PngRenderMethods
      with reproject.SinglebandTileReprojectMethods
      with resample.SinglebandTileResampleMethods
      with split.SinglebandTileSplitMethods
      with summary.SummaryMethods
      with summary.polygonal.PolygonalSummaryMethods
      with viewshed.ViewshedMethods
      with vectorize.VectorizeMethods

  implicit class withMultibandTileMethods(val self: MultibandTile) extends MethodExtensions[MultibandTile]
      with crop.MultibandTileCropMethods
      with mask.MultibandTileMaskMethods
      with merge.MultibandTileMergeMethods
      with prototype.MultibandTilePrototypeMethods
      with reproject.MultibandTileReprojectMethods
      with render.MultibandJpgRenderMethods
      with render.MultibandColorMethods
      with render.MultibandPngRenderMethods
      with resample.MultibandTileResampleMethods
      with split.MultibandTileSplitMethods

  implicit class withSinglebandRasterMethods(val self: SinglebandRaster) extends MethodExtensions[SinglebandRaster]
      with reproject.SinglebandRasterReprojectMethods
      with resample.SinglebandRasterResampleMethods
      with vectorize.SinglebandRasterVectorizeMethods

  implicit class withMultibandRasterMethods(val self: MultibandRaster) extends MethodExtensions[MultibandRaster]
      with reproject.MultibandRasterReprojectMethods
      with resample.MultibandRasterResampleMethods

  implicit class withTileSeqMethods(val self: Traversable[Tile]) extends MethodExtensions[Traversable[Tile]]
      with mapalgebra.local.LocalSeqMethods

  implicit class SinglebandRasterAnyRefMethods(val self: SinglebandRaster) extends AnyRef {
    def getValueAtPoint(point: Point): Int =
      getValueAtPoint(point.x, point.y)

    def getValueAtPoint(x: Double, y: Double): Int =
      self.tile.get(
        self.rasterExtent.mapXToGrid(x),
        self.rasterExtent.mapYToGrid(y)
      )

    def getDoubleValueAtPoint(point: Point): Double =
      getDoubleValueAtPoint(point.x, point.y)

    def getDoubleValueAtPoint(x: Double, y: Double): Double =
      self.tile.getDouble(
        self.rasterExtent.mapXToGrid(x),
        self.rasterExtent.mapYToGrid(y)
      )
  }

  implicit class TraversableTileExtensions(rs: Traversable[Tile]) {
    def assertEqualDimensions(): Unit =
      if(Set(rs.map(_.dimensions)).size != 1) {
        val dimensions = rs.map(_.dimensions).toSeq
        throw new GeoAttrsError("Cannot combine tiles with different dimensions." +
          s"$dimensions are not all equal")
      }
  }

  implicit class TileTupleExtensions(t: (Tile, Tile)) {
    def assertEqualDimensions(): Unit =
      if(t._1.dimensions != t._2.dimensions) {
        throw new GeoAttrsError("Cannot combine rasters with different dimensions." +
          s"${t._1.dimensions} does not match ${t._2.dimensions}")
      }
  }

  type DI = DummyImplicit

  type IntTileMapper = macros.IntTileMapper
  type DoubleTileMapper = macros.DoubleTileMapper
  type IntTileVisitor = macros.IntTileVisitor
  type DoubleTileVisitor = macros.DoubleTileVisitor

  // Keep constant values in sync with macro functions
  @inline final val byteNODATA = Byte.MinValue
  @inline final val ubyteNODATA = 0.toByte
  @inline final val shortNODATA = Short.MinValue
  @inline final val ushortNODATA = 0.toShort
  @inline final val NODATA = Int.MinValue
  @inline final val floatNODATA = Float.NaN
  @inline final val doubleNODATA = Double.NaN

  def isNoData(i: Int): Boolean = macro NoDataMacros.isNoDataInt_impl
  def isNoData(f: Float): Boolean = macro NoDataMacros.isNoDataFloat_impl
  def isNoData(d: Double): Boolean = macro NoDataMacros.isNoDataDouble_impl

  def isData(i: Int): Boolean = macro NoDataMacros.isDataInt_impl
  def isData(f: Float): Boolean = macro NoDataMacros.isDataFloat_impl
  def isData(d: Double): Boolean = macro NoDataMacros.isDataDouble_impl

  def b2ub(n: Byte): Byte = macro TypeConversionMacros.b2ub_impl
  def b2s(n: Byte): Short = macro TypeConversionMacros.b2s_impl
  def b2us(n: Byte): Short = macro TypeConversionMacros.b2us_impl
  def b2i(n: Byte): Int = macro TypeConversionMacros.b2i_impl
  def b2f(n: Byte): Float = macro TypeConversionMacros.b2f_impl
  def b2d(n: Byte): Double = macro TypeConversionMacros.b2d_impl

  def ub2b(n: Byte): Byte = macro TypeConversionMacros.ub2b_impl
  def ub2s(n: Byte): Short = macro TypeConversionMacros.ub2s_impl
  def ub2us(n: Byte): Short = macro TypeConversionMacros.ub2us_impl
  def ub2i(n: Byte): Int = macro TypeConversionMacros.ub2i_impl
  def ub2f(n: Byte): Float = macro TypeConversionMacros.ub2f_impl
  def ub2d(n: Byte): Double = macro TypeConversionMacros.ub2d_impl

  def s2b(n: Short): Byte = macro TypeConversionMacros.s2b_impl
  def s2ub(n: Short): Byte = macro TypeConversionMacros.s2ub_impl
  def s2us(n: Short): Short = macro TypeConversionMacros.s2us_impl
  def s2i(n: Short): Int = macro TypeConversionMacros.s2i_impl
  def s2f(n: Short): Float = macro TypeConversionMacros.s2f_impl
  def s2d(n: Short): Double = macro TypeConversionMacros.s2d_impl

  def us2b(n: Short): Byte = macro TypeConversionMacros.us2b_impl
  def us2ub(n: Short): Byte = macro TypeConversionMacros.us2ub_impl
  def us2s(n: Short): Short = macro TypeConversionMacros.us2s_impl
  def us2i(n: Short): Int = macro TypeConversionMacros.us2i_impl
  def us2f(n: Short): Float = macro TypeConversionMacros.us2f_impl
  def us2d(n: Short): Double = macro TypeConversionMacros.us2d_impl

  def i2b(n: Int): Byte = macro TypeConversionMacros.i2b_impl
  def i2ub(n: Int): Byte = macro TypeConversionMacros.i2ub_impl
  def i2s(n: Int): Short = macro TypeConversionMacros.i2s_impl
  def i2us(n: Int): Short = macro TypeConversionMacros.i2us_impl
  def i2f(n: Int): Float = macro TypeConversionMacros.i2f_impl
  def i2d(n: Int): Double = macro TypeConversionMacros.i2d_impl

  def f2b(n: Float): Byte = macro TypeConversionMacros.f2b_impl
  def f2ub(n: Float): Byte = macro TypeConversionMacros.f2ub_impl
  def f2s(n: Float): Short = macro TypeConversionMacros.f2s_impl
  def f2us(n: Float): Short = macro TypeConversionMacros.f2us_impl
  def f2i(n: Float): Int = macro TypeConversionMacros.f2i_impl
  def f2d(n: Float): Double = macro TypeConversionMacros.f2d_impl

  def d2b(n: Double): Byte = macro TypeConversionMacros.d2b_impl
  def d2ub(n: Double): Byte = macro TypeConversionMacros.d2ub_impl
  def d2s(n: Double): Short = macro TypeConversionMacros.d2s_impl
  def d2us(n: Double): Short = macro TypeConversionMacros.d2us_impl
  def d2i(n: Double): Int = macro TypeConversionMacros.d2i_impl
  def d2f(n: Double): Float = macro TypeConversionMacros.d2f_impl

  // Use this implicit class to fill arrays ... much faster than Array.fill[Int](dim)(val), etc.
  implicit class ByteArrayFiller(val arr: Array[Byte]) extends AnyVal {
    def fill(v: Byte) = { java.util.Arrays.fill(arr, v) ; arr }
  }
  implicit class ShortArrayFiller(val arr: Array[Short]) extends AnyVal {
    def fill(v: Short) = { java.util.Arrays.fill(arr, v) ; arr }
  }
  implicit class IntArrayFiller(val arr: Array[Int]) extends AnyVal {
    def fill(v: Int) = { java.util.Arrays.fill(arr, v) ; arr }
  }
  implicit class FloatArrayFiller(val arr: Array[Float]) extends AnyVal {
    def fill(v: Float) = { java.util.Arrays.fill(arr, v) ; arr }
  }
  implicit class DoubleArrayFiller(val arr: Array[Double]) extends AnyVal {
    def fill(v: Double) = { java.util.Arrays.fill(arr, v) ; arr }
  }
}
