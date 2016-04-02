package geotrellis.raster.merge

import geotrellis.raster._
import geotrellis.raster.bundle._


object Implicits extends Implicits

/**
  * A trait holding the implicit class which makes the extensions
  * methods available.
  */
trait Implicits {
  implicit class withRasterMergeMethods[T <: CellGrid: ? => TileMergeMethods[T]](self: Raster[T])
      extends RasterMergeMethods[T](self)

  implicit class withTileFeatureMergeMethods[T <: Tile, D: ? => BundleMethods[D]](self: TileFeature[T,D])
      extends TileFeatureMergeMethods[T,D](self)
}
