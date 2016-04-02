package geotrellis.raster.bundle

import geotrellis.raster._


object Implicits extends Implicits

trait Implicits {
  implicit def singlebandBundleMethodsToBundleMethods[D: ? => SinglebandTileBundleMethods](x: D): BundleMethods[D] =
    implicitly[SinglebandTileBundleMethods](x).asInstanceOf[BundleMethods[D]]

  implicit def multibandBundleMethodsToBundleMethods[D: ? => MultibandTileBundleMethods](x: D): BundleMethods[D] =
    implicitly[MultibandTileBundleMethods](x).asInstanceOf[BundleMethods[D]]

  implicit def seqBundleMethodsToBundleMethods[D: ? => TileSeqBundleMethods](x: D): BundleMethods[D] =
    implicitly[TileSeqBundleMethods](x).asInstanceOf[BundleMethods[D]]
}
