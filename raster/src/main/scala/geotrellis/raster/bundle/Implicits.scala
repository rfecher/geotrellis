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


object Implicits extends Implicits

trait Implicits {

  /**
    * An implicit conversion from a SinglebandTileBundleMethods to a
    * BundleMethods.
    */
  implicit def singlebandBundleMethodsToBundleMethods[D: ? => SinglebandTileBundleMethods](x: D): BundleMethods[D] =
    implicitly[SinglebandTileBundleMethods](x).asInstanceOf[BundleMethods[D]]

  /**
    * An implicit conversion from a MultibandTileBundleMethods to a
    * BundleMethods.
    */
  implicit def multibandBundleMethodsToBundleMethods[D: ? => MultibandTileBundleMethods](x: D): BundleMethods[D] =
    implicitly[MultibandTileBundleMethods](x).asInstanceOf[BundleMethods[D]]

  /**
    * An implicit conversion from a TileSeqBundleMethods to a
    * BundleMethods.
    */
  implicit def seqBundleMethodsToBundleMethods[D: ? => TileSeqBundleMethods](x: D): BundleMethods[D] =
    implicitly[TileSeqBundleMethods](x).asInstanceOf[BundleMethods[D]]
}
