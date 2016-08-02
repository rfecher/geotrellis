package geotrellis.spark.io.geowave

import com.esotericsoftware.kryo.Kryo
import mil.nga.giat.geowave.core.index.Persistable
import com.esotericsoftware.kryo.Serializer
import org.apache.accumulo.core.data.Key
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input
import mil.nga.giat.geowave.core.index.PersistenceUtils
import geotrellis.spark.io.kryo.KryoRegistrator

object GeowaveKryoRegistrator {
  // GeoWave registrator
class GeoWaveKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) = {
    kryo.addDefaultSerializer(classOf[Persistable], new PersistableSerializer())
    kryo.register(classOf[Key])
    super.registerClasses(kryo)
  }
}

//Default serializer for any GeoWave Persistable object
class PersistableSerializer extends Serializer[Persistable] {
  override def write(kryo: Kryo, output: Output, geowaveObj: Persistable): Unit = {
    val bytes = PersistenceUtils.toBinary(geowaveObj);
    output.writeInt(bytes.length)
    output.writeBytes(bytes)
  }

  override def read(kryo: Kryo, input: Input, t: Class[Persistable]): Persistable = {
    val length = input.readInt()
    val bytes = new Array[Byte](length)
    input.read(bytes)

    PersistenceUtils.fromBinary(bytes, classOf[Persistable])
  }
}
}