package utils

import java.io.{ByteArrayInputStream, DataInputStream}

import org.roaringbitmap.RoaringBitmap

/**
 * Created by xuwei
 */
object RoaringBitmapUtil {

  def bytes2RoaringBitmap(arr: Array[Byte]): RoaringBitmap ={
    val bitMap = new RoaringBitmap()
    val byteArrayIs = new ByteArrayInputStream(arr)
    val dataIs = new DataInputStream(byteArrayIs)
    bitMap.deserialize(dataIs)
    bitMap
  }

}
