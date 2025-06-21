package `case`

case class XDR(
              imsi:String,
              laccell:String,
              lat:Double,
              lot:Double,
              startTime:String
              )

object XDR{
  def fromKafka(record: String): XDR = {
    val arr = record.split("\\|")
    val imsi = arr(0)
    val laccell = arr(1)
    val lat = arr(2).toDouble
    val lot = arr(3).toDouble
    val startTime = arr(4)
    XDR(imsi, laccell, lat, lot, startTime)
  }
}
