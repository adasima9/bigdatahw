package `case`

case class RegionCell(
                       regionId:String,
                       laccell:String
                     )

object RegionCell{
  def fromStr(record: String): RegionCell = {
    val arr = record.split("\\|")
    val regionId = arr(0)
    val laccell = arr(1)
    RegionCell(regionId,laccell)
  }
}

