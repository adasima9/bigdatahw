package `case`

case class UserInfo(
                   imsi:String,
                   gender:Int,
                   age:Int
                   )

object UserInfo{
  def fromStr(record: String): UserInfo = {
    val arr = record.split("\\|")
    val imsi = arr(0)
    val gender = arr(1).toInt
    val age = arr(2).toInt
    UserInfo(imsi,gender,age)
  }
}
