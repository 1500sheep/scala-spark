package common

object CheckDistance {
  //Calculate distance1
  def checkDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double) = {
    val theta = lon1 - lon2
    var dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad((lat2))) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta))

    dist = Math.acos(dist)
    dist = rad2deg(dist)
    dist = dist * 60 * 1.1515

    dist = dist * 1609.344

    dist
  }

  //Calculate distance2
  def deg2rad(deg: Double): Double = {
    (deg * Math.PI / 180.0)
  }

  //Calculate distance3
  def rad2deg(rad: Double): Double = {
    (rad * 180 / Math.PI)
  }
}
