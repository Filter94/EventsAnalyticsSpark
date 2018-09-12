package com.griddynamics.analytics

object Helper {
  /**
    * @param ip string like "127.0.0.1"
    * @return integer which corresponds to the ip bitwise
    */
  def ipToInt(ip: Ip): Int = {
    val ipPortions = ip.split("\\.")
    ipPortions.foldLeft(0) {
      case (acc, next) =>
        (acc << 8) + next.toInt
    }
  }

  val IP_LENGTH = 32

  /**
    * @param maskLen length of a mask to be generated
    * @return bitwise mask of given length
    */
  def generateMask(maskLen: Int): Int = {
    var mask: Int = 0
    // same as pow(2, maskLen) - 1
    for {
      _ <- 0 until maskLen
    } {
      mask = (mask << 1) + 1
    }
    // fill with trailing zero to make it 32 bits long.
    for {
      _ <- 0 until IP_LENGTH - maskLen
    } {
      mask = (mask << 1) + 0
    }
    mask
  }

  /**
    * @param net bitwise representation of a net
    * @param mask mask to be applied to it
    */
  case class Net(net: Int, mask: Int)

  /**
    * Converts string to a case class
    * @param net string like "127.0.0.0/8" representing a net
    * @return corresponding case class
    */
  def netToStruct(net: Network): Net = {
    val slashPos = net.indexOf('/')
    if (slashPos > 0) {
      val ipString = net.substring(0, slashPos)
      val maskLength = net.substring(slashPos + 1).toInt
      val mask = generateMask(maskLength)
      Net(ipToInt(ipString) & mask, mask)
    } else {
      Net(0, 0)
    }
  }

  /**
    * @param ip bitwise representation of ip address
    * @param net case class representing a net
    * @return true if ip is in the net
    */
  def ipInSubnet(ip: Int, net: Net): Boolean = {
    (ip & net.mask) == (net.net & net.mask)
  }

  /**
    * @param ip string like "127.0.0.1"
    * @param net string like "127.0.0.0/8"
    * @return true if ip is in the net
    */
  def ipInNet(ip: Ip, net: Network): Boolean = {
    ipInSubnet(ipToInt(ip), netToStruct(net))
  }
}
