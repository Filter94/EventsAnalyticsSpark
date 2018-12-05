package com.griddynamics.analytics

import org.scalatest.{FunSuite, PrivateMethodTester}

class HelperTest extends FunSuite with PrivateMethodTester {
  test("ipInNet works for some simple positive cases") {
    assert(Helper.ipInNet("127.128.129.130", "127.128.129.0/24"))
    //                                   ...11.b                  ...10.b
    assert(Helper.ipInNet("127.128.3.130", "127.128.2.0/23"))
    assert(Helper.ipInNet("127.128.129.0", "127.128.129.0/24"))
    //                                   ...11.b                  ...10.b
    assert(Helper.ipInNet("127.128.3.0", "127.128.2.0/23"))
  }

  test("ipInNet works for some simple negative cases") {
    assert(!Helper.ipInNet("127.128.129.130", "127.128.2.0/23"))
    assert(!Helper.ipInNet("127.122.129.130", "127.128.129.0/24"))
    assert(!Helper.ipInNet("127.128.129.0", "127.128.2.0/23"))
    assert(!Helper.ipInNet("127.122.129.130", "127.128.129.0/24"))
  }
}
