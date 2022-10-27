package com.eurlanda.datashire.engine.scala

/**
 * Created by zhudebin on 15/10/28.
 */
object CollectionTests {

  def main(args: Array[String]) {
//    test2()
    testBuffer()
  }

  def test1(): Unit = {
    //Array对应的可变ArrayBuffer：

    val ab = collection.mutable.ArrayBuffer[Int]()

    ab += (1,3,5,7)

    ab ++= List(9,11) // ArrayBuffer(1, 3, 5, 7, 9, 11)

    ab toArray // Array (1, 3, 5, 7, 9, 11)

    ab clear // ArrayBuffer()
  }

  def test2(): Unit = {
    val bytes = "helloworld".getBytes.mkString("[", ",", "]")
    println(bytes)
  }

  def testIter(): Unit = {
    val seq = Seq(1,2,3,4,5)
    val iter = seq.iterator
//    println(iter.size)
    println(iter.hasNext)
    println(iter.hasNext)
    while(iter.hasNext) {
      val next = iter.next()
      println(s"===${next}")
    }
  }

  def testBuffer(): Unit = {
    val sb = new StringBuilder()
    val as = if(1>2){
      "1"
    } else {
      null
    }
    sb.append(as).append("hh")
    println(sb.toString())
  }
}
