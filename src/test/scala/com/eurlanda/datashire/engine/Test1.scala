package com.eurlanda.datashire.engine

/**
 * Created by zhudebin on 14-6-5.
 */
object Test1 extends App {

    var oa = Array(1,2,3,4,5,6,7,8,9).foreach(i => {
        (i.toString -> i)
    })


    println(oa)

    println((0 until 20).mkString(","))


    val a = "abcdefghalksdjfasd;fasd"
    val b = "abcdefghalksdjfasd;fasd"
    val abs:Array[Byte] = a.getBytes.asInstanceOf[Array[Byte]]
    val bbs:Array[Byte] = b.getBytes.asInstanceOf[Array[Byte]]
    println(abs.equals(bbs))
    println(abs.eq(bbs))
    println(abs == bbs)

    println(sum(List(1,2,3)))

    def sum(s:List[Int]):Int = {
        s match {
            case i::Nil => i
            case a::tail   => a + sum(tail)
        }
    }

}
