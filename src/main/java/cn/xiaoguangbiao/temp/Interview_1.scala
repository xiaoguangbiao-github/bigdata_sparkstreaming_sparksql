package cn.xiaoguangbiao.temp

import scala.collection.mutable.ListBuffer

/**
 * Author xiaoguangbiao
 * Date 2020/12/26 18:01
 * Desc 练习题--使用多种方式交换2个变量的值
 */
object Interview_1 {
  def main(args: Array[String]): Unit = {
    var a = 1
    var b = 2
    println(a,b) // 1,2

    //方式1:使用临时变量
    val temp = a //1
    a = b //2
    b = temp //1
    println(a,b) //2,1

    //方式2:使用算术运算符
    a = a + b //3
    b = a - b //2
    a = a - b //1
    println(a,b)//1,2

    //方式3:使用位运算符
    a = a ^ b // a ^ b
    b = a ^ b // a ^ b ^ b == a ^ 0 == a
    a = a ^ b // a ^ b ^ a == b ^ 0 == b
    println(a,b)//2,1

    //解释:^异或位运算符,特点:相同为0,不同为1
    //a ^ b 记为  a ^ b
    //a ^ b ^ b == a ^ 0
    //一个数异或0 == 这个数本身
    //a ^ 0 == a
    //a ^ b ^ a = b ^ 0 == b

    //^的其他用法
    val key = 66666666 //密钥
    val password = 88888888//原密码
    val password1 = password ^ key //加密后的密码
    val password2 = password1 ^ key //解密后的密码
    println(password)//88888888
    println(password1)//112531090
    println(password2)//88888888

    //^的其他用法,有一个海量连续数据集合A,如1~N,其中丢了一个数,现在让快速找出丢的数
    //用异或将A集合和B集合(0~N没有丢失的)的元素一一遍历异或
    val A = ListBuffer(1,2,3,4,5,6,7,8,9)
    val B = ListBuffer(1,2,3,4,5,6,7,8,9,10)
    A ++=B //[1,2,3,4,5,6,7,8,9,1,2,3,4,5,6,7,8,9,10]
    //现在A里面就有了A和B所有的元素
    var result = 0
    for(i <- A){
      result ^= i
    }
    println("丢失的数为:"+result)
  }
}
