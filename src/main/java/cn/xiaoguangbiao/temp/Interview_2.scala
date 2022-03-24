package cn.xiaoguangbiao.temp

/**
 * Author xiaoguangbiao
 * Desc 练习题--谷歌爬楼梯
 * Google 曾询问应征者 ：有N阶楼梯 ，你每次只能爬1或2 阶 楼梯；能有多少种方法?
 */
object Interview_2 {
  def main(args: Array[String]): Unit = {
    println(getSum2(1))//1
    println(getSum2(2))//2
    println(getSum2(3))//3
    println(getSum2(4))//5
    println(getSum2(5))//8
    println(getSum2(6))//13
    println(getSum2(50))//


  }
  //定义一个方法用来求爬N级阶梯的爬法种数
  //解法1:使用递归
  def getSum(n:Int):Int={
    if(n <= 2){
      n
    }else{
      getSum(n-1) + getSum(n-2)
    }
  }

  //定义一个方法用来求爬N级阶梯的爬法种数
  //解法2:优化
  //1 2 3 5 8 13
  def getSum2(n:Long):Long={
    var first: Long= 1
    var second: Long = 2
    for(i <- 3L to n){
      second = first + second //3
      first = second - first //2
    }
    second
  }
}
