package cn.xiaoguangbiao.temp;

import java.util.Random;

/**
 * Author xiaoguangbiao
 * Desc 使用蒙特卡罗算法求PI
 */
public class Interview_4 {
    public static void main(String[] args) {
        Random random = new Random();
        int sum = 1000000000;//总共撒的点的数量
        int count = 0;//落在扇形区域的点的数量
        double  x = 0;
        double  y = 0;
        for (int i = 0; i < sum ;i++){
            x = random.nextDouble();//随机产生一个[0.0~1.0)之间的随机数
            y = random.nextDouble();//随机产生一个[0.0~1.0)之间的随机数
            if(x*x + y*y < 1){ //落在扇形区域
                count++;
            }
        }
        double  Pi = 4.0 * count / sum;
        System.out.println("计算出的Pi的值为:"+Pi);

    }
}
