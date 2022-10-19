import java.util.ArrayList;
import java.util.Scanner;

/**
 * @author : joisen
 * @date : 10:19 2022/10/15
 */
public class ChangeToHuiWen {

    /**
     * 逐一对序列的第一个元素和第二个元素进行比较：
     *      first < last: 则第一个元素与第二个元素进行相加 添加回序列，并删除第二个元素
     *      first > last: 则最后一个元素和倒数第二个元素相加，添加回序列，并删除倒数第二个元素
     *      first = last: 则first++  last--，继续比较序列内的元素
     */
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        while(sc.hasNext()){
            int n = sc.nextInt();
            int[] num = new int[n];
            for (int i = 0; i < n; i++) {
                num[i] = sc.nextInt();
            }
            System.out.println(timeToHuiWen(num,n));
        }
    }
    public static int timeToHuiWen(int[] num,int n){
        int res = 0;
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            list.add(num[i]);
        }

        while(list.size() > 1){
            if(list.get(0) < list.get(list.size()-1)){ // 第一个小于最后一个
                int a = list.get(0);
                int b = list.get(1);
                list.set(0,a+b);
                list.remove(1);
                res ++;
            }else if(list.get(0) > list.get(list.size()-1)){// 第一个大于最后一个
                int a = list.get(list.size()-1);
                int b = list.get(list.size()-2);
                list.set(list.size()-2,a+b);
                list.remove(list.size()-1);
                res ++;

            }else{ // 如果第一个元素==最后一个元素  直接跳过这两个元素
                list.remove(0);
                list.remove(list.size()-1);
            }
        }

        return res;
    }



}
