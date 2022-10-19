/**
 * @author : joisen
 * @date : 10:11 2022/10/15
 */
public class test1 {

    public void change(int x){
        x = 100;
        System.out.println("x::::"+x);
    }

    public static void main(String[] args) {
        int x = 1;
        test1 t = new test1();
        t.change(x);
        System.out.println(x);
    }

}
