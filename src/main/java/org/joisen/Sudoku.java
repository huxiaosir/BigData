package org.joisen;

import java.util.ArrayList;
import java.util.List;

/**
 * @author : joisen
 * @date : 17:23 2022/9/18
 */
public class Sudoku {
    // 记录最终的结果
    static int  count = 0;
    // 定义所有类型的枚举
    enum SDKType{TL,TR,MD,BL,BR};
    // 记录每张表中所有的数独解
    static List<int[][]> mids = new ArrayList<>();
    static List<int[][]> tls = new ArrayList<>();
    static List<int[][]> trs = new ArrayList<>();
    static List<int[][]> bls = new ArrayList<>();
    static List<int[][]> brs = new ArrayList<>();
    public static void main(String[] args) {
        int[][] sdk1 = {
                {0,2,3, 0,0,7, 5,0,0, 0,0,0, 3,0,4, 0,0,7, 0,2,0},
                {0,0,7, 0,0,0, 1,0,0, 0,0,0, 0,0,0, 0,2,0, 0,8,0},
                {0,0,4, 0,0,3, 8,0,0, 0,0,0, 0,8,0, 5,0,1, 0,0,0},

                {3,0,0, 0,6,8, 0,9,0, 0,0,0, 5,0,0, 0,0,0, 3,0,0},
                {1,0,0, 0,7,2, 6,0,5, 0,0,0, 6,2,0, 4,0,0, 0,0,1},
                {0,0,0, 5,0,0, 0,0,0, 0,0,0, 0,0,7, 0,0,0, 0,0,0},

                {0,0,0, 8,0,0, 0,0,0, 0,1,6, 0,0,0, 0,0,6, 0,0,5},
                {0,9,0, 0,0,0, 0,2,0, 0,0,0, 0,1,8, 9,0,0, 0,0,0},
                {0,4,6, 0,0,0, 0,8,0, 2,0,3, 4,0,0, 8,1,3, 7,0,0},

                {0,0,0, 0,0,0, 2,0,0, 0,0,0, 0,0,9, 0,0,0, 0,0,0},
                {0,0,0, 0,0,0, 0,0,4, 0,0,0, 6,0,3, 0,0,0, 0,0,0},
                {0,0,0, 0,0,0, 0,0,5, 0,0,0, 0,2,1, 0,0,0, 0,0,0},

                {0,8,0, 0,0,0, 0,4,0, 0,0,0, 0,0,0, 0,0,0, 0,7,0},
                {0,3,5, 0,0,7, 0,0,9, 0,0,8, 3,5,7, 8,0,2, 0,0,0},
                {0,0,4, 0,2,5, 8,0,0, 0,5,0, 0,0,0, 0,0,7, 0,1,3},

                {0,0,0, 0,0,0, 0,0,8, 0,0,0, 0,0,0, 0,2,0, 0,9,0},
                {0,6,9, 0,0,0, 4,2,0, 0,0,0, 5,0,0, 9,0,0, 0,0,0},
                {0,0,0, 0,0,0, 1,0,0, 0,0,0, 6,0,0, 0,8,3, 0,0,0},

                {6,7,0, 8,0,0, 0,0,1, 0,0,0, 8,0,2, 4,0,0, 7,0,0},
                {0,4,0, 2,0,0, 0,0,5, 0,0,0, 0,0,0, 0,9,0, 2,0,0},
                {0,0,1, 0,7,0, 3,0,0, 0,0,0, 0,4,3, 0,0,5, 0,0,0},
        };
        int[][] sdk2 = {
                {0,0,1, 0,2,0, 3,0,7, 0,0,0, 0,9,0, 0,0,0, 0,0,0},
                {0,4,0, 0,0,7, 6,2,0, 0,0,0, 0,0,2, 7,0,3, 0,0,0},
                {0,0,0, 0,0,0, 9,0,0, 0,0,0, 7,0,0, 1,5,0, 0,0,6},

                {0,0,4, 5,1,9, 0,7,0, 0,0,0, 0,1,0, 0,0,9, 8,0,0},
                {0,0,0, 4,0,3, 2,9,0, 0,0,0, 2,0,0, 5,0,0, 0,6,0},
                {0,6,0, 0,0,0, 0,0,0, 0,0,0, 0,0,0, 6,1,0, 0,0,0},

                {0,2,0, 0,0,5, 0,0,0, 0,0,0, 5,0,0, 4,3,0, 0,0,9},
                {0,7,0, 0,9,0, 0,6,0, 0,0,0, 0,7,0, 9,2,6, 3,4,0},
                {0,0,6, 8,0,0, 7,0,2, 8,6,0, 0,0,4, 0,0,0, 0,0,1},

                {0,0,0, 0,0,0, 0,0,4, 0,1,3, 0,0,2, 0,0,0, 0,0,0},
                {0,0,0, 0,0,0, 6,0,0, 0,0,0, 0,1,0, 0,0,0, 0,0,0},
                {0,0,0, 0,0,0, 0,0,0, 9,2,0, 0,0,5, 0,0,0, 0,0,0},

                {5,9,2, 0,7,0, 0,4,0, 0,0,8, 0,0,7, 0,0,0, 0,0,9},
                {0,0,0, 1,0,0, 0,5,0, 3,0,4, 1,0,0, 0,0,4, 0,0,3},
                {3,0,6, 5,0,0, 9,0,0, 0,0,0, 0,0,0, 9,0,2, 0,0,0},

                {8,0,0, 0,2,0, 0,0,0, 0,0,0, 8,0,0, 3,0,0, 0,0,0},
                {7,0,0, 0,0,0, 8,0,3, 0,0,0, 0,0,0, 4,0,0, 0,7,0},
                {0,2,0, 0,0,6, 0,0,0, 0,0,0, 0,0,0, 0,2,0, 1,0,4},

                {0,0,0, 0,6,0, 0,0,9, 0,0,0, 0,0,2, 0,0,7, 0,5,8},
                {0,4,0, 0,0,0, 1,0,0, 0,0,0, 0,0,6, 0,0,0, 0,4,0},
                {0,5,0, 9,0,0, 7,0,4, 0,0,0, 9,0,5, 0,0,0, 2,0,1},
        };
        Sudoku sd = new Sudoku();
        sd.solve(sdk1);
        sd.solve(sdk2);
    }

    public void solve(int[][] table) {
        // 从 21 * 21中分别获得 9*9
        int[][] mid = getSDK(table, SamuraiSuDuKu.SDKType.MD);
        int[][] tl = getSDK(table, SamuraiSuDuKu.SDKType.TL);
        int[][] tr = getSDK(table, SamuraiSuDuKu.SDKType.TR);
        int[][] bl = getSDK(table, SamuraiSuDuKu.SDKType.BL);
        int[][] br = getSDK(table, SamuraiSuDuKu.SDKType.BR);
        // 穷尽所有可能解，放入tls,trs,bls,brs中 ，mids最后计算
        exhaust(0, 0, tl, tls);
        exhaust(0 , 0 , tr, trs);
        exhaust(0 , 0, bl, bls);
        exhaust(0, 0, br, brs);
//      对上面计算的四个方块的结果进行遍历操作
        for (int i = 0; i < tls.size(); i++) {
//           获得一个新的mid 由于check传入 数组为引用， 如果直接传入mid, mid会被同步修改
            int[][] midf = copyArray(mid);
            tl = tls.get(i);
            if(!check(tl, null, null, null, midf)) continue;
            for (int j = 0; j < trs.size(); j++) {
                midf = copyArray(mid);
                tr = trs.get(j);
                if(!check(tl, tr, null, null, midf)) continue;
                for (int k = 0; k < bls.size(); k++) {
                    midf = copyArray(mid);
                    bl = bls.get(k);
                    if (!check(tl, tr, bl,null, midf)) continue;
                    for (int m = 0; m < brs.size(); m++) {
                        midf = copyArray(mid);
                        br = brs.get(m);
                        if(!check(tl,tr,bl,br,midf)) continue;
// 						在计算了四周之后，再计算当前中间的值，会比较方便
                        exhaust(0,0, midf, mids);
                        for (int g = 0; g < mids.size(); g++) {
                            count ++;
                            System.out.println(" =========== Solution " + count + " ================ ");
                            merge(table, tl, tr, bl, br, mids.get(g));
                            print(table);
                        }
                        // 回置
                        mids = new ArrayList<>();
                    }
                }
            }
        }
    }
    //    check函数  注意  mid 会同步修改
    private boolean check(int[][] tl, int[][] tr, int[][] bl, int[][] br, int[][] mid ){
        if(tl != null) // 将tl中和mid重合的元素复制给mid
            for (int i = 0; i < 3; ++i) for (int j = 0; j < 3; ++j)
                mid[i][j] = tl[i + 6][j + 6];
        if(tr != null)// 将tr中和mid重合的元素复制给mid
            for (int i = 0; i < 3; ++i) for (int j = 0; j < 3; ++j)
                mid[i][j + 6] = tr[i + 6][j];
        if(bl != null)// 将bl中和mid重合的元素复制给mid
            for (int i = 0; i < 3; ++i) for (int j = 0; j < 3; ++j)
                mid[i + 6][j] = bl[i][j + 6];
        if(br != null) // 将br中和mid重合的元素复制给mid
            for(int i = 0; i < 3; i++) for(int j = 0; j < 3; j++)
                mid[i + 6][j + 6] = br[i][j];
//       利用缩影数组判断
        int[] cr = new int[9];
        int[] cl = new int[9];
        for(int i = 0; i < 9; i++){
            for(int j = 0; j < 9; j++){
                int x = mid[i][j];
                int y = mid[j][i];
                if( x > 0){
                    x--;
                    cr[x]++;
                }
                if( y > 0){
                    y--;
                    cl[y]++;
                }
                if(cr[x] > 1 || cl[y] > 1) return false;
            }
//            对所有的数组进行清理工作
            for(int k = 0; k < 9; k++){
                cr[k] = 0;
                cl[k] = 0;
            }
        }
        return true;
    }
    // 填表
    private void merge(int[][] table, int[][] tl, int[][] tr, int[][] bl, int[][] br, int[][] md) {
        for (int i = 0; i < 9; ++i)
            for (int j = 0; j < 9; ++j)
                table[i][j] = tl[i][j];
        for (int i = 0; i < 9; ++i)
            for (int j = 12; j < 21; ++j)
                table[i][j] = tr[i][j - 12];
        for (int i = 12; i < 21; ++i)
            for (int j = 0; j < 9; ++j)
                table[i][j] = bl[i - 12][j];
        for (int i = 12; i < 21; ++i)
            for (int j = 12; j < 21; ++j)
                table[i][j] = br[i - 12][j - 12];
        for (int i = 6; i < 15; ++i)
            for (int j = 6; j < 15; ++j)
                table[i][j] = md[i - 6][j - 6];
    }
    // 打印
    private void print(int[][] array){
        for (int i = 0; i < array.length; i++) {
            for (int j = 0; j < array[0].length; j++) {
                if(array[i][j] != 0) System.out.print(array[i][j] + " ");
                else System.out.print("  ");
            }
            System.out.println();
        }
    }
    // 获取 9 * 9的数独
    private int[][] getSDK(int[][] table, SamuraiSuDuKu.SDKType type) {
        int br = 0, bc = 0; // 定义初始
        switch (type){
            case TR: bc = 12; break;
            case BL: br = 12; break;
            case BR: br = 12; bc = 12; break;
            case MD: br = 6; bc = 6; break;
            default: break;
        }
        int[][] res = new int[9][9];
        for(int i = 0; i < 9; i++)
            for(int j = 0; j < 9; j++){
                res[i][j] = table[br + i][bc + j];
            }
        return res;
    }
    //  从左到右，从上到下进行遍历
    private void next(int x, int y, int[][] sdk, List<int[][]> list) {
        if (y == 8) { // 从左向右遍历时到达最右端时转至下一行
            exhaust(x + 1, 0, sdk, list);
        } else { // 未到达最右端时，转至下一列
            exhaust(x, y + 1, sdk, list);
        }
    }
    // 获得所有可能的解
    public void exhaust(int row, int col, int[][] sdk, List<int[][]> list){
        if( row == 9) { // 已经遍历完成，将结果保存到list
            int[][] tmp = copyArray(sdk);
            list.add(tmp);
            return;
        }
        if(sdk[row][col] != 0){ // 当数独的某个元素不为0时，直接跳过改元素
            next(row ,col, sdk , list);
        } else{ // 当元素为0时
            for(int n = 1; n <= 9; ++n){ // 总共9个数字供填充
                if(fill(row, col, n, sdk)){ // 判断将n填充进来是否满足当前的要求，若满足要求则进行赋值，赋值后直接进入下一次遍历。
                    // 若填充该元素后不符合最终的要求则会将该位置置0，继续寻找合适的值。
                    sdk[row][col] = n;
                    next(row, col, sdk , list);
                    sdk[row][col] = 0;
                }
            }
        }
    }
    // 判断填充是否符合要求
    private boolean fill(int row, int col, int n, int[][] sdk) {
        boolean res = true;
//     遍历一行，看是否有和n相同的元素，有则返回false
        for(int m = 0; m < 9; m++){
            if(sdk[row][m] == n) {
                res = false;
                break;
            }
        }
//      遍历一列，判断是否有和n相同的元素，有则返回false
        for(int m = 0; m < 9; m++){
            if(sdk[m][col] == n){
                res = false;
                break;
            }
        }
//      判断n是否满足将9*9的矩阵切分成9个3*3的小矩阵的要求
//      rs、cs为填充位置在9块中的哪一块
        int rs = row / 3 * 3;
        int cs = col / 3 * 3;
        x:for(int i = rs; i < rs + 3; i++){
            for(int j = cs; j < cs + 3; j++){
                if(sdk[i][j] == n){
                    res = false;
                    break x;
                }
            }
        }
        return res;
    }
    private int[][] copyArray(int[][] arr) {
        int[][] res = new int[arr.length][arr[0].length];
        for (int i = 0; i < arr.length; i++) {
            for (int j = 0; j < arr[i].length; j++) {
                res[i][j] = arr[i][j];
            }
        }
        return res;
    }
}
