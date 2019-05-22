package com.aleiye.leetcode;

import java.util.*;

/**
 * @PackageName: com.aleiye.leetcode
 * @ClassName: Solution
 * @Date: 2019/4/30 15:06
 * @Auther: Linxu
 * @Description: TODO
 */
public class Solution {
    /**
     * 861. 翻转矩阵后的得分
     * @param A
     * @return
     */
    public static int matrixScore(int[][] A) {
        int rowLen = A[0].length;   //4
        int colLen = A.length;      //3
        int num = 0;
        for(int i=0;i<colLen;i++){
            if (A[i][0] != 1) {
                for (int j=0;j<rowLen;j++) {
                    A[i][j] = A[i][j] ^ 1;
                }
            }
        }

        for (int[] a:A) {
            for (int aa:a) {
                System.out.print(aa);
            }
            System.out.println();
        }
        System.out.println("-------------------");

        for(int i=0;i<rowLen;i++){
            int a = 0;
            for (int j=0;j<colLen;j++) {
                a += A[j][i];
            }
            if (a * 2 <= colLen) {
                for (int j=0;j<colLen;j++) {
                    A[j][i] = A[j][i] ^ 1;
                }
            }
        }

        for (int[] a:A) {
            for (int aa:a) {
                System.out.print(aa);
            }
            System.out.println();
        }
        System.out.println("-------------------");

        for(int i=0;i<colLen;i++){
            for (int j=0;j<rowLen;j++) {
                num += A[i][j] * Math.pow(2,rowLen-j-1);
                System.out.println(A[i][j] * Math.pow(2,rowLen-j-1));
            }
        }

        return num;
    }

    /**
     * 9. 回文数
     * @param x
     * @return
     */
    public static boolean isPalindrome(int x) {
        String s = String.valueOf(x);
        int left = 0;
        int right = s.length()-1;
//        char[] chars = s.toCharArray();
        while (left < right) {
//            if (chars[left] != chars[right]){
//                return false;
//            }
            if (s.charAt(right) != s.charAt(left)){
                return false;
            }
            left++;
            right--;
        }
        return true;
    }

    /**
     * 892. 三维形体的表面积
     * @param grid
     * @return
     */
    public static int surfaceArea(int[][] grid) {
        int area = 0;
        for(int i = 0; i < grid.length; i++){
            for(int j = 0; j < grid[0].length; j++){
                if(grid[i][j] > 0){
                    area += grid[i][j] * 4 + 2;
                }
                if(i > 0 && grid[i-1][j] != 0){
                    area -= (Math.min(grid[i][j], grid[i-1][j]) * 2);
                }
                if(j > 0 && grid[i][j-1] != 0){
                    area -= (Math.min(grid[i][j], grid[i][j-1]) * 2);
                }
            }
        }
        return area;
    }

    /**
     * 728. 自除数
     * @param left
     * @param right
     * @return
     */
    public static List<Integer> selfDividingNumbers(int left, int right) {
        List<Integer> nums = new ArrayList<Integer>();
        for(int i = left; i <= right; i++){
            if (isSelfDividingNumbers(i)){
                nums.add(i);
            }
        }
        return nums;
    }
    public static boolean isSelfDividingNumbers(int num) {
        int temp=num;
        while(temp > 0){
            if(temp % 10 == 0){
                return false;
            } else if(num % (temp % 10) != 0){
                return false;
            }
            temp /= 10;
        }
        return true;
    }

    /**
     * 438. 找到字符串中所有字母异位词
     * @param s
     * @param p
     * @return
     */
    public static List<Integer> findAnagrams(String s, String p) {
        ArrayList<Integer> num = new ArrayList<Integer>();
        char[] ps = p.toCharArray();
        char[] ss = s.toCharArray();
        A:for (int i = 0; i < s.length() - p.length() + 1; i++) {
            int[] a = new int[26];
            for (int j = 0; j < 26; j++) {
                a[j] = 0;
            }
            for (int j = 0; j < p.length(); j++) {
                int temp = (int)ps[j] - 'a';
                a[temp] += 1;
            }
            B:for(int j = 0; j < p.length(); j++) {
                int temp = (int)ss[i+j] - 'a';
                if(a[temp]-- == 0){
                    continue A;
                }
            }
            num.add(i);
        }
        return num;
    }


    private static long num = 0;
    private static long[][] a;
    /**
     * 935. 骑士拨号器
     * @param N
     * @return
     */
    public static int knightDialer(int N) {
        a = new long[10][N];
        for (int i = 0; i < 10; i++) {
            a[i][0] = 1;
        }
        for (int j = 1; j < N; j++) {
            for (int i = 0; i < 10; i++){
                a[i][j] = next(i,j);
            }
        }
        for (int i = 0; i< 10; i++) {
            num = (num + a[i][N-1]) % 1000000007;
        }

        return (int)num;
    }
    public static long next(int i,int j){
        switch (i) {
            case 0:
                return (a[4][j-1] + a[6][j-1]) % 1000000007;
            case 1:
                return (a[6][j-1] + a[8][j-1]) % 1000000007;
            case 2:
                return (a[7][j-1] + a[9][j-1]) % 1000000007;
            case 3:
                return (a[4][j-1] + a[8][j-1]) % 1000000007;
            case 4:
                return ((a[0][j-1] + a[3][j-1]) % 1000000007 + a[9][j-1]) % 1000000007;
            case 5:
                return 0;
            case 6:
                return ((a[0][j-1] + a[1][j-1]) % 1000000007 + a[7][j-1]) % 1000000007;
            case 7:
                return (a[2][j-1] + a[6][j-1]) % 1000000007;
            case 8:
                return (a[1][j-1] + a[3][j-1]) % 1000000007;
            case 9:
                return (a[2][j-1] + a[4][j-1]) % 1000000007;
            default :
                return 0;
        }
    }

    /**
     * 67. 二进制求和
     * @param a
     * @param b
     * @return
     */
    public static String addBinary(String a, String b) {
        String s = "";
        int al = a.length(), bl = b.length(),n1,n2 = 0;
        int maxl = Math.max(al,bl);
        for (int i = 1; i <= Math.abs((al-bl)); i++) {
            if (al > bl) {
                b = "0" + b;
            } else if (al < bl) {
                a = "0" + a;
            }
        }
        StringBuffer temp = new StringBuffer();
        for (int i = maxl - 1; i >= 0; i--){
            n1 = n2 + a.charAt(i) - '0' + b.charAt(i) -'0';
            n2 = n1 / 2;
            temp.append(n1 % 2);
        }
        if (n2 >= 1) {
            temp.append(n2);
        }
        return temp.reverse().toString();
    }

    /**
     * 870. 优势洗牌
     * @param A
     * @param B
     * @return
     */
    public static int[] advantageCount(int[] A, int[] B) {
        int len = A.length;
        int[] C = B.clone();
        Arrays.sort(A);
        Arrays.sort(C);
        int[] temp = new int[len];
        int i = 0, j = len-1;
        for (int k = 0 ; k < len ; k++) {
            if (A[k] > C[i]) {
                temp[i] = A[k];
                i++;
            } else {
                temp[j] = A[k];
                j--;
            }
        }
        int[] D = new int[len];
        for(int m = 0 ; m < len ; m++){
            for(int n = 0 ; n < len ; n++){
                if(B[m] == C[n]){
                    D[m] = temp[n];
                    C[n] = -1;      //处理重复值
                    break;
                }
            }
        }
        return D;
    }

    /**
     * 811. 子域名访问计数
     * @param cpdomains
     * @return
     */
    public static List<String> subdomainVisits(String[] cpdomains) {
        String temp = "";
        Map<String, Integer> map = new HashMap<String, Integer>();
        List<String> list = new ArrayList<String>();
        for (String cpdomain : cpdomains) {
            Integer time = Integer.valueOf(cpdomain.substring(0,cpdomain.indexOf(" ")));
            String domain = cpdomain.substring(cpdomain.indexOf(" ")+1,cpdomain.length());
            String[] domains = domain.split("\\.");

            for (int i = domains.length - 1; i >= 0; i--) {
                if (i == domains.length - 1) {
                    temp = domains[i];
                } else {
                    temp = domains[i] + "." + temp;
                }
                if(map.get(temp) == null){
                    map.put(temp,time);
                } else {
                    map.put(temp,time+map.get(temp));
                }
            }
        }

        Iterator<Map.Entry<String, Integer>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Integer> e = iterator.next();
            list.add(e.getValue() + " " + e.getKey());
        }
        return list;
    }

    public static void main(String[] args) {
        String[] a = {"900 google.mail.com", "50 yahoo.com", "1 intel.mail.com", "5 wiki.org"};
        System.out.println(subdomainVisits(a));

    }
}
