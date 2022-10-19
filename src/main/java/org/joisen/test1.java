package org.joisen;

import java.util.Random;

/**
 * @author : joisen
 * @date : 21:34 2022/9/27
 */
public class test1 {
    public static void main(String[] args) {
        Random random = new Random();
        Tree root = new Tree();
        Tree node = root;
        int n = 1;
        while(true){
            Tree cur = new Tree(random.nextInt(100));
            node.left = cur;
            node = node.left;
            preorder(root);
            System.out.println(n);
            n++;
        }

    }
    public static void preorder(Tree root){
        if(root != null){
            preorder(root.left);
            preorder(root.right);
        }
    }
}

class Tree{
    int val;
    Tree left;
    Tree right;
    Tree() {}
    Tree(int val) { this.val = val; }
    Tree(int val, Tree left, Tree right) {
        this.val = val;
        this.left = left;
        this.right = right;
    }
}
