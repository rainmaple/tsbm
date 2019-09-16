package cn.edu.ruc.adapter;/**
 * @program: tsbm
 * @description: ${description}
 * @author: rainmaple
 * @date: 2019-09-15 21:06
 **/

import java.sql.SQLOutput;
import java.util.Arrays;
import java.util.List;

/**
 * @ClassName test
 * @Description: TODO
 * @Author rainmaple
 * @Date 2019/9/15 
 * @Version V1.0
 **/
public class test {
    public static void main(String args[]){
        String [] a = new String []{"1","2"};
        List list = Arrays.asList(a);
        ((List) list).clear();
        list.add(3);
        System.out.println(list.toString());
    }
}
