package com.jingsky.mv.util;

import com.jingsky.mv.vo.View;
import com.jingsky.mv.vo.ViewCol;
import com.jingsky.mv.vo.ViewLeftJoin;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * 用来生成tableview中json的工具类
 */
public class MakeTableViewUtil {

    public static void main(String[] args){
        //列
        ViewCol viewCol1=new ViewCol("id","t_course_level_course_map","ID");
        ViewCol viewCol2=new ViewCol("course_name","t_course","COURSE_NAME");
        ViewCol viewCol4=new ViewCol("course_id","t_course","ID");
        ViewCol viewCol3=new ViewCol("level_name","t_course_level","LEVEL_NAME");
        List<ViewCol> viewColList=new ArrayList<>();
        viewColList.add(viewCol1);
        viewColList.add(viewCol2);
        viewColList.add(viewCol3);
        viewColList.add(viewCol4);
        //join
        ViewLeftJoin join1=new ViewLeftJoin("t_course","COURSE_ID","ID");
        ViewLeftJoin join2=new ViewLeftJoin("t_course_level","COURSE_LEVEL_ID","ID");
        List<ViewLeftJoin> viewLeftJoinList=new ArrayList<>();
        viewLeftJoinList.add(join1);
        viewLeftJoinList.add(join2);

        View view=new View();
        view.setMvName("t_course_mv");
        view.setId(1);
        view.setMasterTable("t_course_level_course_map");
        view.setMasterTablePk("id");
        view.setMasterWhereSql("1=1");
        view.setViewColList(viewColList);
        view.setViewLeftJoinList(viewLeftJoinList);

        JSONObject jsonObject=new JSONObject(view);
        System.out.println(jsonObject);
        System.out.println(view.makeSourceSql());
    }

}
