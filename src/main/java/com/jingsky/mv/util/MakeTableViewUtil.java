package com.jingsky.mv.util;

import com.jingsky.mv.vo.View;
import com.jingsky.mv.vo.ViewCol;
import com.jingsky.mv.vo.ViewLeftJoin;

import java.util.ArrayList;
import java.util.List;

/**
 * 用来生成tableview中json的工具类
 */
public class MakeTableViewUtil {

    public static void main(String[] args){
        //列
        ViewCol viewCol1=new ViewCol("user_name","t_user","name");
        ViewCol viewCol2=new ViewCol("user_info","t_user","info");
        ViewCol viewCol3=new ViewCol("role_name","t_role","name");
        ViewCol viewCol4=new ViewCol("login_time","t_user_login","login_time");
        ViewCol viewCol5=new ViewCol("note","t_user_login","login_note");
        List<ViewCol> viewColList=new ArrayList<>();
        viewColList.add(viewCol1);
        viewColList.add(viewCol2);
        viewColList.add(viewCol3);
        viewColList.add(viewCol4);
        viewColList.add(viewCol5);
        //join
        ViewLeftJoin join1=new ViewLeftJoin("t_user","user_id","id");
        ViewLeftJoin join2=new ViewLeftJoin("t_role","role_id","id");
        List<ViewLeftJoin> viewLeftJoinList=new ArrayList<>();
        viewLeftJoinList.add(join1);
        viewLeftJoinList.add(join2);

        View view=new View();
        view.setMvName("t_login_mv");
        view.setId(1);
        view.setMasterTable("t_user_login");
        view.setMasterTablePk("id");
        view.setMasterWhereSql("login_time>'2021-12-21 00:00:00'");
        view.setViewColList(viewColList);
        view.setViewLeftJoinList(viewLeftJoinList);

        System.out.println(view.makeSourceSql());
        System.out.println(makeInsertTableViewSql(view));
    }

    public static String makeInsertTableViewSql(View view){
//        JSONObject jsonObject=new JSONObject(view);
//        StringBuffer sb=new StringBuffer("insert into t_mv_table_view(id,mv_name,cols_json,master_table,master_table_pk,where_sql,leftJoinJson,create_by,last_update_by) values (");
//        sb.append("'"+view.getId()+"','"+view.getMvName()+"','"+jsonObject.getJSONArray("viewColList")+"','"+view.getMasterTable()+"','"+view.getMasterTablePk()+"',\""+view.getMasterWhereSql()+"\",'"+jsonObject.getJSONArray("viewLeftJoinList")+"','mark','mark')");
//        return sb.toString();
        return null;
    }

}
