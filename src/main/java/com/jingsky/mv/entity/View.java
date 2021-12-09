package com.jingsky.mv.entity;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 视图类,非数据表
 */
@Data
@NoArgsConstructor
public class View {
    private Integer id;
    //视图表名
    private String mvName;
    //目标列名
    private List<ViewCol> viewColList;
    //主表名
    private String masterTable;
    //主表主键ID
    private String masterTablePk;
    //主表where条件
    private String masterWhereSql;
    //left join的所有表
    private List<ViewLeftJoin> viewLeftJoinList;
    private String sql;
    private long current=System.currentTimeMillis();

    public View(TableView tableView){
        this.id=tableView.getId();
        this.mvName=tableView.getMvName();
        this.masterTable=tableView.getMasterTable();
        this.masterTablePk=tableView.getMasterTablePk();
        this.masterWhereSql=tableView.getWhereSql();

        this.viewColList =JSON.parseArray(tableView.getColsJson(), ViewCol.class);
        this.viewLeftJoinList=JSON.parseArray(tableView.getColsJson(),ViewLeftJoin.class);
    }

    public String toSql(){
        if(sql!=null){
            return sql;
        }
        StringBuffer sb=new StringBuffer("select \n");
        //拼接列
        for(ViewCol col : this.viewColList){
            sb.append("    "+col.getSourceTable()+"."+col.getSourceCol()+" as "+col.getCol()+",\n");
        }
        //去掉最后一个多余的,
        sb=new StringBuffer(sb.substring(0,sb.length()-2)+"\n");
        //拼接主表和where
        sb.append("from "+this.masterTable);
        sb.append("\n");
        //拼接left join
        String leftTable=this.masterTable;
        for(ViewLeftJoin leftJoin : this.viewLeftJoinList){
            sb.append("left join "+leftJoin.getTable());
            sb.append(" on "+leftTable+"."+leftJoin.getJoinLeftCol()+"="+leftJoin.getTable()+"."+leftJoin.getJoinCol());
            sb.append("\n");
            leftTable=leftJoin.getTable();
        }
        sb.append("where "+current+"="+current+" and (" + this.masterWhereSql+")\n");
        sb.append("order by "+this.masterTable+"."+this.masterTablePk);
        sql=sb.toString();
        return sql;
    }

    /**
     * 追加ID条件至主表的where条件，但不改变view
     * @param id
     * @return String
     */
    public String tmpAddIdToMasterWhere(String id){
        return toSql().replaceAll(current+"="+current,"id='"+id+"'");
    }
}
