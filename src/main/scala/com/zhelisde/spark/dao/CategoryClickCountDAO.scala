package com.zhelisde.spark.dao



import com.zhelisde.spark.domain.CategoryClickCount
import com.zhelisde.spark.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object CategoryClickCountDAO {

    val tableName = "category_clickcount"
    val cf = "info"
    val qualifier = "click_count"

    /**
      * 保存数据, 一批批保存，所以是list
      * @param list
      */
    def save(list:ListBuffer[CategoryClickCount]): Unit ={
        val table = HBaseUtils.getInstance().getTable(tableName)
        for(els <- list){
            table.incrementColumnValue(Bytes.toBytes(els.categoryID), Bytes.toBytes(cf),
                Bytes.toBytes(qualifier), els.clickCount)

        }
    }

    def count(day_category:String) : Long = {
        val table = HBaseUtils.getInstance().getTable(tableName)
        val get = new Get(Bytes.toBytes(day_category))
        val value = table.get(get).getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifier))
        if(value == null){
            0L
        } else {
            Bytes.toLong(value)
        }
    }

    def main(args: Array[String]): Unit = {
        val list = new ListBuffer[CategoryClickCount]
        list.append(CategoryClickCount("20171122_1",300))
        list.append(CategoryClickCount("20171122_2", 600))
        list.append(CategoryClickCount("20171122_3", 1600))
        save(list)
        print(count("20171122_1")+"---"+count("20171122_2")+"---"+count("20171122_3"))
    }

}
