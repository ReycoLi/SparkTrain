package com.zhelisde.spark.dao

import com.zhelisde.spark.domain.CategorySearchClickCount
import com.zhelisde.spark.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer
/**
  * DAO for reference on each category
  */
object CategorySearchClickCountDAO {

    val tableName = "category_search_count"
    val cf = "info"
    val qualifier = "click_count"

    /**
      * 保存数据, 一批批保存，所以是list
      *
      * @param list
      */
    def save(list: ListBuffer[CategorySearchClickCount]): Unit = {
        val table = HBaseUtils.getInstance().getTable(tableName)
        for (els <- list) {
            table.incrementColumnValue(Bytes.toBytes(els.day_search_category), Bytes.toBytes(cf),
                Bytes.toBytes(qualifier), els.clickCount)

        }
    }

    /**
      * 根据row key查询
      * @param day_category
      * @return
      */
    def count(day_category: String): Long = {
        val table = HBaseUtils.getInstance().getTable(tableName)
        val get = new Get(Bytes.toBytes(day_category))
        val value = table.get(get).getValue(Bytes.toBytes(cf), Bytes.toBytes(qualifier))
        if (value == null) {
            0L
        } else {
            Bytes.toLong(value)
        }
    }

    def main(args: Array[String]): Unit = {
        val list = new ListBuffer[CategorySearchClickCount]
        list.append(CategorySearchClickCount("20171122_1_2", 300))
        list.append(CategorySearchClickCount("20171122_2_3", 100))
        list.append(CategorySearchClickCount("20171122_1_4", 1600))
        save(list)
        print(count("20171122_1_2") + "---" + count("20171122_2_3") + "---" + count("20171122_1_4"))
    }

}
