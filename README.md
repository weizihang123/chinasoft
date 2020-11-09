# chinasoft
package app

import app.FunctionDemo1App.sparkContext
import base.BaseApp
import org.apache.spark.rdd.RDD

/**
 * @author weixu
 * @create 2020-08-06 15:16
 */
object Function1Demo2App extends BaseApp{
  override val outputPath: String = "D:\\code\\output"

  def main(args: Array[String]): Unit = {
    runApp{
      val rdd:RDD[String] = sparkContext.textFile("/D:/code/input/user_visit_action.txt")
      val rdd1 = rdd.filter(line=>line.split("_")(5)=="null")
      println("---------一次性封装所有数据-----------")
      val rdd3: RDD[(String, Int, Int, Int)] = rdd1.flatMap(line => {
        val strings = line.split("_")
        if (strings(6) != "-1") {
          List((strings(6), 1, 0, 0))
        } else if (strings(8) != "null") {
          val strings1 = strings(8).split(",")
          for (elem <- strings1) yield (elem, 0, 1, 1)
        } else if (strings(10) != "null") {
          val strings1 = strings(10).split(",")
          for (elem <- strings1) yield (elem, 0, 0, 1)
        } else {
          Nil
        }
      })
      val rdd4 = rdd3.map {
        case (categorys, cc, oc, pc) => (categorys, (cc, oc, pc))
      }
      val result: RDD[(String, (Int, Int, Int))] = rdd4.reduceByKey {
        case ((cc1, oc1, pc1), (cc2, oc2, pc2)) => (cc1 + cc2, oc1 + oc2, pc1 + pc2)
      }
      val tuples: Array[(String, (Int, Int, Int))] = result.sortBy(x=>x._2,false).take(10)
      sparkContext.makeRDD(tuples,1).saveAsTextFile(outputPath)
    }
  }
}

package app

import base.BaseApp
import bean.{AccBean, CategoryAcc}
import org.apache.spark.rdd.RDD

/**
 * @author weixu
 * @create 2020-08-06 15:50
 */
object Function1Demo3App extends BaseApp{
  override val outputPath: String = "D:\\code\\output"

  def main(args: Array[String]): Unit = {
    runApp{
      val acc = new CategoryAcc
      sparkContext.register(acc,"myAcc")
      val rdd: RDD[String] = sparkContext.textFile("/D:/code/input/user_visit_action.txt")
      val rdd1 = rdd.filter(line=>line.split("_")(5)=="null")
       rdd1.foreach(line => {
        val words = line.split("_")
        if (words(6) != "-1") {
          acc.add(words(6),"click")
        } else if (words(8) != "null") {
          val categorys = words(8).split(",")
          for (elem <- categorys) yield acc.add(elem,"order")
        } else if (words(10) != "null") {
          val categorys = words(10).split(",")
          for (elem <- categorys) yield acc.add(elem, "pay")
        }
      })
      val list: List[AccBean] = acc.value.values.toList
      val result = list.sortBy(x=>x).take(10)
      sparkContext.makeRDD(result,1).saveAsTextFile(outputPath)
    }
  }
}


package app

import base.BaseApp
import org.apache.spark.rdd.RDD

/**
 * @author weixu
 * @create 2020-08-07 10:29
 * 原话：Top10热门品类中每个品类的Top10活跃Session点击统计
 * 理解： Top10热门品类中每个品类的点击量Top10的Session
 * 步骤： ①求出Top10热门品类
 * ​      ②过滤Top10热门品类的点击数据
 * ​			③将点击数据按照  category-session分组，求出每个品类下，每个Session的点击总量
 * ​			④按照category分组，在每个category组下，按照点击总量进行降序排序，排序后取前10
 */
object Function2Demo1App extends BaseApp{
  override val outputPath: String = "Function2Demo1App"

  def main(args: Array[String]): Unit = {
    runApp{
      //①求出Top10热门品类
      val rdd: RDD[String] = sparkContext.textFile("output")
      val rdd1: RDD[String] = rdd.map(line=>line.split(",")(0).substring(1))
      val top10categorys: Array[String] = rdd1.collect()
      //制作广播变量
      val bc = sparkContext.broadcast(top10categorys)
      // 过滤Top10热门品类的点击数据
      val rdd2 = sparkContext.textFile("input/user_visit_action.txt")
      val top10cateforys: RDD[(String, String)] = rdd2.map(line => {
        val words = line.split("_")
        (words(6), words(2))
      }).filter(x => bc.value.contains(x._1))

      //将点击数据按照  category-session分组，求出每个品类下，每个Session的点击总量
      val rdd3: RDD[((String, String), Int)] = top10cateforys.map(x=>(x,1)).reduceByKey(_+_)

      //按照category分组
      val rdd4: RDD[(String, Iterable[(String, Int)])] = rdd3.map {
        case ((category, session), sessionCount) => (category, (session, sessionCount))
      }.groupByKey()
      rdd4


      // 在每个category组下，按照点击总量进行降序排序，排序后取前10
      val rdd5: RDD[(String, List[(String, Int)])] = rdd4.mapValues(iter=> iter.toList.sortBy(x=> -x._2).take(10))
      rdd5.coalesce(1).saveAsTextFile(outputPath)
    }
  }

}


package app

import java.text.DecimalFormat

import base.BaseApp
import bean.UserVisitAction
import org.apache.spark.rdd.RDD


/**
 * @author weixu
 * @create 2020-08-07 11:09
 */
object Function3Demo1App extends BaseApp{
  override val outputPath: String = "D:\\code\\output\\Function2Demo1App"

  def main(args: Array[String]): Unit = {
    runApp {
      //统计每一个页面的访问总量！
      val rdd: RDD[UserVisitAction] = getallBeans()
      val rdd1: RDD[(Long, Int)] = rdd.map(bean=>(bean.page_id,1)).reduceByKey(_+_)
      val map = rdd1.collect().toMap
      val bc = sparkContext.broadcast(map)
      println("-----------------------------统计单跳页面有哪些-----------------")

      //将数据，按照用户和sessionId进行分组
      val rdd3: RDD[((Long, String), Iterable[UserVisitAction])] = rdd.groupBy(bean=>(bean.user_id,bean.session_id))

      //将每个用户和每个session按照操作的时间进行升序排序，取排序后的操作的页面ID即可
      val rdd4: RDD[((Long, String), List[UserVisitAction])] = rdd3.mapValues(iter=>iter.toList.sortBy(bean=>bean.action_time))
      val rdd5: RDD[List[Long]] = rdd4.values.map(iter => {
        for (elem <- iter) yield (elem.page_id)
      })
      rdd5

      //取排序后的操作的页面ID即可  (3,1,4,2)


      //两两组合,求单跳的页面
      val rdd6: RDD[(Long, Long)] = rdd5.flatMap(list=>list.zip(list.tail))

      //将所有的单跳页面进行分组，统计个数
      val rdd7: RDD[((Long, Long), Int)] = rdd6.map(x=>(x,1)).reduceByKey(_+_)

      println("-----------------------------统计单跳页面跳转率-----------------")
      val formater = new DecimalFormat(".00%")
      val result: RDD[String] = rdd7.map {
        case ((fromPage, toPage), pv) => fromPage + "-" + toPage + "=" + formater.format(pv.toDouble / bc.value.getOrElse(fromPage, 1))
      }
      result.saveAsTextFile(outputPath)
    }

  }
  def getallBeans():RDD[UserVisitAction]={
    val rdd = sparkContext.textFile("D:/code/input/user_visit_action.txt")
    val rdd1: RDD[UserVisitAction] = rdd.map(line => {
      val words = line.split("_")
      UserVisitAction(
        words(0),
        words(1).toLong,
        words(2),
        words(3).toLong,
        words(4),
        words(5),
        words(6).toLong,
        words(7).toLong,
        words(8),
        words(9),
        words(10),
        words(11),
        words(12).toLong
      )
    })
    rdd1
  }
}


package app

import java.text.DecimalFormat

import app.Function3Demo1App.sparkContext
import base.BaseApp
import bean.UserVisitAction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * @author weixu
 * @create 2020-08-07 19:26
 */
object Function3Demo2App extends BaseApp{
  override val outputPath: String = "output/Function3Demo2App"

  def main(args: Array[String]): Unit = {
   runApp{
     //统计每一个页面的访问总量！
     val rdd: RDD[UserVisitAction] = getallBeans()
     val rd1: RDD[(Long, Int)] = rdd.map(bean=>(bean.page_id,1)).reduceByKey(_+_)
     val map = rd1.collect().toMap
     val bc = sparkContext.broadcast(map)

     //将数据，按照用户和sessionId进行分组
     val rdd1: RDD[((Long, String), Iterable[UserVisitAction])] = rdd.groupBy(x=>(x.user_id,x.session_id))
     //将每个用户和每个session按照操作的时间进行升序排序，取排序后的操作的页面ID即可
     val rdd3: RDD[((Long, String), List[UserVisitAction])] = rdd1.mapValues(x=>x.toList.sortBy(y=>y.action_time))
     //取排序后的操作的页面ID即可  (3,1,4,2)
     val rdd4: RDD[((Long, String), List[Long])] = rdd3.mapValues(x => {
       for (elem <- x) yield elem.page_id
     })

     //两两组合,求单跳的页面
     val rdd5: RDD[List[Long]] = rdd4.values
     val rdd6: RDD[(Long, Long)] = rdd5.flatMap(x=>x.zip(x.tail))
     //将所有的单跳页面进行分组，统计个数
     val rdd7: RDD[((Long, Long), Int)] = rdd6.map(x=>(x,1)).reduceByKey(_+_)
     val formater: DecimalFormat = new DecimalFormat(".00%")
     val rdd8: RDD[String] = rdd7.map {
       case ((fromPage, toPage), pv) => fromPage + "-" + toPage + "=" + formater.format(pv.toDouble / bc.value.getOrElse(fromPage, 1))
     }
     rdd8.saveAsTextFile(outputPath)
   }
  }
  def getallBeans():RDD[UserVisitAction]={
    val rdd = sparkContext.textFile("D:/code/input/user_visit_action.txt")
    val rdd1: RDD[UserVisitAction] = rdd.map(line => {
      val words = line.split("_")
      UserVisitAction(
        words(0),
        words(1).toLong,
        words(2),
        words(3).toLong,
        words(4),
        words(5),
        words(6).toLong,
        words(7).toLong,
        words(8),
        words(9),
        words(10),
        words(11),
        words(12).toLong
      )
    })
    rdd1
  }
}


package app

import base.BaseApp
import org.apache.spark.rdd.RDD

/**
 * @author weixu
 * @create 2020-08-06 14:02
 */
object FunctionDemo1App extends BaseApp{
  override val outputPath: String = "D:\\code\\output"

  def main(args: Array[String]): Unit = {
    runApp{
      val rdd:RDD[String] = sparkContext.textFile("/D:/code/input/user_visit_action.txt")
      val rdd1 = rdd.filter(line=>line.split("_")(5)=="null")

      println("---------------------点击数---------------------")
      val rdd2 = rdd1.flatMap(line => {
        val words = line.split("_")
        if (words(6) != "-1") {
          List((words(6), 1))
        } else {
          Nil
        }
      })
      val clickRDD: RDD[(String, Int)] = rdd2.reduceByKey(_+_)
      println("-----------------下单数-------------------")
      val rdd4 = rdd1.flatMap(line => {
        val words = line.split("_")
        if (words(8) != "null") {
          val categorys = words(8).split(",")
          for (elem <- categorys) yield(elem,1)
        } else {
          Nil
        }
      })
      val orderRDD = rdd4.reduceByKey(_+_)

      println("-----------------支付数-------------------")
      val rdd5 = rdd1.flatMap(line => {
        val words = line.split("_")
        if (words(10) != "null") {
          val strings = words(10).split(",")
          for (elem <- strings) yield (elem,1)
        } else {
          Nil
        }
      })
      val payRDD = rdd5.reduceByKey(_+_)
      println("-----------------关联-------------------")
      val rdd6: RDD[(String, ((Int, Option[Int]), Option[Int]))] = clickRDD.leftOuterJoin(orderRDD).leftOuterJoin(payRDD)
      val rdd7 = rdd6.map {
        case (categoryId, ((clickCount, orderCount), payCount)) => (categoryId, (clickCount, orderCount.getOrElse(0), payCount.getOrElse(0)))
      }
      rdd7
      println("------------------------排序前十----------------")
      val result = rdd7.sortBy(x=>x._2,false).take(10)
      sparkContext.makeRDD(result,1).saveAsTextFile(outputPath)


    }
  }
}
