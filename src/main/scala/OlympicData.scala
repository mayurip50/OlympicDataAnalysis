import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.SparkSession

object OlympicData {

    PropertyConfigurator.configure("C:/spark-2.4.7-bin-hadoop2.7/spark-2.4.7-bin-hadoop2.7/conf/log4j.properties")

    def main(args:Array[String]): Unit ={

      val spark=SparkSession.builder().master("local").appName("Olympic Data").getOrCreate()

      val df=spark.read.csv("Data/olympic_data.csv")
      // df.show()
      df.createOrReplaceTempView("data")
//Athlete, Age, Country, Year, Closing Ceremony Date, Sport, Gold Medals, Silver Medals, Bronze Medals, Total Medals
    //  1. No of athletes participated in each Olympic event
      spark.sql("select _c3,count(*) from data group by _c3").show()

     // 2. No of medals each country won in each Olympic in ascending order
     spark.sql("select _c2,_c3 ,sum(_c9) as cnt from data group by _c3,_c2 order by _c3,cnt DESC").show(1000)

     // 3. Top 10 athletes who won highest gold medals in all the Olympic events
spark.sql("select _c0 , sum(_c9) as count from data group by _c0 order by count DESC limit 10").show()

      //  4. No of athletes who won gold and whose age is less than 20
      spark.sql("select count(*) as  count from data where (_c6>0) and (_c1<20) ").show()

      //  5. Youngest athlete who won gold in each category of sports in each Olympic
      spark.sql("select * from data where _c1=(select min(_c1) from data where _c1>0) and _c6>0 ").show()

      //   6. No of atheletes from each country who has won a medal in each Olympic in each sports
      spark.sql("select count(*) as cnt,_c2,_c3,_c5 from data where _c9>0 group by _c2,_c3,_c5 order by _c3,cnt DESC ").show()

      //  7. No of athletes won at least a medal in each events in all the Olympics
    spark.sql("select DISTINCT _c5, count(*) as cnt from data where _c9>0 group by _c5 order by cnt DESC").show()


       // 8. Country won highest no of medals in wrestling in 2012
      spark.sql("select _c2 ,sum(_c9) as cnt from data where _c5='Wrestling' and _c3='2012' group by _c2 order by cnt DESC limit 1" ).show()



  }
}
