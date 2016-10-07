package com.datareply.analyticsusecase

/*import org.apache.log4j.Logger*/
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._



object app extends Serializable {
  def main(args:Array[String]) = {
    val name = "analyticsusecase"
    val conf = new SparkConf().setAppName(name)
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._

    val cleaned_data = sqlContext.sql("select dossier,nota_dossier,nazione_evento from edh_testcase.l1_testcase")
    val note = cleaned_data.select("dossier","nota_dossier","nazione_evento").repartition(128).cache()
    //note.take(1)
    val infortuni_file = "/data/dizionari/infortuni_gdn.txt"
    val gravidanza_file = "/data/dizionari/gravidanza.txt"
    val interventi_file = "/data/dizionari/interventi_gdn.txt"
    val malattie_file = "/data/dizionari/malattie_gdn.txt"
    val sintomi_file = "/data/dizionari/sintomi_gdn.txt"
    val nations_file = "/data/dizionari/nationsList.txt"
    val nationsISO_file = "/data/dizionari/nationsISOmap.txt"


    val df_nations = GetNations(sc,sqlContext,nations_file,nationsISO_file,note,"nat")
    val df_infortuni = topicDiscovery(sc,sqlContext,
        infortuni_file,note,"inf")
    val df_gravidanza = topicDiscovery(sc,sqlContext,
        gravidanza_file,note,"gra")
    val df_interventi = topicDiscovery(sc,sqlContext,
        interventi_file,note,"int")
    val df_malattie = topicDiscovery(sc,sqlContext,
        malattie_file,note,"mal")
    val df_sintomi = topicDiscovery(sc,sqlContext,
        sintomi_file,note,"sin")


    df_nations.write.format("orc").mode("overwrite").saveAsTable("al_testcase.final_results_nations")
    df_infortuni.write.format("orc").mode("overwrite").saveAsTable("al_testcase.final_results_infortuni")
    df_gravidanza.write.format("orc").mode("overwrite").saveAsTable("al_testcase.final_results_gravidanza")
    df_interventi.write.format("orc").mode("overwrite").saveAsTable("al_testcase.final_results_interventi")
    df_malattie.write.format("orc").mode("overwrite").saveAsTable("al_testcase.final_results_malattie")
    df_sintomi.write.format("orc").mode("overwrite").saveAsTable("al_testcase.final_results_sintomi")

    sc.stop()

 }

def topicDiscovery (sc:org.apache.spark.SparkContext,
                    sqlContext:org.apache.spark.sql.SQLContext,
                    inputfile:String,
                    note: org.apache.spark.sql.DataFrame,
                    dossier_class:String) : org.apache.spark.sql.DataFrame = {

    import sqlContext.implicits._

    val raw_data = sc.textFile(inputfile)
                        .map( line =>
                            {
                            val inf_name = line.split("\\|")(0).toLowerCase()
                            val inf_sin = line.split("\\|").toList
                            (inf_name,inf_sin)
                            }
                            )
                        .flatMap {case(inf_name,inf_sin) =>
                                inf_sin.map(elem => (dossier_class,inf_name,elem.trim.toLowerCase()) )
                        }.filter(record => record._2.trim != "")
                        .toDF("dossier_class", "name","sin")


    val df_fact = note.join(raw_data,$"nota_dossier".contains($"sin"))
                  //.repartition(200)

        df_fact.select("dossier","nota_dossier","dossier_class","name","sin")
            .map(line => {
            val count_match = (" " + line(4).toString + " ").r.findAllIn(line(1).toString.toLowerCase()).length
            val flag_prior = if(List("infortunio","ferita","frattura").contains(line(3).toString.trim)) 1 else 2
            (line(0).toString.toLowerCase(),
             line(2).toString.toLowerCase(),
             line(3).toString.toLowerCase(),
             line(4).toString.toLowerCase(),
             count_match.toInt,
             flag_prior)
            }
            )
            .filter(x=> x._5 > 0)
            .toDF("dossier","dossier_class","name","sin","count_match","flag_prior")
            .groupBy("dossier","dossier_class","name","flag_prior")
            .agg(sum("count_match").alias("count_match"))
        .select("dossier","dossier_class","name","flag_prior","count_match")
        .withColumn("row_num", row_number.over(Window.partitionBy("dossier").orderBy(desc("flag_prior"),desc("count_match"))))
        .map{ line =>
            val top1 = line(5) match {
                case x if x == 1 => (line(2).toString, line(4).toString)
                case x if x != 1 => ("".toString, "")
            }
            val top2 = line(5) match {
                case x if x == 2 => (line(2).toString, line(4).toString)
                case x if x != 2 => ("".toString, "")
            }
            val top3 = line(5) match {
                case x if x == 3 => (line(2).toString, line(4).toString)
                case x if x != 3 => ("".toString, "")
            }
            ((line(0), line(1)),(top1,top2,top3))
        }
        .reduceByKey( (x,y) => ((x._1._1 + y._1._1,
                                x._1._2 + y._1._2),
                                (x._2._1 + y._2._1,
                                x._2._2 + y._2._2),
                                (x._3._1 + y._3._1,
                                x._3._2 + y._3._2)))
        .map(x => (x._1._1.toString,x._1._2.toString,
         x._2._1._1,x._2._1._2, x._2._2._1,x._2._2._2,x._2._3._1,x._2._3._2))
        .toDF("dossier","dossier_class","top1","count_top1","top2","count_top2","top3","count_top3")

}

def GetNations (sc:org.apache.spark.SparkContext,
                sqlContext:org.apache.spark.sql.SQLContext,
                nations_file:String,
                nationsISO_file:String,
                note:org.apache.spark.sql.DataFrame,
                dossier_class:String) : org.apache.spark.sql.DataFrame = {

    import sqlContext.implicits._

    val raw_nations = sc.textFile(nations_file)
                            .map( line =>
                                {
                                val nat_name = line.split("\\|")(0).toLowerCase()
                                val nat_sin = line.split("\\|").toList
                                (nat_name,nat_sin)
                                }
                                )
                            .flatMap {case(nat_name,nat_sin) =>
                                    nat_sin.map(elem => (dossier_class,nat_name,elem.trim.toLowerCase()) )
                            }.filter(record => record._2.trim != "")
                            .toDF("dossier_class", "nat_name","nat_sin")

    val raw_nationsISO = sc.textFile(nationsISO_file)
                            .filter(line => line.trim != "")
                            .filter(line => line.split("\\|").length != 1)
                            .map( line =>
                                {
                                val natISO_name = line.split("\\|")(0).toLowerCase().trim
                                val natISO_sin = line.split("\\|")(1).toLowerCase().trim
                                (natISO_name,natISO_sin)
                                }
                                )
                            .toDF("natISO_name","natISO_cod")

    val df_fact2 = note.join(raw_nations, $"nota_dossier".contains($"nat_sin"))

    val step1b = df_fact2.select("dossier","nota_dossier","dossier_class","nat_name","nat_sin","nazione_evento")
        //.repartition(200)
    val step1a = step1b
        .map(line => {
        val count_match = (" " + line(4).toString + " ").r.findAllIn(line(1).toString.toLowerCase()).length
        val nation = count_match match {
            case x if x == 0 => "Unknown"
            case x if x != 0 => line(3).toString.toLowerCase()
        }
        ((line(0).toString.toLowerCase(),
         line(5).toString.toLowerCase(),
         nation,
         line(2).toString.toLowerCase()),
         (
         count_match.toInt
         ))
    })
    val step1 = step1a
    .reduceByKey((x,y) => x + y)
    .map(x => (x._1._1,x._1._2,x._1._3,x._1._4,x._2))
        .toDF("dossier","cod_iso","nat_name","dossier_class","count")
    .withColumn("row_num", row_number.over(Window.partitionBy("dossier").orderBy(desc("count"))))
    .filter($"row_num" < 2)

    val step2 = step1
    .join(raw_nationsISO, raw_nationsISO("natISO_name") === step1("nat_name"), "left")
    .na.fill("",Seq("natISO_cod","natISO_name"))
    .select("dossier","cod_iso","natISO_cod","natISO_name","dossier_class")
    .map( record => {
        val combined_nation = if(( record(1) == "nd" || record(1) == "it")
        && (record(2) == "Unknown" || record(2) == "it")) "it"
        else if ((record(2) == "Unknown" && record(1) == "ee")) "ee"
        else if ((record(1) != "nd" && record(1) != "it" && record(1) != "ee" )) record(1)
        else record(2)
        (record(0).toString,
        record(4).toString,
        record(3).toString,
        record(1).toString,
        combined_nation.toString)
        }
        ).toDF("dossier","dossier_class","nat_found","natiso_source","combined_nation")

        step2
}

}
