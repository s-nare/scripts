import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SQLContext, SaveMode}
import org.joda.time.{DateTime, Hours, Interval}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import java.sql.Timestamp
import scala.util.Random
import org.apache.spark.sql.expressions.Window
import sqlContext.implicits._
import org.apache.spark.sql.UserDefinedFunction


import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}
val formatter = DateTimeFormat.forPattern("ddMMyyyy")
val fromX  =today().minusDays(11)
val toX = fromX.plusDays(6)

val myCohort = new Cohort("test")

myCohort.removeCohortFiles()
myCohort.firstEvent = "editor_open"
myCohort.firstCondition = ""
myCohort.secondEvent = "edit_item_open"
myCohort.secondCondition = "item ='effects'"
myCohort.startDate = fromX
myCohort.endDate = toX
myCohort.user_group = "all"
myCohort.platform = "apple"
myCohort.display()







class Cohort(cohortName: String = "test") {
      import sqlContext.implicits._
      import org.joda.time.format.DateTimeFormat
      import org.joda.time.{DateTime, Days}
      val formatter = DateTimeFormat.forPattern("ddMMyyyy")
      case class CustomException(message: String = "", cause: Throwable = null) extends Exception(message, cause);
      // var cohortName = "test"
      var app: String = "com.picsart.studio" ;
      var startDate :DateTime = formatter.parseDateTime("04072018") ;
      var endDate :DateTime = formatter.parseDateTime("10072018") ;
      var firstEvent: String = "editor_open" ;
      var firstCondition: String = "" ;
      var custom_df:org.apache.spark.sql.DataFrame = null;
      var custom_join_type = "inner"
      var secondEvent: String = "edit_item_open" ;
      var secondCondition: String = "item ='effects'" ;
      var platform: String = "apple" ;
      var experiment: Array[String] = Array("", "");
      var experiment_join = true;
      var experiment_start_date = startDate;
      var period = "daily";
      var user_group: String = "all" ;
      var limit = 0;
      
      protected var parentfolder = "/user/";
      protected var user_folder = System.getenv("USER");
      protected var cohortId: String = cohortName ;
      protected var rnd_uf = s"temp_cohort_${Random.alphanumeric take 10 mkString("")}";
      protected var rnd_uf2 = s"temp_cohort_${Random.alphanumeric take 10 mkString("")}";
      protected val all_platforms = Array("android", "apple", "windows");
      protected val mformatter = DateTimeFormat.forPattern("yyyy-MM-dd");
      protected val pformatter = DateTimeFormat.forPattern("yyyy/MM/dd");
      protected val tformatter = DateTimeFormat.forPattern("yyyy/MM/dd hh:mm:ss");
      protected var temp :org.apache.spark.sql.DataFrame = null;
      protected var temp_gr:org.apache.spark.sql.GroupedData = null;
      
      def removeFiles(files: Seq[String]) = { files.foreach { file => fs.delete(new Path(file), true) } }
      def removeIfExists(file: String, files: String*) = {
            def removeFile(f: String) = { if (exist(file)) fs.delete(new Path(f), true) }
            removeFile(file)
            files.foreach { file => removeFile(file) }
      }
      def exist(file: String) = fs.exists(new Path(file))
      def hasColumn(df:DataFrame, col:String):Boolean = Try(df(col)).isSuccess
      
      val fs = FileSystem.get(new Configuration())

      if (cohortName == "") { cohortId = rnd_uf2; }


      if (cohortName == "") {
            println("You did not specify a cohort name. A random string will be generated, and cohort will be deleted after 'display()'. Please specify a cohort name if you want to keep the files for later use.")
                  println(s"Random seed: $rnd_uf")
      } else{
            if (
                  (fs.exists(new Path(parentfolder + user_folder + "/cohorts/JointData " + cohortId + ".parquet"))) &&
                  (fs.exists(new Path(parentfolder + user_folder + "/cohorts/firstDayDataFor " + cohortId + ".parquet")))
            ) { println("Cohort already exists. Be sure that you drop it before use, if you don't remember its parameters") }
      }  

      def drop(): Unit = {
            removeCohortFiles()
      }

      def check() :Boolean = {
            if (cohortId == "") { println("Please, choose a cohort ID. Try to include event and/or experiment names"); return false; }
            if (firstEvent == "") { println("Please, define the First Event"); return false; }
            if (secondEvent == "") { println("Please, define the Second Event"); return false; }
            return true;
      }

      def calculate(do_not_change:Boolean = true): Unit = {
            if ((parentfolder == "/") && do_not_change == true) { println("No user folder selected. Please use 'display()'"); return; }
            if (!check()) {return;}
            if (endDate.compareTo(startDate) < 0 ) { println("ERROR: Start Date is greater than End Date !"); return; }
            val firstEventDays = Days.daysBetween(startDate, endDate).getDays()
            var expName = experiment(1)
            
            var installs = getDevices().where(col("app") === app).withColumnRenamed("install_date", "old_install_date").withColumn("install_date", substring(col("old_install_date"),0,10))
                  .groupBy("device_id")
                  .agg(min(col("install_date")).as("install_date"))
                  .where(col("install_date") >= startDate.toLocalDate().toString() && col("install_date") <= endDate.plusDays(1).minusMillis(1).toLocalDate().toString())
                  .select(col("device_id"), col("install_date"))
                  .distinct()
            var app_install = getEvent(app, "app_install", startDate, endDate)
                  .where(col("platform") === "windows")
                  .select(col("device_id")).distinct()  
            
            var df1 = getEvent(app, firstEvent, startDate, endDate)

            if (firstEvent == "app_open") { df1 = getMobileDevicesAdId(startDate, endDate) }
                  if (Array("new", "existing") contains (user_group)) {
                        var temp_join_type = "inner"
                        if (user_group == "existing") { temp_join_type = "left" }
                        if (platform != "windows") {
                            df1 = df1
                                   .join(installs, installs("device_id") === df1("device_id") && to_date(df1("timestamp")) === col("install_date") , temp_join_type)
                            if (user_group == "existing") { df1 = df1.where(installs("device_id").isNull) }
                            df1 = df1.drop(installs("device_id")).drop(col("install_date")).distinct()
                        } else {
                              df1 = df1
                                    .join(app_install, app_install("device_id") === df1("device_id") && to_date(df1("timestamp")) === app_install("timestamp"),temp_join_type)
                              if (user_group == "existing") { df1 = df1.where(app_install("device_id").isNull) }
                              df1.drop(app_install("device_id")).drop(app_install("timestamp"))
                  }
            }
            
            if (custom_df != null) {
                  val has_date = hasColumn(custom_df, "date")
                  if(has_date) {
                        df1 = df1.join(custom_df, custom_df("device_id") === df1("device_id") && custom_df("date") === to_date(df1("timestamp")), custom_join_type)
                        if(custom_join_type == "left") {df1 = df1.where(custom_df("device_id").isNull)}                      
                        df1 = df1.drop(custom_df("device_id")).drop(custom_df("date"))
                  } else {
                        df1 = df1.join(custom_df, custom_df("device_id") === df1("device_id"), custom_join_type)
                        if(custom_join_type == "left") {df1 = df1.where(custom_df("device_id").isNull)}                      
                        df1 = df1.drop(custom_df("device_id"))
                  }
                  
            }

            if (firstCondition != "") {
                  df1.registerTempTable("Filtering1")
                  df1 = sqlContext.sql("""Select * from Filtering1 where """ + firstCondition)
                  if (df1.count() < 10) { throw new CustomException("Row count for first event is less then 10  after applying First Condition: " + firstCondition) }
            }

            if (platform != "") {
                  if (!(all_platforms.contains(platform))) { throw new CustomException("Platform is neither apple, android or windows") }
                  df1 = df1.where(col("platform").like(platform))
            }

            if (experiment(0) != "") {
                  if (experiment(1) == "") {
                        if (!(Array("y", "Y") contains (readLine("Experiment Variant is Empty. Do you wish to continue (y/n) ?"))))
                        {
                              var answer = readLine("Enter a name for Experiment Variant. Type END to exit method.")
                              if (answer == "END") { return; }
                              else { expName = answer }
                        }
                        else { return; }
                  }
                  
                  if (experiment_join) {
                        df1 = df1.join(
                              getEvent(app, "experiment_participate", experiment_start_date, endDate).
                                    where(col("experiment_id") === experiment(0) && col("variant") === experiment(1)).
                                    groupBy(col("device_id").as("adid")).agg(min(col("timestamp")).as("ttmp")), 
                              col("device_id") === col("adid") && col("timestamp") >= col("ttmp"), "inner").
                        drop(col("adid")).drop(col("ttmp")).distinct()
                  }
                  else {
                        df1.registerTempTable("experimenting1")
                        df1 = df1.select(col("device_id"), col("timestamp"), explode(col("experiments")).as("exp")).
                        where(col("exp.name") === experiment(0) && col("exp.variant") === expName).drop(col("exp.name")).drop(col("exp.variant"))
                  }
                  
                  if (df1.count() < 10) {
                        throw new CustomException("Row count for first event is less then 10  after applying Experiment filter to First Event. Experiment Name: "
                        + experiment(0) + " Experiment Variant: " + expName + ". Double check the parameters")
                  }
            }

            df1.groupBy(col("device_id"), to_date(col("timestamp")).as("day"))
            .agg(min("timestamp").as("min_timestamp"), max("timestamp").as("max_timestamp")).drop("day")
            .write.mode(SaveMode.Overwrite).parquet(parentfolder+user_folder+"/cohorts/tmp/firsttemp_" + cohortId)

            var secondEventsDays = firstEventDays //7
            var df2 = getEvent(app, secondEvent , startDate, endDate)

            if (secondEvent == "app_open") { df2 = getMobileDevicesAdId(startDate, endDate, 1) }

            if (secondCondition != "") {
                  df2.registerTempTable("Filtering1")
                  df2 = sqlContext.sql("""Select * from Filtering1 where """ + secondCondition)
                  if (df2.count() < 10) {throw new CustomException("Row count for second event is less then 10 after applying Second Condition: " + secondCondition)}
            }

            if (platform != "") { df2 = df2.where(col("platform").like(platform)) }

            temp = df2.groupBy(col("device_id"), to_date(col("timestamp")).as("day"))
            .agg(max(col("timestamp")).as("max_timestamp")).drop("day")
            temp.write.mode(SaveMode.Overwrite).parquet(parentfolder+user_folder+"/cohorts/tmp/secondtemp_" + cohortId)

            val firstDf = sqlContext.read.parquet(parentfolder+user_folder+"/cohorts/tmp/firsttemp_" + cohortId).drop(col("max_timestamp"))
            val secondDf = sqlContext.read.parquet(parentfolder+user_folder+"/cohorts/tmp/secondtemp_" + cohortId)
            var limit_1 = limit
            
            var a =
            firstDf
            .join(secondDf,
                  firstDf("device_id") === secondDf("device_id")
                  and col("min_timestamp") < col("max_timestamp"),
            "inner")
            
            period match {
                  case "daily" =>
                        temp_gr = a.groupBy(to_date(col("min_timestamp")).as("first_date"), to_date(col("max_timestamp")).as("second_date"))
                  case "weekly" =>
                        temp_gr = a.groupBy(get_week(col("min_timestamp"), lit(startDate.toLocalDate.toString)).as("first_date"), get_week(col("max_timestamp"), lit(startDate.toLocalDate.toString)).as("second_date"))
                  case other => throw new CustomException("Choose a valid period, one of: 'daily', 'weekly'. 'monthly' is under construction")
            }
            
            a = temp_gr.agg(countDistinct(firstDf("device_id")).as("count")).distinct()
            temp_gr = null

            if (exist(parentfolder+user_folder+"/cohorts/JointData " + cohortId + ".parquet")) 
            {
                  sqlContext.read.parquet(parentfolder+user_folder+"/cohorts/JointData " + cohortId + ".parquet")
                  .write.mode(SaveMode.Overwrite)
                  .parquet(parentfolder+user_folder+"/cohorts/temp.parquet")
                  
                  temp = sqlContext.read.parquet(parentfolder+user_folder+"/cohorts/temp.parquet")
                  
                  temp
                  .join(a.select(col("first_date"), col("second_date")), a("first_date") === temp("first_date") && temp("second_date")=== a("second_date"), "left")
                  .where(a("first_date").isNull).drop(a("first_date")).drop(a("second_date")).unionAll(a)
                  .distinct()
                  .repartition(1)
                  .write.mode(SaveMode.Overwrite).parquet(parentfolder+user_folder+"/cohorts/JointData " + cohortId + ".parquet")
            } else {        
                  a
                  .distinct()
                  .repartition(1)
                  .write.mode(SaveMode.Append).parquet(parentfolder+user_folder+"/cohorts/JointData " + cohortId + ".parquet")
            }

            period match {
                  case "daily" =>
                        temp_gr = firstDf.groupBy(to_date(col("min_timestamp")).as("first_date"))
                  case "weekly" =>
                        temp_gr = firstDf.groupBy(get_week(col("min_timestamp"), lit(startDate.toLocalDate.toString)).as("first_date"))
                  case other => throw new CustomException("Choose a valid period, one of: 'daily', 'weekly'. 'monthly' is under construction")
            }

            var b = temp_gr.agg(countDistinct("device_id").as("count"))
            if (exist(parentfolder+user_folder+"/cohorts/firstDayDataFor " + cohortId + ".parquet")) {
                  sqlContext.read.parquet(parentfolder+user_folder+"/cohorts/firstDayDataFor " + cohortId + ".parquet")
                        .write.mode(SaveMode.Overwrite).parquet(parentfolder+user_folder+"/cohorts/temp.parquet")
                  
                  temp = sqlContext.read.parquet(parentfolder+user_folder+"/cohorts/temp.parquet")

                  temp
                  .join(b.select(col("first_date")), b("first_date") === temp("first_date"), "left")
                  .where(b("first_date").isNull)
                  .drop(b("first_date")).unionAll(b)
                  .distinct().repartition(1)
                  .write.mode(SaveMode.Overwrite).parquet(parentfolder+user_folder+"/cohorts/firstDayDataFor " + cohortId + ".parquet")
            } else {
                  b.distinct()
                  .repartition(1)
                  .write.mode(SaveMode.Append).parquet(parentfolder+user_folder+"/cohorts/firstDayDataFor " + cohortId + ".parquet")
            }

            removeIfExists(parentfolder+user_folder+"/cohorts/temp.parquet")
            removeIfExists(parentfolder+user_folder+"/cohorts/tmp/firsttemp_" + cohortId)
            removeIfExists(parentfolder+user_folder+"/cohorts/tmp/secondtemp_" + cohortId)
      }

      def show(do_not_change:Boolean = true, startDate :DateTime = this.startDate, endDate :DateTime = this.endDate) {
            if ((parentfolder == "/") && do_not_change == true){
                  println("No user folder specified. Cohorts in 'TMP' folder are deleted after each 'display()'")
                  return;
            }
            if (!check()) { return; }
            if(
                  !(exist(parentfolder+user_folder+"/cohorts/firstDayDataFor " + cohortId + ".parquet") ||
                  !(exist(parentfolder+user_folder+"/cohorts/JointData " + cohortId + ".parquet")))
            ) {
                  println("Cohort Files Not found in /user/" + user_folder + "/cohorts for Cohort: " + cohortId + ". Call calculate() first, or use display()")
                  return;
            }

            var pivot = sqlContext.read.parquet(parentfolder+user_folder+"/cohorts/firstDayDataFor " + cohortId + ".parquet").
                  where(col("first_date") >= (mformatter.print(startDate)) && col("first_date") <= mformatter.print(endDate)).
                  withColumn("pos", lit(-1))
            var pivot2 = sqlContext.read.parquet(parentfolder+user_folder+"/cohorts/JointData " + cohortId + ".parquet")
                  .where(col("first_date") >= (mformatter.print(startDate)) && col("first_date") <= mformatter.print(endDate))
                  .withColumn("rwnm",rowNumber().over(Window.partitionBy(col("first_date")).orderBy(col("second_date"))))
                  .withColumn("pos", (col("rwnm")-1))
                  .drop(col("rwnm")).drop("second_date")
            var pivoted = pivot.unionAll(pivot2).groupBy(col("first_date")).pivot("pos").sum("count")
            val cols_array = "first_date"+: pivoted.drop(col("first_date")).columns.map(x => x+"th").toList  
            var renamed = pivoted.toDF(cols_array.toList: _*)
            if (limit != 0 && !(limit > cols_array.size -1)) { 
                  renamed = renamed.select("first_date", renamed.drop("first_date").columns.toList.take(limit+2): _*) 
            }

            val k = renamed.
            withColumnRenamed("first_date", "firstdate").
            withColumnRenamed("-1th","firstday").
            withColumnRenamed("0th","first").
            withColumnRenamed("1th","second").
            withColumnRenamed("2th", "third").
            withColumnRenamed("3th", "forth").
            withColumnRenamed("4th", "fifth").
            withColumnRenamed("5th", "sixth").
            withColumnRenamed("6th", "seventh")
            k.show(500,false)
           publish(k,"retention_N", true)
      }

      def display(deleteAfter:Int = 0) {
            calculate(false)
            println("Calculation succeeded!!!")
            show(false)

            if (parentfolder == "/" || deleteAfter == 1 || cohortName == "") { removeCohortFiles(); }
      }

      def removeCohortFiles() {
            fs.delete(new Path(parentfolder + user_folder + "/cohorts/" + "firstDayDataFor " + cohortId + ".parquet"), true)            
            fs.delete(new Path(parentfolder + user_folder + "/cohorts/" + "JointData " + cohortId + ".parquet"), true)                        
            println("Cohort files deleted !")
      }

      def getMobileDevicesAdId(param_from:DateTime, param_to:DateTime, state:Int = 0): DataFrame = {// For DAU related calculations
            import org.apache.spark.sql.functions.unix_timestamp
            var entity = "mobile_devices_by_ad_id"
            var resultat:org.apache.spark.sql.DataFrame = null;
            
            // If  period > Q32017 (active_date is present), do not loop over each parquet
            if ((param_to.isAfter(DateTime.parse("2017-06-01")) && param_from.isAfter(DateTime.parse("2017-06-01")))) {
                  resultat = getMobileDevicesAdId_o(param_from, param_to, state)
            }
            
            // If period < Q32017 (active_date is not present), loop over each parquet to add active_date column
            else {
                  var from = param_from.withTimeAtStartOfDay().plusDays(1).minusSeconds(1)
                  if (state == 0) { from = param_from.withTimeAtStartOfDay() }
                  var resultat = sqlContext.read.parquet(s"/analytics/events/PARQUET/mobile_devices_by_ad_id/" + pformatter.print(from))
                        .withColumn("timestamp", lit(new Timestamp(from.withTimeAtStartOfDay().plusDays(1).minusSeconds(1).getMillis())))
                        .withColumnRenamed("advertising_id", "device_id")
                        .drop(col("active_date"))
                  val days = Days.daysBetween(param_from.withTimeAtStartOfDay(), param_to.withTimeAtStartOfDay()).getDays()
                  for (i <- 0 to days) {
                        val nextday = sqlContext.read.parquet(s"/analytics/events/PARQUET/"+entity+"/" + pformatter.print(from.plusDays(i)))
                              .withColumn("timestamp", lit(new Timestamp(from.plusDays(i).getMillis())))
                              .withColumnRenamed("advertising_id", "device_id")                        
                              .drop(col("active_date"))
                        resultat = resultat.unionAll(nextday)
                  }
            }
            return resultat.where(col("app") === app);            
      };

      def getMobileDevicesAdId_o(param_from:DateTime, param_to:DateTime, state:Int = 0): DataFrame = {
        var entity = "mobile_devices"
        var coln = "device_id"
        
        // If before DEC 2017, read mobile_devices_by_ad_id parquet and take advertising_id field, else mobile_devices parquet with device_id field
        if (param_to.isBefore(DateTime.parse("2017-12-15")) || param_from.isBefore(DateTime.parse("2017-12-15"))){
            entity = "mobile_devices_by_ad_id"
            coln = "advertising_id"
        }
        
        val days = Days.daysBetween(param_from.withTimeAtStartOfDay(), param_to.withTimeAtStartOfDay()).getDays
        val files = (0 to days).map { d=>
          val dateStr = param_from.plusDays(d).toString(pathFormatter)
          s"/analytics/events/PARQUET/"+entity+s"/$dateStr/"
        }
        
        var ret_ret = sqlContext.read.parquet(files:_*).withColumn("timestamp", col("active_date").cast("timestamp").as("timestamp")).drop(col("active_date")).
        withColumnRenamed(coln, "device_id")
        if (state == 1) {
            ret_ret.withColumn("timestamp", (col("timestamp").cast("long") + 86399).cast("timestamp"))
        }
        
        return ret_ret
      };

      var get_week: UserDefinedFunction = udf[java.sql.Date, java.sql.Date, String] { (the_date, y_date) =>
          val my_date = DateTime.parse(y_date)
          var one_date = DateTime.parse(the_date.toString)
          var two_date = one_date.minusDays(my_date.getDayOfWeek-1)
          var three_date = two_date.minusDays(two_date.getDayOfWeek)
          var returnn = three_date.plusDays(my_date.getDayOfWeek)
          java.sql.Date.valueOf(returnn.toLocalDate.toString)
      }

} 