
:load /home/kpi/bi-automation/scripts/grno_script.scala

def erase_table(table_name1 : String, condition : String = ""/*, slack_user : String = ""*/) {
   var JDBC_DRIVER = "org.postgresql.Driver";  
   var DB_URL = "jdbc:postgresql://172.16.33.44:5432/dwh";
   var USER = "dwh";
   var PASS = "4F51hnXVMZoDcHrLvf";
   var conn:java.sql.Connection = null;
   var stmt:java.sql.Statement = null;
   conn = java.sql.DriverManager.getConnection(DB_URL, USER, PASS);
   stmt = conn.createStatement();
   var sql:String = s"DELETE FROM $table_name1" ;
   if (condition != "") { sql = sql+ " WHERE " + condition}
   stmt.executeUpdate(sql);
   stmt.close()
   /*if (slack_user != "") { slackMessage("EXECUTED QUERY: " + sql, slack_user) }
   println("EXECUTED QUERY: " + sql)*/
}


def premium_effect (dd : DateTime, method: String) : Unit ={ 

    val version_ios = "(platform = 'apple' AND LENGTH(version) = 3 AND version >= '680')"
    val version_android = "(platform = 'android' AND LENGTH(version) = 9 AND version >= '993000391')"
    val table_name1 = "editor_effects_premium"

    // 1---------------------------Effect Try to Apply Ratio by Effect Name (Distinct Editor Sessions)
    val ef_apply = getEvent("com.picsart.studio", "effect_apply", dd, dd).filter(version_ios + " OR " + version_android).
        filter($"is_premium" === true && $"is_subscribed" === true).
        select($"editor_sid", $"effect_name")
    val ef_try = getEvent("com.picsart.studio", "effect_try", dd, dd).filter(version_ios + " OR " + version_android).
        select($"platform", $"editor_sid", $"effect_name", $"timestamp", $"is_premium", $"is_subscribed")
    val try_apply = ef_try.
        filter($"is_premium" === true && $"is_subscribed" === true).
        select($"platform", $"editor_sid", $"effect_name", to_date($"timestamp").as("dd")).
        as("c").
            join(ef_apply.as("d"), $"c.editor_sid" === $"d.editor_sid" && $"c.effect_name" === $"d.effect_name", "left").
            select($"c.dd", $"c.platform", $"c.effect_name", $"c.editor_sid".as("try_sid"), $"d.editor_sid".as("apply_sid")).
            groupBy($"dd", $"platform", $"effect_name").
            agg(countDistinct($"apply_sid").as("applies"), (countDistinct($"apply_sid") * 1.0/countDistinct($"try_sid")).as("try_apply"))

    // 2---------------------------Joining Subscription Done with Popup from Effects
    val popup = getEvent("com.picsart.studio","popup_open", dd, dd).filter(version_ios + " OR " + version_android).
        filter($"id".contains ("subscription")).
        filter($"source" === "effects" || $"source" === "editor_effect_apply").
        select($"tip_sid", $"source", $"source_sid", ($"timestamp").as("date_pop"), $"platform").distinct()
    var done = getEvent("com.picsart.studio", "subscription_done", dd, dd).filter(version_ios + " OR " + version_android).
        select($"user_id", ($"timestamp"). as ("date_done"), $"version", $"source", $"source_sid", $"platform").distinct()
    var pop_done = popup.as("a").join(done.as("b"), $"a.tip_sid" === $"b.source_sid" && "a.date_pop" <= "b.date_done", "inner").
        select($"a.source_sid".as("sid"), $"date_done".as("t"), $"a.platform").//distinct().
        withColumn("effect_name", lit("xxx")).
        withColumn("event", lit("done")).
        select($"sid", $"t", $"platform", $"effect_name", $"event")

    // 3---------------------------Subscriptions from Effects, by Effect Name
    val p1 = ef_try.
        filter($"is_premium" === true && $"is_subscribed" === false).
        select($"editor_sid".as("sid"), $"timestamp".as("t"), $"effect_name", $"platform").
        withColumn("event", lit("effect")).
        select($"sid", $"t", $"platform", $"effect_name", $"event").
        unionAll(pop_done).
            select($"sid", $"t", $"platform", $"effect_name", $"event").
            withColumn("next", lead("event", 1).over(Window.partitionBy($"sid").orderBy($"t"))).
            filter($"event" === "effect" && $"next" === "done").
            groupBy($"platform", to_date($"t").as("dd"), $"effect_name").agg(countDistinct($"sid").as("value")).
            withColumn("parameter", lit("Subscribers")).
            select($"dd", $"platform", $"effect_name", $"parameter", $"value")
    val p2 = try_apply.select($"dd", $"platform", $"effect_name", lit("Apply Sessions").as("parameter"), $"applies".as("value")).filter(($"value" !== 0) && ($"value".isNotNull))
    val p3 = try_apply.select($"dd", $"platform", $"effect_name", lit("Try to Apply ratio").as("parameter"), round($"try_apply", 4).as("value")).filter(($"value" !== 0) && ($"value".isNotNull))

    if (method == "show"){
        p1.orderBy($"dd", $"platform", $"effect_name").show(100, false)
        p2.orderBy($"dd", $"platform", $"effect_name").show(100, false)
        p3.orderBy($"dd", $"platform", $"effect_name").show(100, false)    
    } else if (method == "publish") {
        erase_table(table_name1, "dd = '" + dd.toLocalDate().toString + "'" /*+ "' AND scope <> 'Overall'","hakob"*/)
        publish(p1, table_name1, true)
        publish(p2, table_name1, true)
        publish(p3, table_name1, true)    
    }
}


val dd = today().minusDays(4)

premium_effect(dd, "publish" )

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def erase_table(table_name2 : String, condition : String = ""/*, slack_user : String = ""*/) {
   var JDBC_DRIVER = "org.postgresql.Driver";  
   var DB_URL = "jdbc:postgresql://172.16.33.44:5432/dwh";
   var USER = "dwh";
   var PASS = "4F51hnXVMZoDcHrLvf";
   var conn:java.sql.Connection = null;
   var stmt:java.sql.Statement = null;
   conn = java.sql.DriverManager.getConnection(DB_URL, USER, PASS);
   stmt = conn.createStatement();
   var sql:String = s"DELETE FROM $table_name2" ;
   if (condition != "") { sql = sql+ " WHERE " + condition}
   stmt.executeUpdate(sql);
   stmt.close()
   /*if (slack_user != "") { slackMessage("EXECUTED QUERY: " + sql, slack_user) }
   println("EXECUTED QUERY: " + sql)*/
}

def effect_overall(dd :DateTime, method : String) : Unit={

    val platforms = "(platform IN ('android', 'apple'))"
    val table_name2 = "editor_effects_overall"
    val method = "show"

    // 1------------------------------------Daily State of Subscribed Users
    var formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    var dates_seq : Seq[(String, String)] = Seq()
    for (f<- 0 to Days.daysBetween(ff, dd).getDays()) {
        val b = (ff.plusDays(f).toString(formatter), "A")
        dates_seq = dates_seq :+ b
    }
    val dates = dates_seq.toDF("dd", "A").select($"dd")
    val ord = sqlContext.read.parquet("/analytics/entities/orders")
    val users = sqlContext.read.parquet("/analytics/entities/users_orders").
        select($"device_id",$"user_id", $"order_id"). 
        filter($"device_id". isNotNull).
        distinct()
    val ord_users = ord.as ("a").join(users. as ("b"), $"a._id" === $"b.order_id", "inner").
        select($"_id",$"device_id", explode($"renewals").as("rn")).
        select($"_id", $"device_id", to_date($"rn.purchaseDate").as("rn_purchase"), $"rn.expireDate".as("rn_expire"))
    val dates_data = dates.as("a").join(ord_users.as("b"), $"a.dd" >= $"rn_purchase" && $"a.dd" <= $"rn_expire", "left").
        select($"a.dd", $"b.device_id").distinct()

    // 2------------------------------------Events
    val ef_apply_stick = getEvent("com.picsart.studio", "effect_apply", ff, dd).filter(platforms).
        select($"platform", $"device_id", $"editor_sid", to_date($"timestamp").as("date")).as("a").
            join(dates_data.as("b"), $"a.device_id" === $"b.device_id" && $"a.date" === $"b.dd", "left").
            withColumn("is_subscribed", when($"b.device_id".isNull, lit("Non Subscribed")).when($"b.device_id".isNotNull, lit("Subscribed"))).
            select($"a.platform", $"a.device_id", $"a.editor_sid", $"a.date", $"is_subscribed")
    val ef_apply = getEvent("com.picsart.studio", "effect_apply", dd, dd).filter(platforms).
        select($"platform", $"device_id", $"editor_sid", to_date($"timestamp").as("date")).as("a").
            join(dates_data.as("b"), $"a.device_id" === $"b.device_id" && $"a.date" === $"b.dd", "left").
            withColumn("is_subscribed", when($"b.device_id".isNull, lit("Non Subscribed")).when($"b.device_id".isNotNull, lit("Subscribed"))).
            select($"a.platform", $"a.device_id", $"a.editor_sid", $"a.date", $"is_subscribed")
    val ef_try = getEvent("com.picsart.studio", "effect_try", dd, dd).filter(platforms).
        select($"platform",$"device_id", $"editor_sid", to_date($"timestamp").as("date")).as("a").
            join(dates_data.as("b"), $"a.device_id" === $"b.device_id" && $"a.date" === $"b.dd", "left").
            withColumn("is_subscribed", when($"b.device_id".isNull, lit("Non Subscribed")).when($"b.device_id".isNotNull, lit("Subscribed"))).
            select($"a.platform", $"a.device_id", $"a.editor_sid", $"a.date", $"is_subscribed")

    // 3------------------------------------Stickiness
    val dau = ef_apply_stick.
        groupBy($"date", $"platform", $"is_subscribed").agg(countDistinct($"device_id").as("dau")).
        groupBy($"platform", $"is_subscribed").agg(avg($"dau").as("dau"))
    val wau = ef_apply_stick.
        groupBy($"platform", $"is_subscribed").agg(countDistinct($"device_id").as("wau"))
    val k1 = dau.as("a").
        join(wau.as("b"), $"a.platform" === $"b.platform" && $"a.is_subscribed" === $"b.is_subscribed", "inner").
        select($"a.platform", $"a.is_subscribed", round(($"a.dau" / $"b.wau"), 4).as("value")).
        withColumn("parameter", lit("Stickiness")).
        withColumn("dd", lit(tt)).
        select($"dd", $"platform", $"parameter", $"is_subscribed", $"value")

    // 4------------------------------------Try/Apply
    val k2 = ef_try.as("a").
        join(ef_apply.as("b"), $"a.editor_sid" === $"b.editor_sid" && $"a.is_subscribed" === $"b.is_subscribed", "left").
        select($"a.date", $"a.platform", $"a.is_subscribed", $"a.editor_sid".as("tries"), $"b.editor_sid".as("applies")).
        groupBy($"date", $"platform", $"is_subscribed").agg((countDistinct($"applies")*1.0/countDistinct($"tries")).as("value")).
        withColumn("parameter", lit("Try to Apply ratio")).
        select($"date".cast("string").as("dd"), $"platform", $"parameter", $"is_subscribed", round($"value", 4).as("value"))

    if (method == "show"){
        k1.orderBy($"dd", $"platform", $"is_subscribed").show(100, false)
        k2.orderBy($"dd", $"platform", $"is_subscribed").show(100, false)
    } else if (method == "publish") {
        erase_table(table_name2, "dd = '" + dd.toLocalDate().toString + "'")
        publish(k1, table_name2, true)
        publish(k2, table_name2, true)
    }
}


var dd = today().minusDays(4)
val tt = dd.toString().slice(0,10)
val ff = dd.minusDays(6)

effect_overall(dd, "publish")
















