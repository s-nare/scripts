val tox = DateTime.parse("2018-04-01") 
val fromx = DateTime.parse("2018-03-01")

//Cohorts Definition----------------------------------------------------------------------------------------------------------
val ed_done = getEvent("com.picsart.studio","editor_done_click",fromx,tox).
select(
    $"device_id",
    $"platform",
    to_date($"timestamp").as("date")
    ).
    groupBy($"device_id",$"platform").
        agg(
        countDistinct($"date").as("active_days")
        ).
        withColumn("cohort", 
            when($"active_days" >=13, lit("active")).
            when($"active_days" <=3 && $"active_days" >=1, lit("passive")).
            otherwise("-")).
        drop($"active_days").
        filter(!($"cohort"==="-"))
    
val draw_done = getEvent("com.picsart.studio","draw_done",fromx,tox).
filter(($"total_draw_actions" > 0 && $"platform"==="apple") || ($"total_draw_actions" >2 && $"platform"==="android")).
select(
    $"device_id",
    $"platform",
    to_date($"timestamp").as("date")
    ).
    groupBy($"device_id",$"platform").
        agg(
            countDistinct($"date").as("date_count")
            ).
        withColumn("cohort", 
            when($"date_count" >= 10, lit("active")).
            when($"date_count" <= 2 && $"date_count">= 1, lit("passive")).
            otherwise("-")).
        drop($"date_count").
        filter(!($"cohort"==="-"))

//Cohorts Intersections----------------------------------------------------------------------------------------------------------
ed_done.as("a").
    join(draw_done.as("b"), $"a.device_id" === $"b.device_id", "full").
    select(
        when($"a.platform".isNotNull, $"a.platform").otherwise($"b.platform").as("platform"),
        when($"a.device_id".isNotNull, $"a.device_id").otherwise($"b.device_id").as("device_id"),
        when($"a.cohort".isNotNull, $"a.cohort").otherwise(lit("-")).as("ed_cohort"),
        when($"b.cohort".isNotNull, $"b.cohort").otherwise(lit("-")).as("dr_cohort")
    ).
    groupBy($"platform", $"ed_cohort", $"dr_cohort").
    agg(countDistinct($"device_id")).
    orderBy($"platform", $"ed_cohort", $"dr_cohort").
    show(100, false)
    
//----------------------------------------------------------------------------------------------------------
val to = DateTime.parse("2018-04-15") 
val from = DateTime.parse("2018-04-01")
val draw_open = getEvent("com.picsart.studio","draw_open", from, to).
    select($"source",$"platform",$"device_id",to_date($"timestamp").as("date")).as("a").
    join(draw_done.as("b"), 
        $"a.device_id"=== $"b.device_id","inner").
    select($"a.source",$"a.platform",$"a.device_id",$"b.cohort",$"a.date")

draw_open.groupBy($"source",$"platform",$"cohort",$"date").
        agg(
        countDistinct($"device_id").as("openers")).
        groupBy($"source",$"platform",$"cohort").
        agg(avg($"openers")).
        orderBy($"source",$"platform",$"cohort")
    

draw_open.groupBy($"source",$"platform",$"cohort",$"date").
        agg(
        count($"device_id").as("count_devices")).
        groupBy($"source",$"platform",$"cohort").
        agg(avg($"count_devices")).
        orderBy($"source",$"platform",$"cohort")



 ///draw_done//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

val to = DateTime.parse("2018-04-15") 
val from = DateTime.parse("2018-04-01")

val draw_done_2 = getEvent("com.picsart.studio","draw_done", from, to).
filter(($"total_draw_actions" > 0 && $"platform"==="apple") || ($"total_draw_actions" >2 && $"platform"==="android")).
    select($"source",$"platform",$"device_id",to_date($"timestamp").as("date")).as("a").
    join(draw_done.as("b"), 
        $"a.device_id"=== $"b.device_id","inner").
    select($"a.source",$"a.platform",$"a.device_id",$"b.cohort",$"a.date")


draw_done_2.groupBy($"source",$"platform",$"cohort",$"date").
            agg(countDistinct($"device_id").as("done_users")).
            groupBy($"source",$"platform",$"cohort").
            agg(avg($"done_users").as("done_users")).
            orderBy($"source",$"platform",$"cohort")





draw_done_2.groupBy($"source",$"platform",$"cohort",$"date").
            agg(count($"device_id").as("done_events")).
            groupBy($"source",$"platform",$"cohort").
            agg(avg($"done_events").as("done_events")).
            orderBy($"source",$"platform",$"cohort")


 ///editor_done///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////           


val ed_done_2 = getEvent("com.picsart.studio","editor_done_click",from,to).
select($"device_id",to_date($"timestamp").as("date")).
groupBy($"device_id",$"date").agg(count($"device_id").as("ed_count"))



 val draw_opens = getEvent("com.picsart.studio","draw_open",from,to).
        select($"device_id",$"source",to_date($"timestamp").as("date")).as("a").
            join(draw_done.as("b"),$"a.device_id"=== $"b.device_id","inner").
        select($"a.device_id",$"a.date",$"a.source",$"b.cohort",$"b.platform").distinct().as("a").
            join(ed_done_2.as("b"),$"a.device_id"=== $"b.device_id" && $"a.date"=== $"b.date","inner").
        select($"a.device_id",$"a.platform",$"a.cohort",$"a.source",$"a.date",$"b.ed_count").
        groupBy($"platform",$"source",$"cohort",$"date").
        agg(sum($"ed_count").as("ed_count"), countDistinct($"device_id").as("ed_devices")).
        groupBy($"platform",$"source",$"cohort").agg(avg($"ed_count"),avg($"ed_devices")).
                orderBy($"platform",$"source",$"cohort").show(100,false)

///draft/////////////////////////////////////////////////////////////////////


val to = DateTime.parse("2018-04-15") 
val from = DateTime.parse("2018-04-01")

val draw_done_2 = getEvent("com.picsart.studio","draw_done", from, to).
filter($"total_draw_actions" >0 && $"platform"==="android" && $"source"==="draft").
    select($"platform",$"device_id",to_date($"timestamp").as("date")).as("a").
    join(draw_done.as("b"), 
        $"a.device_id"=== $"b.device_id","inner").
    select($"a.platform",$"a.device_id",$"b.cohort",$"a.date")


draw_done_2.groupBy($"platform",$"cohort",$"date").
            agg(countDistinct($"device_id").as("done_users"), count($"device_id").as("done_events")).
            groupBy($"platform",$"cohort").
            agg(avg($"done_users").as("done_users"),avg($"done_events").as("done_events")).
            orderBy($"platform",$"cohort")





draw_done_2.groupBy($"platform",$"cohort",$"date").
            agg(count($"device_id").as("done_events")).
            groupBy($"platform",$"cohort").
            agg(avg($"done_events").as("done_events")).
            orderBy($"platform",$"cohort")



///draw_done super users and editor_done users overlapping///////////////////////////////////////////////////////////////////
val tox = DateTime.parse("2018-04-01") 
val fromx = DateTime.parse("2018-03-01")

val draw_done_SU = getEvent("com.picsart.studio","draw_done",fromx,tox).
    filter(($"total_draw_actions" > 0 && $"platform"==="apple") || ($"total_draw_actions" >2 && $"platform"==="android")).
    select($"device_id",$"platform",to_date($"timestamp").as("date")).
    groupBy($"device_id",$"platform").agg(countDistinct($"date").as("date_count")).
    filter($"date_count" >= 10)
val editor_dones = getEvent("com.picsart.studio","editor_done_click",fromx,tox).
    select($"device_id",to_date($"timestamp").as("date")).
    groupBy($"device_id").agg(countDistinct($"date").as("date_count"))


editor_dones.as("a").
    join(draw_done_SU.as("b"),$"a.device_id"=== $"b.device_id", "right").
    select($"b.device_id".as("draw_devices"), $"b.platform", when($"a.date_count".isNull, 0).otherwise($"a.date_count").as("done_days")).
    groupBy($"done_days",$"platform").agg(countDistinct($"draw_devices").as("draw_SU")).
    orderBy($"platform", $"done_days").
    show(70, false)

//Uploads of Drawing SU, source = "editor" /////////////////////////////////////////////////
val tox = DateTime.parse("2018-04-01") 
val fromx = DateTime.parse("2018-03-01")

val draw_done_SU = getEvent("com.picsart.studio","draw_done",fromx,tox).
    filter($"user_id".isNotNull).
    filter(($"total_draw_actions" > 0 && $"platform"==="apple") || ($"total_draw_actions" >2 && $"platform"==="android")).
    select($"user_id", to_date($"timestamp").as("date")).
    groupBy($"user_id").agg(countDistinct($"date").as("date_count")).
    filter($"date_count" >= 10).
    select($"user_id")
val photo_upload = getEvent("com.picsart.studio","photo_upload",fromx,tox).
    filter($"user_id".isNotNull && ($"photo_id" !== 0) && ($"photo_id" !== "0") && $"photo_id".isNotNull && $"editor_sid".isNotNull).
    select($"user_id",$"editor_sid",$"photo_id", $"platform").distinct()

val ed_done = getEvent("com.picsart.studio","editor_done_click", fromx,tox).
    filter($"user_id".isNotNull && $"editor_sid".isNotNull).
    select($"user_id", $"editor_sid").distinct().as("a").
        join(draw_done_SU.as("b"),$"a.user_id"=== $"b.user_id","inner").
        select($"a.user_id",$"a.editor_sid").distinct()

val dd = getEvent("com.picsart.studio","draw_done",fromx,tox).
    filter($"user_id".isNotNull && $"editor_sid".isNotNull).
    filter($"total_draw_actions" > 20).
    filter($"source"==="editor").
    select($"user_id", $"editor_sid").as("a").
        join(ed_done.as("b"), $"a.editor_sid"=== $"b.editor_sid", "inner").
        select($"a.user_id",$"a.editor_sid").as("a").
            join(photo_upload.as("b"),$"a.editor_sid"=== $"b.editor_sid","inner").
            select($"a.user_id",$"b.photo_id",$"b.platform")
dd.sample(false, 0.01).show(2000, false)
show(2000, false)
//Uploads of Drawing SU, General, actions > 100 /////////////////////////////////////////////////
val pu = getEvent("com.picsart.studio","photo_upload",fromx,tox).
    filter($"user_id".isNotNull && ($"photo_id" !== 0) && ($"photo_id" !== "0") && $"photo_id".isNotNull && $"editor_sid".isNotNull).
    filter($"total_draw_actions" > 100).
    select($"user_id", $"photo_id", $"platform").as("a").
        join(dd.select($"photo_id").as("b"), $"a.photo_id" === $"b.photo_id", "left").
        filter($"b.photo_id".isNull).drop($"b.photo_id")
pu.count()
pu.sample(false, 0.015).show(3000, false)









