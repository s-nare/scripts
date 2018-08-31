
val to = DateTime.parse("2018-04-07")
val from = DateTime.parse("2018-03-13")
val exp = getEvent("com.picsart.studio","experiment_participate",from,to).
    filter($"platform"==="android" && $"experiment_id"==="e111").
    select($"device_id", $"variant").distinct()


val toX = DateTime.parse("2018-03-13")
val fromX = DateTime.parse("2018-02-13")
val done  = getEvent("com.picsart.studio","editor_done_click",fromX,toX).
    filter($"platform"==="android").
    select($"device_id",to_date($"timestamp").as("date")).
    groupBy($"device_id").agg(countDistinct($"date").as("active_days")).
    withColumn("cohort",
        when($"active_days" >= 13, lit("active")).
        when($"active_days" <=3 && $"active_days" >=1, lit("passive")).
        otherwise(lit("-"))
    ).
    drop($"active_days").
    filter($"cohort" === "active" || $"cohort" === "passive")

var users = exp.as("a").
    join(done.as("b"), $"a.device_id" === $"b.device_id", "inner").
    drop($"b.device_id")


exp.groupBy($"variant").agg(countDistinct($"device_id").as("devices")).orderBy($"variant").show(false)
users.groupBy($"variant").pivot("cohort").agg(countDistinct($"device_id").as("devices")).orderBy($"variant").show(false)
getEvent("com.picsart.studio","experiment_participate",from,to).
    filter($"platform"==="android" && $"experiment_id"==="e111").
    select($"device_id", to_date($"timestamp").as("date"), $"variant").
    groupBy($"date").pivot("variant").agg(countDistinct($"device_id")).show(50,false)

//------------------------------------------------------------------------------------------------------------------------------------local
val to = DateTime.parse("2018-04-07")
val from = DateTime.parse("2018-03-24")

val open = getEvent("com.picsart.studio","edit_item_open", from, to).
    filter($"item"==="quick_brush").
    select($"device_id",to_date($"timestamp").as("date")).as("a").
    join(users.as("b"), $"a.device_id" === $"b.device_id", "inner").
    select($"b.device_id", $"b.variant", $"b.cohort", $"a.date").
    groupBy($"device_id", $"variant", $"cohort", $"date").count()

val apply = getEvent("com.picsart.studio","quick_brush_page_close", from, to).
    filter($"exit_action"==="apply").
    select($"device_id",to_date($"timestamp").as("date")).as("a").
    join(users.as("b"),$"a.device_id"=== $"b.device_id","inner").
    select($"a.device_id",$"b.variant",$"b.cohort",$"a.date").
    groupBy($"a.device_id",$"b.variant",$"b.cohort",$"a.date").count()

val joined_open_apply = open.as("a").
join(apply.as("b"), $"a.device_id" === $"b.device_id" && $"a.date" === $"b.date", "left").
select(
    $"a.variant", $"a.cohort", $"a.date",
    $"a.device_id".as("open_device_id"), $"a.count".as("opens"),
    $"b.device_id".as("apply_device_id"), $"b.count".as("applies_1")).
withColumn("applies", when($"applies_1".isNull, lit("0")).otherwise($"applies_1")).drop($"applies_1").
groupBy($"variant", $"cohort", $"date").agg(
    countDistinct($"open_device_id").as("count_opens"),
    countDistinct($"apply_device_id").as("count_applies"),
    sum($"opens").as("opens"),
    sum($"applies").as("applies")
).
groupBy($"variant",$"cohort").agg(
    avg($"count_opens"),
    avg($"count_applies"),
    avg($"opens"),
    avg($"applies"),
    (avg($"opens")/avg($"count_opens")).as("ratio-1"),
    (avg($"applies")/avg($"count_applies")).as("ratio_2"),
    (avg($"count_applies")/avg($"count_opens")).as("apply_DAU/open_DAU"))

//---------------------------------------------------------------------------------------------------------------------------------------global
val to = DateTime.parse("2018-04-07")
val from = DateTime.parse("2018-03-24")

val open = getEvent("com.picsart.studio","edit_item_open",from,to).
    filter($"item"==="quick_brush").
    select($"device_id", to_date($"timestamp").as("date")).as("a").
    join(users.as("b"),"a.device_id" === $"b.device_id", "inner").
    select($"b.device_id", $"b.variant", $"b.cohort", $"a.date").
    groupBy($"a.device_id",$"variant",$"cohort",$"date").count()
val done = getEvent("com.picsart.studio","editor_done_click",from,to).
select($"device_id",to_date($"timestamp").as("date")).as("a").
    join(users.as("b"),$"a.device_id"=== $"b.device_id","inner").
    select($"a.device_id",$"b.variant",$"b.cohort",$"a.date").
    groupBy($"a.device_id",$"b.variant",$"b.cohort",$"a.date").count()
val joined_open_done = open.as("a").
    join(done.as("b"), $"a.device_id"===$"b.device_id" && $"a.date" === $"b.date", "left").
    select(
        $"a.variant",$"a.cohort",$"a.date",
        $"a.device_id".as("open_device_id"),$"a.count".as("opens"),
        $"b.device_id".as("done_device_id"),$"b.count".as("dones_1")
    ).
    withColumn("dones", when($"dones_1".isNull, lit("0")).otherwise($"dones_1")).drop($"dones_1").
    groupBy($"variant",$"cohort",$"date").
    agg(
        countDistinct($"open_device_id").as("count_opens"),
        countDistinct($"done_device_id").as("count_dones"),
        sum($"opens").as("opens"),
        sum($"dones").as("dones")
    ).
    groupBy($"variant",$"cohort").agg(
        avg($"count_opens"),
        avg($"count_dones"),
        avg($"opens"),
        avg($"dones"),
        (avg($"opens")/avg($"count_opens")).as("ratio-1"),
        (avg($"dones")/avg($"count_dones")).as("ratio_2"),
        (avg($"count_dones")/avg($"count_opens")).as("done_DAU/open_DAU")
    )

//-------------------------------------------------------------------------------------------------------apply and setting_change
val to = DateTime.parse("2018-04-07")
val from = DateTime.parse("2018-03-24")

val apply = getEvent("com.picsart.studio","quick_brush_page_close", from, to).
    filter($"exit_action"==="apply").
    select($"editor_sid",$"device_id",to_date($"timestamp").as("date")).as("a").
    join(users.as("b"),$"a.device_id"=== $"b.device_id","inner").
    select($"a.editor_sid", $"a.device_id",$"b.variant",$"b.cohort",$"a.date").
    groupBy($"a.device_id",$"a.editor_sid",$"b.variant",$"b.cohort",$"a.date").count()

val setting_change = getEvent("com.picsart.studio","quick_brush_setting_change",from,to).
    filter($"setting_name".in("size","opacity")).
    select($"editor_sid",$"device_id",to_date($"timestamp").as("date")).as("a").
    join(users.as("b"),$"a.device_id"=== $"b.device_id","inner").
    select($"a.editor_sid",$"a.device_id",$"b.variant",$"b.cohort",$"a.date").
    groupBy($"a.editor_sid",$"a.device_id",$"b.variant",$"b.cohort",$"a.date").count()

val joined_apply_sett_change = apply.as("a").
    join(setting_change.as("b"),$"a.editor_sid"=== $"b.editor_sid" && $"a.date"=== $"b.date", "left").
        select(
            $"a.editor_sid".as("apply_editor_sid"),
            $"b.editor_sid".as("change_editor_sid"),
            $"a.variant",
            $"a.cohort",
            $"a.date", 
            $"a.count".as("applies"), 
            $"b.count".as("changes")
        ).
        groupBy($"variant",$"cohort",$"date").
        agg(
            countDistinct($"apply_editor_sid").as("count_apply_ed"), 
            countDistinct($"change_editor_sid").as("count_change_ed")).
        groupBy($"variant",$"cohort").
        agg(
            avg($"count_apply_ed").as("count_apply_ed"),
            avg($"count_change_ed").as("count_change_ed"),
            (avg($"count_change_ed")/avg($"count_apply_ed")).as("avg_ratio")
        ).
        select($"variant", $"cohort", $"count_apply_ed", $"count_change_ed", $"avg_ratio")


//----------------------------------------------------------------------------------------------apply5+

val to = DateTime.parse("2018-04-07")
val from = DateTime.parse("2018-03-24")

val appliers_5 = getEvent("com.picsart.studio","quick_brush_page_close", from, to).
    filter($"exit_action"==="apply").
    select($"device_id",to_date($"timestamp").as("date")).
    groupBy($"device_id", $"date").count().as("a").
    join(exp.as("b"), $"a.device_id" === $"b.device_id","inner").
    select($"a.device_id",$"b.variant",$"a.date", $"a.count").
    withColumn("applies>=5", when($"count" >= 5, lit(1)).otherwise(lit(0))).
    groupBy($"variant", $"date").agg(sum($"applies>=5").as("5+ appliers"), countDistinct($"device_id").as("all appliers")).
    withColumn("ratio", $"5+ appliers" / $"all appliers")
appliers_5.    
    groupBy($"variant").agg(avg($"5+ appliers"), avg($"all appliers"), avg($"ratio")).
    orderBy($"variant").
    show(100, false)
    

//-----------------------------------------------------------------------------------------------------distributions

val to = DateTime.parse("2018-04-07")
val from = DateTime.parse("2018-03-13")
val exp = getEvent("com.picsart.studio","experiment_participate",from,to).
    filter($"platform"==="android" && $"experiment_id"==="e111").
    select($"device_id", $"variant").distinct()



val toX = DateTime.parse("2018-03-13")
val fromX = DateTime.parse("2018-02-13")
val done  = getEvent("com.picsart.studio","editor_done_click",fromX,toX).
    filter($"platform"==="android").
    select($"device_id",to_date($"timestamp").as("date")).
    groupBy($"device_id").agg(countDistinct($"date").as("active_days")).
    withColumn("cohort",
        when($"active_days" >= 13, lit("active")).
        when($"active_days" <=3 && $"active_days" >=1, lit("passive")).
        otherwise(lit("-"))
    ).
    drop($"active_days").
    filter($"cohort" === "active" || $"cohort" === "passive")

var users = exp.as("a").
    join(done.as("b"), $"a.device_id" === $"b.device_id", "inner").
    drop($"b.device_id")



val to = DateTime.parse("2018-04-07")
val from = DateTime.parse("2018-03-24")




val open = getEvent("com.picsart.studio","edit_item_open", from, to).
    filter($"item"==="quick_brush").
    select($"device_id",to_date($"timestamp").as("date")).as("a").
    join(users.as("b"), $"a.device_id" === $"b.device_id", "inner").
    select($"b.device_id", $"b.variant", $"b.cohort",$"a.date").
    groupBy($"device_id", $"variant", $"date",$"cohort").count()

val apply = getEvent("com.picsart.studio","quick_brush_page_close", from, to).
    filter($"exit_action"==="apply").
    select($"device_id",to_date($"timestamp").as("date")).as("a").
    join(users.as("b"),$"a.device_id"=== $"b.device_id","inner").
    select($"a.device_id",$"b.variant",$"b.cohort",$"a.date").
    groupBy($"a.device_id",$"b.variant",$"b.cohort",$"a.date").count()

val joined_open_apply = open.as("a").
join(apply.as("b"), $"a.device_id" === $"b.device_id" && $"a.date"=== $"b.date", "left").
select($"a.device_id", $"a.variant", $"a.date",$"a.cohort", $"a.count".as("opens"), $"b.count".as("applies_1")).
withColumn("applies", when($"applies_1".isNull, lit("0")).otherwise($"applies_1")).
 withColumn("opens_grouped",
        when($"opens" <= 5, $"opens").
        when($"opens" <= 10 && $"opens" >= 6, lit("6-10")).
        when($"opens" <= 20 && $"opens" >= 11, lit("11-20")).
        when($"opens" <= 50 && $"opens" >= 21, lit("21-50")).
        when($"opens" >= 51, lit("51+"))
    ).
 withColumn("applies_grouped",
        when($"applies" <= 5, $"applies").
        when($"applies" <= 10 && $"applies" >= 6, lit("6-10")).
        when($"applies" <= 20 && $"applies" >= 11, lit("11-20")).
        when($"applies" <= 50 && $"applies" >= 21, lit("21-50")).
        when($"applies" >= 51, lit("51+"))
    )

 joined_open_apply.
    groupBy($"variant",$"cohort",$"date",$"opens_grouped").agg(countDistinct($"device_id").as("open_devices")).
    groupBy($"variant",$"cohort").pivot("opens_grouped").agg(avg("open_devices")).
    show(false)
 
 joined_open_apply.
    groupBy($"variant",$"cohort",$"date",$"applies_grouped").agg(countDistinct($"device_id").as("apply_devices")).
    groupBy($"variant",$"cohort").pivot("applies_grouped").agg(avg("apply_devices")).
    show(false)







joined_open_apply.
    groupBy($"opens", $"cohort", $"variant").agg(countDistinct($"device_id").as("open_devices")).
    withColumn("opens_grouped",
        when($"opens" <= 5, $"opens").
        when($"opens" <= 10 && $"opens" >= 6, lit("6-10")).
        when($"opens" <= 20 && $"opens" >= 11, lit("11-20")).
        when($"opens" <= 50 && $"opens" >= 21, lit("21-50")).
        when($"opens" >= 51, lit("51+"))
    ).
    groupBy($"cohort", $"variant").pivot("opens_grouped").agg(sum($"open_devices")).
    show(false)


joined_open_apply.

    groupBy($"applies",$"cohort",$"variant").agg(countDistinct($"device_id").as("apply_devices")).
    withColumn("applies_grouped",
        when($"applies" <= 5, $"applies").
        when($"applies" <= 10 && $"applies">=6, lit("6-10")).
        when($"applies" <= 20 && $"applies">=11, lit("11-20")).
        when($"applies" <= 50 && $"applies">=21, lit("21-50")).
        when($"applies" >=51, lit("51+"))
    ).
    groupBy($"cohort",$"variant").pivot("applies_grouped").agg(sum($"apply_devices")).
    show(false)


 

