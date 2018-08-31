val from = DateTime.parse("2018-07-04")
val to = DateTime.parse("2018-07-10")

val fromx = DateTime.parse("2018-06-26")
val tox = DateTime.parse("2018-07-03")

val platform = "apple"
val version = "680"

//////////// cohorts

val eff_try_0 = getEvent("com.picsart.studio", "effect_try", fromx, to).
filter($"is_premium" === true && $"platform" === platform && $"version" >= version)
val eff_try_1 = eff_try_0.select($"device_id", $"is_subscribed").
withColumn("subscribed", when($"is_subscribed" === true, lit(10)).when($"is_subscribed" === false, lit(1)).otherwise(lit(1000000))).
select($"device_id", $"subscribed").distinct().
groupBy($"device_id").agg(sum($"subscribed").as("point")).
filter($"point" === 1).
select($"device_id")
val eff_try_2 = eff_try_0.groupBy($"device_id").agg(min(to_date($"timestamp")).as("d"))
val cohort = eff_try_1.as("a").join(eff_try_2.as("b"), $"a.device_id" === $"b.device_id", "inner").select($"b.device_id", $"b.d")




// val eff_try = getEvent("com.picsart.studio", "effect_try", fromx, to).
// filter($"is_premium" === true).
// select($"device_id", $"is_subscribed", to_date($"timestamp").as("d")).
// withColumn("subscribed", when($"is_subscribed" === true, lit(10)).when($"is_subscribed" === false, lit(1)).otherwise(lit(1000000))).
// select($"device_id", $"d", $"subscribed").distinct().
// groupBy($"device_id", $"d").agg(sum($"subscribed").as("point")).
// groupBy($"device_id").agg(min($"d").as("d"), countDistinct($"point").as("values"), min($"point").as("min_value")).
// //groupBy($"d", $"values", $"min_value").agg(countDistinct($"device_id")).orderBy($"d", $"values", $"min_value").show(100, false)
// filter($"values" === 1 && $"min_value" === 1).
// select($"device_id", $"d")


val cohort_1 = getEvent("com.picsart.studio", "effect_try", fromx, to).
filter($"is_premium" === true && $"is_subscribed" === true && $"platform" === platform && $"version" >= version).
select($"device_id", to_date($"timestamp").as("d")).
groupBy($"device_id").agg(min($"d").as("d")).
select($"device_id", $"d")


/////////////////////////////// subscribers

var formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
/*
var from = today().minusDays(35)//formatter.parseDateTime("2018-03-01")//
var to = today().plusDays(30)
*/
var dates_seq : Seq[(String, String)] = Seq()
for (f<- 0 to Days.daysBetween(from, to).getDays()) {
    val b = (from.plusDays(f).toString(formatter), "A")
    dates_seq = dates_seq :+ b
}
val dates = dates_seq.toDF("dd", "A").select($"dd")

//---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
var orders = sqlContext.read.parquet("/analytics/entities/orders").//sqlContext.read.parquet("/tmp/o").
    withColumn("purchase_dateX",to_date($"purchase_date")).
    withColumn("expire_dateX",to_date($"expire_date")).
    withColumn("platform", when($"market" === "google", lit("android")).otherwise(lit("apple"))).
    filter($"platform" === "apple").
    select($"_id", $"purchase_dateX", $"expire_dateX", $"platform").
    drop($"purchase_date").drop($"expire_date").distinct()   
    

var users = sqlContext.read.parquet("/analytics/entities/users_orders").
select($"device_id".as("did"), $"order_id".as("order_id")).filter($"did".isNotNull).distinct()

val data = orders.as("a").join(users.as("b"), $"a._id" === $"b.order_id", "inner").
select(
        $"b.did", $"a._id",
        $"a.purchase_dateX".as("start_date"), 
        $"a.expire_dateX".as("finish_date")
    )

val dates_data = dates.as("a").join(data.as("b"), $"a.dd" >= $"start_date" && $"a.dd" <= $"finish_date", "left").
select($"a.dd",$"b.did", $"b._id")


val k = dates_data.as("a").join(cohort_1.as("b"), $"a.did" === $"b.device_id", "left").
select($"a.dd".as("sub_date"), $"a.did".as("all_sub_dv"), $"b.device_id".as("try_dv"), $"b.d".as("min_date_try")).
filter($"try_dv".isNull).
select($"all_sub_dv", $"try_dv").distinct()


val g = item_open.as("a").join(k.as("b"), $"a.device_id" === $"b.all_sub_dv", "inner").
select($"a.device_id", $"a.editor_sid", $"a.open_date").as("a").
join(eff_apply.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
select(
        $"a.open_date",
        $"a.device_id".as("open_dv"),
        $"b.device_id".as("apply_dv"),
        $"b.count".as("apply_count")
    ).
groupBy($"open_date").
agg(
        countDistinct($"open_dv").as("open_dv"), 
        countDistinct($"apply_dv").as("apply_dv"), 
        sum($"apply_count").as("apply_count")
    ).
agg(avg($"open_dv"), avg($"apply_dv"), avg($"apply_count"))


val e = editor_open.as("a").join(k.as("b"), $"a.device_id" === $"b.all_sub_dv", "inner").
select($"a.device_id", $"a.editor_sid", $"a.open_date").as("a").
join(ed_done.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
select(
        $"a.open_date",
        $"a.device_id".as("open_dv"),
        $"b.device_id".as("done_dv"),
        $"b.editor_sid".as("done_sid")
    ).
groupBy($"open_date").
agg(
        countDistinct($"open_dv").as("open_dv"), 
        countDistinct($"done_dv").as("done_dv"), 
        countDistinct($"done_sid").as("done_sid")
    ).
agg(avg($"open_dv"), avg($"done_dv"), avg($"done_sid"))
 


//////////////////////////////////////////////////////////// 


//////////////// Ed-open->Done and effect open -> apply
val open_subscriber = editor_open.as("a").join(dates_data.as("b"), $"a.device_id" === $"b.did" && $"a.open_date" === $"b.dd", "inner").
select($"a.device_id", $"a.editor_sid", $"a.open_date").as("a").
join(ed_done.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
select(
        $"a.open_date",
        $"a.device_id".as("open_dv"),
        $"b.device_id".as("done_dv"),
        $"b.editor_sid".as("done_sid")
    ).
groupBy($"open_date").
agg(
        countDistinct($"open_dv").as("open_dv"), 
        countDistinct($"done_dv").as("done_dv"), 
        countDistinct($"done_sid").as("done_sid")
    ).
agg(avg($"open_dv"), avg($"done_dv"), avg($"done_sid"))


val effect_subscribed = item_open.as("a").join(dates_data.as("b"), $"a.device_id" === $"b.did" && $"a.open_date" === $"b.dd", "inner").
select($"a.device_id", $"a.editor_sid", $"a.open_date").as("a").
join(eff_apply.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
select(
        $"a.open_date",
        $"a.device_id".as("open_dv"),
        $"b.device_id".as("apply_dv"),
        $"b.count".as("apply_count")
    ).
groupBy($"open_date").
agg(
        countDistinct($"open_dv").as("open_dv"), 
        countDistinct($"apply_dv").as("apply_dv"), 
        sum($"apply_count").as("apply_count")
    ).
agg(avg($"open_dv"), avg($"apply_dv"), avg($"apply_count"))
effect_subscribed.show(false)


////////////////////////////////////////// ed open -> done  od and new


val editor_open = getEvent("com.picsart.studio", "editor_open", from,to).
    filter( $"platform" === platform && $"version" >= version).
    select($"device_id", $"editor_sid", to_date($"timestamp").as("open_date"))


val ed_done = getEvent("com.picsart.studio", "editor_done_click",from, to).
    filter($"platform"===platform && $"version" >= version).
    select($"device_id",$"editor_sid",to_date($"timestamp").as("done_date")).
    distinct()

val summarize = editor_open.as("a").join(ed_done.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
  select(
         $"a.open_date",
         $"a.device_id".as("open_dv"),
         $"b.device_id".as("done_dv"),
         $"b.editor_sid".as("done_sid")
    ).
  groupBy($"open_date").agg(countDistinct($"open_dv").as("open_dv"), countDistinct($"done_dv").as("done_dv"), countDistinct($"done_sid").as("done_sid")).
  agg(avg($"open_dv"), avg($"done_dv"), avg($"done_sid"))




////////////////////////////////// ed open -> done (non subscribed) 



val open_non_sub = editor_open.as("a").join(cohort.as("b"), $"a.device_id" === $"b.device_id" && $"open_date" >= $"d", "inner").
select($"a.device_id", $"a.open_date", $"a.editor_sid").as("c").
join(ed_done.as("d"), $"c.editor_sid" === $"d.editor_sid","left").
select(
        $"c.open_date",
        $"c.device_id".as("open_dv"),
        $"d.device_id".as("done_dv"),
        $"d.editor_sid".as("done_sid")
).
groupBy($"open_date").agg(countDistinct($"open_dv").as("open_dv"), countDistinct($"done_dv").as("done_dv"), countDistinct($"done_sid").as("done_sid")).
agg(avg($"open_dv"), avg($"done_dv"), avg($"done_sid"))

open_non_sub.show(false)



///////////////////////////// ed open -> done (subscribed) 

val open_sub = editor_open.as("a").
    join(dates_data.as("b"), $"a.device_id" === $"b.did" && $"a.open_date" === $"b.dd", "inner").
    select($"a.device_id", $"a.editor_sid", $"a.open_date").as("c").
        join(cohort_1.as("d"), $"c.device_id" === $"d.device_id" && $"c.open_date" >= $"d.d", "inner").
        select($"c.device_id", $"c.open_date", $"c.editor_sid").as("e").
            join(ed_done.as("f"), $"e.editor_sid" === $"f.editor_sid", "left").
            select(
                    $"e.open_date",
                    $"e.device_id".as("open_dv"),
                    $"f.device_id".as("done_dv"),
                    $"f.editor_sid".as("done_sid")
            ).
            groupBy($"open_date").agg(countDistinct($"open_dv").as("open_dv"), countDistinct($"done_dv").as("done_dv"), countDistinct($"done_sid").as("done_sid")).
            agg(avg($"open_dv"), avg($"done_dv"), avg($"done_sid"))

open_sub.show(false)









///////////// effect open -> apply  new and old


val item_open = getEvent("com.picsart.studio", "edit_item_open", from,to).
    filter( $"item" === "effects" && $"platform" === platform && $"version" >= version).
    select($"device_id", $"editor_sid", to_date($"timestamp").as("open_date")).distinct()


val eff_apply = getEvent("com.picsart.studio", "effect_apply",from, to).
    filter($"platform"===platform && $"version" >= version).
    select($"device_id",$"editor_sid").
    groupBy($"device_id", $"editor_sid").count()

val summarize = item_open.as("a").join(eff_apply.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
  select(
         $"a.open_date",
         $"a.device_id".as("open_dv"),
         $"b.device_id".as("apply_dv"),
         $"b.editor_sid".as("applies")
    ).
  groupBy($"open_date").agg(countDistinct($"open_dv").as("open_dv"), countDistinct($"apply_dv").as("apply_dv"), countDistinct($"applies").as("applies")).
  agg(avg($"open_dv"), avg($"apply_dv"), avg($"applies"))









///////////// effect open -> apply (non subscribed) 


val item_apply_nonsub = item_open.as("a").join(cohort.as("b"), $"a.device_id" === $"b.device_id" && $"open_date" >= $"d", "inner").
select($"a.device_id", $"a.editor_sid", $"a.open_date").as("c").join(eff_apply.as("d"), $"c.editor_sid" === $"d.editor_sid", "left").
  select(
         $"c.open_date",
         $"c.device_id".as("open_dv"),
         $"d.device_id".as("apply_dv"),
         $"d.editor_sid".as("applies")
    ).
  groupBy($"open_date").agg(countDistinct($"open_dv").as("open_dv"), countDistinct($"apply_dv").as("apply_dv"), countDistinct($"applies").as("applies")).
  agg(avg($"open_dv"), avg($"apply_dv"), avg($"applies"))









///////////// effect open -> apply (subscribed)


val item_apply_sub = item_open.as("a").
join(dates_data.as("b"), $"a.device_id" === $"b.did" && $"a.open_date" === $"b.dd", "inner").
select($"a.device_id", $"a.editor_sid", $"a.open_date").as("c").
    join(cohort_1.as("d"), $"c.device_id" === $"d.device_id" && $"c.open_date" >= $"d.d", "inner").
    select($"c.device_id", $"c.editor_sid", $"c.open_date").as("e").
        join(eff_apply.as("f"), $"e.editor_sid" === $"f.editor_sid", "left").
        select(
            $"e.open_date",
            $"e.device_id".as("open_dv"),
            $"f.device_id".as("apply_dv"),
            $"f.count".as("applies")
        ).
        groupBy($"open_date").agg(countDistinct($"open_dv").as("open_dv"), countDistinct($"apply_dv").as("apply_dv"), sum($"applies").as("applies")).
        agg(avg($"open_dv"), avg($"apply_dv"), avg($"applies"))








//////////////////////////// Tool apply distribution


val ef_apply = getEvent("com.picsart.studio", "effect_apply", from, to).
filter($"platform" === platform && $"version" >= version)


ef_apply.
select(
        $"device_id",
        $"category_name", 
        to_date($"timestamp").as("date")
    ).
groupBy($"category_name", $"date").agg(countDistinct($"device_id").as("apply_dv")).
groupBy($"category_name").agg(avg($"apply_dv"))




///////////////////////// Tool apply stickiness

ef_apply.
select($"device_id", to_date($"timestamp").as("date")).
groupBy($"date").agg(countDistinct($"device_id").as("dv_id")).
agg(avg($"dv_id").as("dv_id")).show(false)


ef_apply.
select($"device_id", to_date($"timestamp").as("date")).
agg(countDistinct($"device_id")).show(false)


////////////////////// effect try -> apply

val _apply = ef_apply.filter($"is_subscribed" === true).
select($"editor_sid", $"effect_name", to_date($"timestamp").as("date")).
groupBy($"editor_sid", $"date", $"effect_name").count()


val _try = getEvent("com.picsart.studio", "effect_try", from, to).
filter($"is_subscribed" === true && $"platform" === platform && $"version" >= version).
select($"editor_sid", to_date($"timestamp").as("date"), $"effect_name").
groupBy($"editor_sid", $"date", $"effect_name").count().
as("c").
    join(_apply.as("d"), $"c.editor_sid" === $"d.editor_sid" && $"c.effect_name" === $"d.effect_name", "left").
    select(
            $"c.date",
            $"c.effect_name".as("try_eff"),
            $"c.editor_sid".as("try_sid"),
            $"d.editor_sid".as("apply_sid"),
            $"c.count".as("try_count"),
            $"d.count".as("apply_count")
        ).
    groupBy($"date", $"try_eff").
    agg(
            countDistinct($"try_sid").as("try_sid"), 
            countDistinct($"apply_sid").as("apply_sid"),
            sum($"try_count").as("try_c"), sum($"apply_count").as("apply_c")
        ).
    groupBy($"try_eff").
    agg(
        avg($"try_sid"), 
        avg($"apply_sid"),
        avg($"try_c"), 
        avg($"apply_c")
    )











