var formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
val fromX = DateTime.parse("2018-07-22")
val toX = DateTime.parse("2018-07-29")
val version = "700"
val platform = "apple"
val ord = sqlContext.read.parquet("/analytics/entities/orders")
val users = sqlContext.read.parquet("/analytics/entities/users_orders").
    select($"device_id",$"user_id", $"order_id"). 
    filter($"device_id". isNotNull).
    distinct()
val ord_users=ord.as ("a").join(users. as ("b"), $"a._id"===$"b.order_id", "inner")

val orders1 = ord_users.
    withColumn("platform", when($"market" === "google", "android").otherwise("apple")).
    filter($"platform" === platform).
     withColumn("type",
        when(lower($"subscription_id").like("%monthly%"), "monthly").
        when(lower($"subscription_id").like("%yearly%"), "yearly").
        otherwise(lit("other"))).
    select($"_id",$"device_id", $"type",$"purchase_date",explode($"renewals").as("rn")).
    select($"_id", $"device_id", $"type", to_date($"rn.purchaseDate").as("rn_purchase"), $"rn.paymentStatus".as("rn_payment")).
    withColumn("N", rowNumber().over(Window.partitionBy($"_id").orderBy($"rn_purchase"))).distinct()

val orders2 = ord_users.
    withColumn("platform", when($"market" === "google", "android").otherwise("apple")).
    filter($"platform" === "apple").
    select($"device_id", explode($"renewals").as("rn")).
    select($"device_id", ($"rn.purchaseDate").as("rn_purchase"),$"rn.expireDate".as("rn_expire")).
    filter($"rn_purchase" >= fromX.toString().slice(0,10)).
    select($"device_id", $"rn_purchase", $"rn_expire")

val orders = (/*orders1 ||*/ orders2)
 

var dates_seq : Seq[(String, String)] = Seq()
for (f<- 0 to Days.daysBetween(fromX, toX).getDays()) {
    val b = (fromX.plusDays(f).toString(formatter), "A")
    dates_seq = dates_seq :+ b
}
val dates = dates_seq.toDF("dd", "A").select($"dd")


val data = dates.as("a").join(orders.as("b"), $"a.dd" >= $"b.rn_purchase" && $"a.dd" <= $"b.rn_expire", "left").
select($"b.device_id",$"a.dd")






///////////////////////////////////////////////////////
getEvent("com.picsart.studio", "subscription_validation", fromX, toX).
    filter($"platform" === platform /*&& $"version" >= version && length($"version") === 3*/).
    select($"device_id", to_date($"timestamp").as("dd"), when($"source" === "video_editor_create_flow", lit("video")).otherwise(lit("-")).as("source")).
    groupBy($"dd", $"source").agg(countDistinct($"device_id")).
    orderBy($"dd", $"source").
    show(false)

//////////////////////////////////////////////////////////////////////////////

val photo_chooser = getEvent("com.picsart.studio", "photo_chooser_photo_click", fromX, toX).
filter($"platform" === platform && $"version" >= version && length($"version") === 3).
filter($"media_type" === "video").
select($"device_id", $"timestamp",to_date($"timestamp").as("date"), $"pc_sid").distinct()

val non_subscribers = photo_chooser.as("a").
join(orders2.as("b"), $"a.device_id" === $"b.device_id" && $"a.date" <= $"b.rn_purchase" ,"inner").
select($"a.device_id",$"a.timestamp", $"a.pc_sid", $"b.rn_purchase")

val tutorial_open = getEvent("com.picsart.studio", "onboarding_tutorial_open", fromX, toX).filter($"platform" === platform && $"version" >= version && length($"version") === 3).
filter($"source" === "video_editor_tutorial").
select($"device_id", $"source_sid", $"timestamp").distinct()

val sub_offer = getEvent("com.picsart.studio", "subscription_offer_open", fromX, toX).filter($"version" >= version && $"platform" === platform && length($"version") === 3).
filter($"source" === "video_editor_create_flow").
select($"device_id", $"sub_sid", $"source_sid", $"timestamp")

val sub_done = getEvent("com.picsart.studio", "subscription_done", fromX, toX).filter($"version" >= version && $"platform" === platform && length($"version") === 3).
select($"device_id", $"sub_sid", $"timestamp")

val sub_validation = getEvent("com.picsart.studio", "subscription_validation", fromX, toX).filter($"version" >= version && $"platform" === platform && length($"version") === 3).
filter($"response_type" !== "invalid").
select($"device_id", $"sub_sid", $"timestamp")

val sub_flow =
    photo_chooser.as("a").join(tutorial_open.as("b"), $"a.pc_sid" === $"b.source_sid" && $"a.timestamp" <= $"b.timestamp", "left").
        select($"a.timestamp".as("dd"), $"a.device_id".as("chooser_dv"), $"b.device_id".as("tutorial_dv"), $"b.source_sid", $"b.timestamp").as("a").
            join(sub_offer.as("b"), $"a.source_sid" === $"b.source_sid" && $"a.timestamp" <= $"b.timestamp", "left").
            select($"a.dd", $"chooser_dv", $"tutorial_dv", $"b.device_id".as("offer_dv"),$"b.sub_sid", $"b.timestamp").as("a").
                join(sub_done.as("b"), $"a.sub_sid" === $"b.sub_sid" && $"a.timestamp" <= $"b.timestamp", "left").
                select($"a.dd", $"chooser_dv", $"tutorial_dv", $"offer_dv", $"b.device_id".as("done_dv")).
                groupBy(to_date($"dd").as("ddd")).
                agg(
                    countDistinct($"chooser_dv").as("chooser_dv"),
                    countDistinct($"tutorial_dv").as("tutorial_dv"),
                    countDistinct($"offer_dv").as("offer_dv"),
                    countDistinct($"done_dv").as("done_dv")
                ).
                orderBy($"ddd")


val without_tuorial =
    photo_chooser.as("a").join(sub_offer.as("b"), $"a.pc_sid" === $"b.source_sid" && $"a.timestamp" <= $"b.timestamp", "left").
        select($"a.timestamp".as("dd"), $"a.device_id".as("chooser_dv"), $"b.device_id".as("offer_dv"),$"b.sub_sid", $"b.timestamp").as("a").
            join(sub_done.as("b"), $"a.sub_sid" === $"b.sub_sid" && $"a.timestamp" <= $"b.timestamp", "left").
            select($"a.dd", $"chooser_dv", $"offer_dv", $"b.device_id".as("done_dv")).
            groupBy(to_date($"dd").as("ddd")).
            agg(
                countDistinct($"chooser_dv").as("chooser_dv"),
                countDistinct($"offer_dv").as("offer_dv"),
                countDistinct($"done_dv").as("done_dv")
            ).
            orderBy($"ddd")


////////////////////////////////////free trials
val ft = orders.
    filter($"N" === 1).
    filter(to_date($"rn_purchase") >= fromX.toString().slice(0, 10) && to_date($"rn_purchase") <= toX.toString().slice(0, 10)).
    filter($"rn_payment" === "FREE_TRIAL").
    select($"_id",$"type",$"device_id", $"rn_purchase").distinct()

val paid = orders.
    filter($"N" === 2).
    filter(to_date($"rn_purchase") >= fromX.toString().slice(0, 10) && to_date($"rn_purchase") <= toX.toString().slice(0, 10)).
    filter($"rn_payment" === "PAID").
    select($"_id", $"type", $"device_id").distinct()

//Conversions
val ft_offer_done = sub_offer.as("a").
    join(sub_done.as("b"), $"a.sub_sid" === $"b.sub_sid", "inner").
    select($"a.device_id", $"b.timestamp").as("a").
        join(ft.as("b"), $"a.device_id" === $"b.device_id", "inner").
        select($"_id",$"type",$"b.device_id",$"b.rn_purchase")

val video_ft_paid = ft_offer_done.as("a").
    join(paid.as("b"), $"a._id" === $"b._id", "left").
    select($"a.rn_purchase", $"a._id".as("ft"), $"b._id".as("paid"), $"a.type").
    groupBy($"rn_purchase").pivot("type").
    agg(countDistinct($"ft").as("ft"), countDistinct($"paid").as("paid"))






/* WEIGHT OF SUBSCRIBERS FROM EXACT SOURCE (FROM BACKEND DATA)
val ord=sqlContext.read.parquet("/analytics/entities/orders")
val users = sqlContext.read.parquet("/analytics/entities/users_orders").select($"device_id",$"user_id", $"order_id").filter($"device_id". isNotNull).distinct()
val ord_users=ord.as ("a").join(users. as ("b"), $"a._id"=== $"b.order_id", "inner")
/////////////////////////////////////////////////////////////////
val orders = ord_users.
    withColumn("platform", when($"market" === "google", "android").otherwise("apple")).filter($"platform" === "apple").
    select($"device_id",$"purchase_date").select($"device_id", to_date($"purchase_date").as("purchase_dateX")).
    filter($"purchase_dateX" >= fromX.toString().slice(0, 10) && $"purchase_dateX" <= toX.toString().slice(0, 10))
val sub_offer = getEvent("com.picsart.studio", "subscription_offer_open", fromX, toX).
filter($"version" >= version && $"platform" === platform).select($"device_id", $"sub_sid", to_date($"timestamp").as("sub_date"), $"source")
val sub_done = getEvent("com.picsart.studio", "subscription_done", fromX, toX).select($"device_id", to_date($"timestamp").as("done_date"), $"source", $"sub_sid")
val offer_done =sub_offer.as("a").join(sub_done.as("b"), $"a.sub_sid" === $"b.sub_sid", "inner").
select($"b.device_id", $"a.sub_date", $"a.sub_sid",  $"a.source").filter($"source" === "video_editor_create_flow")
val subscribed = orders.as("a").join(offer_done.as("b"), $"a.device_id" === $"b.device_id" && $"b.sub_date" === $"a.purchase_dateX", "left").
select($"a.device_id".as("all_subscribers"), $"b.device_id".as("video_subscribers"), $"a.purchase_dateX").
groupBy($"purchase_dateX").
agg(countDistinct($"all_subscribers").as("all_subscribers"), countDistinct($"video_subscribers").as("video_subscribers"))
*/ 









​