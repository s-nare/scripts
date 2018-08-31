import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}
val formatter = DateTimeFormat.forPattern("ddMMyyyy")
val from  = formatter.parseDateTime("01052018")
val to = formatter.parseDateTime("31052018")

var from_str = from.toString().substring(0,10)
var to_str = to.toString().substring(0,10)
var top_countries = Array("ar","br","ca","cn","co","fr","de","in","id","it","jp","my","mx","ph","ru","kr","es","tr","gb","us","vn")
val countries = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/user/amber/csv/country_code.csv").select($"country_name", lower($"country_code").as("country_code"))

val open = getEvent("com.picsart.studio", "camera_open", from, to).
    filter(($"platform" === "apple" && length($"version") === 3 && $"version" >= "546") || ($"platform" === "android" && length($"version") === 9 && $"version" >= "993000213")).
    select($"device_id", to_date($"timestamp").as("dd"), $"platform", $"camera_sid", lower($"country_code").as("c")).
    withColumn("cc", when($"c".isin(top_countries:_*), $"c").otherwise("-"))
val capture = getEvent("com.picsart.studio", "camera_capture", from, to).
    filter(($"platform" === "apple" && length($"version") === 3 && $"version" >= "546") || ($"platform" === "android" && length($"version") === 9 && $"version" >= "993000213"))
val done = getEvent("com.picsart.studio", "camera_preview_close", from, to).filter($"action" === "done").
    filter(($"platform" === "apple" && length($"version") === 3 && $"version" >= "546") || ($"platform" === "android" && length($"version") === 9 && $"version" >= "993000213")).
    select($"device_id", to_date($"timestamp").as("dd"), $"platform", $"camera_sid", lower($"country_code").as("c")).
    withColumn("cc", when($"c".isin(top_countries:_*), $"c").otherwise("-"))

//-------------------------------------------------------ED/DAU






//-------------------------------------------------------CAPTURES TOTAL
    val total = capture.groupBy($"platform").agg(countDistinct($"camera_sid").as("total_sid"))
    val scene = capture.filter(($"scene_name".isNotNull) && (lower($"scene_name") !== "none")).groupBy($"platform").agg(countDistinct($"camera_sid").as("scene_sid"))
    val sticker = capture.filter($"custom_sticker_number" > 0 || $"random_sticker_number" > 0).groupBy($"platform").agg(countDistinct($"camera_sid").as("sticker_sid"))
    val filter = capture
        .filter(($"effect_name".isNotNull) && (lower($"effect_name") !== "none"))
        .filter(($"scene_name".isNull) || (lower($"scene_name") === "none"))
        .groupBy($"platform").agg(countDistinct($"camera_sid").as("filter_sid"))
    val nothing = capture
        .filter(($"scene_name".isNull) || (lower($"scene_name") === "none"))
        .filter(($"effect_name".isNull) || (lower($"effect_name") === "none"))
        .filter($"custom_sticker_number" === 0 && $"random_sticker_number" === 0).groupBy($"platform").agg(countDistinct($"camera_sid").as("nothing_sid"))
    val part2 = total.as("a")
        .join(scene.as("b"), $"a.platform" === $"b.platform", "inner")
        .join(filter.as("c"), $"a.platform" === $"c.platform", "inner")
        .join(sticker.as("d"), $"a.platform" === $"d.platform", "inner")
        .join(nothing.as("e"), $"a.platform" === $"e.platform", "inner")
        .select($"a.platform", $"a.total_sid".as("Total"), $"b.scene_sid".as("Scene"), $"c.filter_sid".as("Filter"), $"d.sticker_sid".as("Sticker"), $"e.nothing_sid".as("Nothing"))
//-------------------------------------------------------CAPTURES DONE
    val capture_done = capture.as("a").join(done.select($"camera_sid").distinct().as("b"), $"a.camera_sid" === $"b.camera_sid", "inner").drop($"b.camera_sid")
    val total = capture_done.groupBy($"platform").agg(countDistinct($"camera_sid").as("total_sid"))
    val scene = capture_done.filter(($"scene_name".isNotNull) && (lower($"scene_name") !== "none")).groupBy($"platform").agg(countDistinct($"camera_sid").as("scene_sid"))
    val sticker = capture_done.filter($"custom_sticker_number" > 0 || $"random_sticker_number" > 0).groupBy($"platform").agg(countDistinct($"camera_sid").as("sticker_sid"))
    val filter = capture_done
        .filter(($"effect_name".isNotNull) && (lower($"effect_name") !== "none"))
        .filter(($"scene_name".isNull) || (lower($"scene_name") === "none"))
        .groupBy($"platform").agg(countDistinct($"camera_sid").as("filter_sid"))
    val nothing = capture_done
        .filter(($"scene_name".isNull) || (lower($"scene_name") === "none"))
        .filter(($"effect_name".isNull) || (lower($"effect_name") === "none"))
        .filter($"custom_sticker_number" === 0 && $"random_sticker_number" === 0).groupBy($"platform").agg(countDistinct($"camera_sid").as("nothing_sid"))
    val part2 = total.as("a")
        .join(scene.as("b"), $"a.platform" === $"b.platform", "inner")
        .join(filter.as("c"), $"a.platform" === $"c.platform", "inner")
        .join(sticker.as("d"), $"a.platform" === $"d.platform", "inner")
        .join(nothing.as("e"), $"a.platform" === $"e.platform", "inner")
        .select($"a.platform", $"a.total_sid".as("Total"), $"b.scene_sid".as("Scene"), $"c.filter_sid".as("Filter"), $"d.sticker_sid".as("Sticker"), $"e.nothing_sid".as("Nothing"))

//-------------------------------------------------------PART 1
    
    val dau = done.groupBy($"dd", $"cc", $"platform").agg(countDistinct($"device_id").as("DAU")).groupBy($"cc", $"platform").agg(avg($"DAU").as("DAU"))
    val mau = done.groupBy($"cc", $"platform").agg(countDistinct($"device_id").as("MAU"))
    dau.as("a").
        join(mau.as("b"), $"a.platform" === $"b.platform" && $"a.cc" === $"b.cc", "inner").
        select($"a.cc", $"a.platform", $"a.DAU", $"b.MAU", ($"a.DAU"/ $"b.MAU").as("DAU/MAU")).as("c").
        join(countries.as("d"), $"c.cc" === $"d.country_code","left").
        select($"d.country_name",$"c.platform", $"c.DAU", $"c.MAU", $"DAU/MAU"). 
        withColumn("country", when($"country_name".isNull, ("other")).otherwise($"country_name")).drop($"country_name").   
        orderBy($"country",$"platform").
        show(1000, false)

 




 val camera_DAU = open.as("a").join(done.
    groupBy($"dd",$"cc",$"device_id",$"platform").agg(
        countDistinct($"camera_sid").as("count_camera_sid")
    ).as("b"), $"a.device_id"=== $"b.device_id" && $"a.dd"=== $"b.dd", "left").
 select($"a.dd",$"a.cc",$"a.platform",$"a.device_id".as("open_dv"),$"b.device_id".as("done_dv"),$"b.count_camera_sid").
 groupBy($"dd",$"cc",$"platform").agg(
    countDistinct($"open_dv").as("open_dv"),
    countDistinct($"done_dv").as("done_dv"),
    sum($"count_camera_sid").as("count_done")).
 groupBy($"cc",$"platform").agg(
        avg($"count_done").as("count_done"),
        (avg($"done_dv")/avg($"open_dv")).as("done/open"),
        (avg($"count_done")/avg($"done_dv")).as("dones/dau")
    ).as("a").
 join(countries.as("b"),$"a.cc"=== $"b.country_code","left").
 select($"b.country_name", $"a.platform", $"count_done",$"a.done/open",$"a.dones/dau").
 withColumn("country", when($"country_name".isNull, ("other")).otherwise($"country_name")).drop($"country_name").   
orderBy($"country",$"platform").
show(1000, false)

//////////////////////////////////////// Total

val dau = done.groupBy($"dd",$"platform").agg(countDistinct($"device_id").as("DAU")).groupBy($"platform").agg(avg($"DAU").as("DAU"))
    val mau = done.groupBy($"platform").agg(countDistinct($"device_id").as("MAU"))
    dau.as("a").
        join(mau.as("b"), $"a.platform" === $"b.platform", "inner").
        select($"a.platform", $"a.DAU", $"b.MAU", ($"a.DAU"/ $"b.MAU").as("DAU/MAU")).show(false)
      


 val camera_DAU = open.as("a").join(done.
    groupBy($"dd",$"cc",$"device_id",$"platform").agg(
        countDistinct($"camera_sid").as("count_camera_sid")
    ).as("b"), $"a.device_id"=== $"b.device_id" && $"a.dd"=== $"b.dd", "left").
 select($"a.dd",$"a.cc",$"a.platform",$"a.device_id".as("open_dv"),$"b.device_id".as("done_dv"),$"b.count_camera_sid").
 groupBy($"dd",$"platform").agg(
    countDistinct($"open_dv").as("open_dv"),
    countDistinct($"done_dv").as("done_dv"),
    sum($"count_camera_sid").as("count_done")).
 groupBy($"platform").agg(
        avg($"count_done").as("count_done"),
        (avg($"done_dv")/avg($"open_dv")).as("done/open"),
        (avg($"count_done")/avg($"done_dv")).as("dones/dau")
    ).show(1000, false)







