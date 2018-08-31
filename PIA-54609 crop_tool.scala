val fromx = DateTime.parse("2018-04-26")
val tox = DateTime.parse("2018-05-26")
val platform = "apple"
/*val version = "660"*/

 

val su = getEvent("com.picsart.studio","editor_done_click", fromx, tox).
filter($"platform"===platform).
select($"device_id", to_date($"timestamp").as("date")).
groupBy($"device_id").
agg(
	countDistinct($"date").as("active_days")
).
withColumn("cohort",
    when($"active_days" >= 13, lit("active")).
    when($"active_days" <=3 && $"active_days" >=1, lit("passive")).
    otherwise(lit("-"))
).
drop($"active_days").
filter($"cohort" === "active")




val from = DateTime.parse("2018-05-20")
val to = DateTime.parse("2018-05-26")




val crop_open = getEvent("com.picsart.studio","tool_crop_open", from,to).
filter($"platform"===platform && $"version" >= version).
select($"device_id",$"editor_sid",to_date($"timestamp").as("open_date")).as("a").
join(su.as("b"), $"a.device_id"=== $"b.device_id","inner").
select($"a.device_id",$"a.editor_sid",$"a.open_date",$"b.cohort").
groupBy($"a.device_id",$"a.editor_sid",$"a.open_date",$"b.cohort").count()

val crop_apply = getEvent("com.picsart.studio","tool_crop_apply",from, to).
filter($"platform"===platform && $"version" >= version).
select($"device_id",$"editor_sid",to_date($"timestamp").as("apply_date"),$"aspect_ratio").
groupBy($"device_id",$"editor_sid",$"apply_date").count()

val ed_done = getEvent("com.picsart.studio","editor_done_click",from, to).
filter($"platform"===platform && $"version" >= version).
select($"device_id",$"editor_sid",to_date($"timestamp").as("done_date")).distinct()

///////////////////tool apply distribution

crop_apply.
select(
	$"device_id",
	$"apply_date",
	$"count"
).as("a").
join(su.as("b"),
	$"a.device_id"=== $"b.device_id", "inner"
).
select(
	$"a.device_id",
	$"a.apply_date",
	$"b.cohort",
	$"a.count".as("apply_count")
).
groupBy($"apply_date",$"cohort",$"apply_count").
agg(
	countDistinct($"device_id").as("dv_id")
).
groupBy($"apply_count").
agg(
avg($"dv_id").as("dv_id")
).
withColumn("applies", 
  when($"apply_count" >= 11 && $"apply_count" <= 15, lit("11-15")).
  when($"apply_count" >= 16 && $"apply_count" <= 20, lit("16-20")).
  when($"apply_count" >= 21 && $"apply_count" <= 50, lit("21-50")).
  when($"apply_count" >= 51 && $"apply_count" <= 253, lit("51-253")).
  otherwise($"apply_count")
).
drop($"apply_count").
groupBy($"applies").
agg(
  sum($"dv_id")
).
orderBy($"applies")


//---ADD TOOL APPLY STICKINESS FOR SU


val  crop_apply_1 = getEvent("com.picsart.studio","tool_crop_apply",from, to).
filter($"platform"===platform && $"version" >= version).
select(
	$"device_id",
	$"editor_sid",
	to_date($"timestamp").as("date")
).as("a").
join(su.as("b"), $"a.device_id" === $"b.device_id", "inner").
select($"date", $"a.device_id",$"b.cohort")


crop_apply_1.groupBy($"date",$"cohort").
agg(countDistinct($"device_id").as("DAU")).
groupBy($"cohort").
agg(
	avg($"DAU").as("DAU")
).show(false)



crop_apply_1.groupBy($"cohort").
agg(countDistinct($"device_id").as("WAU")).show(false)



////////////////////  Aspect_ratio and Lock distribution


val apply = getEvent("com.picsart.studio","tool_crop_apply",from, to).
filter($"platform"===platform && $"version" >= version).
select(
	$"aspect_ratio",$"lock", 
	$"editor_sid",
	$"device_id",
	to_date($"timestamp").as("date")
).as("a").
join(su.as("b"), $"a.device_id" === $"b.device_id", "inner").
select(
	$"a.aspect_ratio", $"a.lock", 
	$"a.date", $"b.cohort",
	$"a.editor_sid".as("apply_sid"),
	$"a.device_id"
).as("c").
join(ed_done.as("d"), $"c.apply_sid" === $"d.editor_sid", "left").
select( 
	$"c.aspect_ratio", $"c.lock",
	$"c.date", $"c.cohort",
	$"c.apply_sid",
	$"d.editor_sid"
).
groupBy($"aspect_ratio", $"lock",$"date", $"cohort").
agg(
	countDistinct($"apply_sid").as("apply_sid"),
	countDistinct($"editor_sid").as("done_sid")
).
groupBy($"aspect_ratio", $"lock", $"cohort").
agg(
	avg($"apply_sid"),
	avg($"done_sid")
)





////////////////////////////////////////////////////////

val summarize = crop_open.as("a").
join(crop_apply.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
select(
    $"a.open_date", $"a.cohort",
   	$"a.device_id".as("open_dv"),
    $"a.editor_sid".as("open_sid"),
    $"a.count".as("open_count"),
    $"b.device_id".as("apply_dv"),
    $"b.editor_sid".as("apply_sid"),
    $"b.count".as("apply_count")
).as("c").
join(ed_done.as("d"), $"c.apply_sid" === $"d.editor_sid", "left").
select(
  	$"c.open_date",
  	$"c.cohort",
 	$"c.open_dv",
 	$"c.open_sid",
 	$"c.open_count",
  	$"c.apply_dv",
  	$"c.apply_sid",
  	$"c.apply_count",
  	$"d.device_id".as("done_dv"),
  	$"d.editor_sid".as("done_sid")
).
groupBy($"open_date", $"cohort").
agg(
    countDistinct($"open_dv").as("open_dau"),
    countDistinct($"apply_dv").as("apply_dau"),
    countDistinct($"done_dv").as("done_dau"),

    countDistinct($"open_sid").as("open_sessions"),
    countDistinct($"apply_sid").as("apply_sessions"),
    countDistinct($"done_sid").as("done_sessions"),

    sum($"open_count").as("open_count"),
    sum($"apply_count").as("apply_count")
).
groupBy($"cohort").
agg(
    avg($"open_dau").as("open_dau"),
    avg($"apply_dau").as("apply_dau"),
    avg($"done_dau").as("done_dau"),
    avg($"open_sessions").as("open_sessions"),
    avg($"apply_sessions").as("apply_sessions"),
    avg($"done_sessions").as("done_sessions"),
    avg($"open_count").as("open_count"),
    avg($"apply_count").as("apply_count")
)

//////// Crop Done -> ED
val edit_done_su = getEvent("com.picsart.studio", "editor_done_click", from, to).
filter($"platform"===platform /*&& $"version" >= version*/).
select(
  $"device_id",
  to_date($"timestamp").as("date"),
  $"editor_sid"
).distinct().as("a").
join(su.as("b"), $"a.device_id" === $"b.device_id", "inner").
select(
  $"a.device_id",
  $"a.date",$"b.cohort",
  $"a.editor_sid"
)

val crop_apply_su = getEvent("com.picsart.studio", "tool_crop_apply", from, to).
filter($"platform"===platform /*&& $"version" >= version*/).
select(
  $"device_id", 
  to_date($"timestamp").as("date"), 
  $"editor_sid"
).as("a").
join(su.as("b"), $"a.device_id" === $"b.device_id", "inner").
select(
  $"a.device_id",
  $"a.editor_sid",
  $"a.date",
  $"b.cohort"
)

val crop_done_ED = edit_done_su.as("a").
join(crop_apply_su.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
select(
  $"a.cohort",
  $"a.editor_sid".as("done_sid"),
  $"b.editor_sid".as("apply_sid"),
  $"a.date"
).
groupBy($"cohort", $"date").
agg(
  countDistinct($"done_sid").as("done_sid"),
  countDistinct($"apply_sid").as("apply_sid"),
  (countDistinct($"apply_sid")/countDistinct($"done_sid")).as("ed -> apply")
).
agg(
  avg($"done_sid"),
  avg($"apply_sid"),
  avg($"ed -> apply")
)








