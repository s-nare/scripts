val from = DateTime.parse("2018-07-31")
val to = DateTime.parse("2018-08-06")
val platform = "android"
val version = "993000370"


val mask_open = getEvent("com.picsart.studio", "edit_item_open", from,to).
    filter($"item" === "mask" && $"platform" === platform && length(version) === 9 && $"version" >= version).
    select($"platform", $"device_id", $"editor_sid", to_date($"timestamp").as("open_date")).
    groupBy($"platform", $"device_id", $"editor_sid", $"open_date").count()

val mask_apply = getEvent("com.picsart.studio", "edit_mask_apply", from, to).
    filter($"platform" === platform && $"version" >= version).
        select($"device_id", $"editor_sid", to_date($"timestamp").as("apply_date")).
        groupBy($"device_id", $"editor_sid", $"apply_date").count()

val ed_done = getEvent("com.picsart.studio", "editor_done_click",from, to).
    filter($"platform"===platform && $"version" >= version).
    select($"device_id",$"editor_sid",to_date($"timestamp").as("done_date")).
    distinct()

////////////////////////////////////////////////////////
val summarize = mask_open.as("a").
  join(mask_apply.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
  select(
    $"a.platform",
    $"a.open_date",
    $"a.device_id".as("open_dv"),
    $"b.device_id".as("apply_dv"),
    $"a.count".as("open_count"),
    $"b.count".as("apply_count"),
    $"a.editor_sid".as("open_sid"),
    $"b.editor_sid".as("apply_sid")
  ).as("c").
  join(ed_done.as("d"),$"c.apply_sid" === $"d.editor_sid", "left").
  select(
    $"c.platform",
    $"c.open_date",
    
    $"c.open_dv",
    $"c.apply_dv",
    $"d.device_id".as("done_dv"),

    $"c.open_count",
    $"c.apply_count",
    
    $"c.open_sid",
    $"c.apply_sid",
    $"d.editor_sid".as("done_sid")
  ).
  groupBy($"open_date", $"platform").
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
  groupBy($"platform").
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


////////////////edit_mask_category_open distribution

val from = DateTime.parse("2018-06-01")
val to = DateTime.parse("2018-06-07")
val platform = "apple"
val version = "620"



val cc = getEvent("com.picsart.studio", "edit_mask_category_open", from, to).
filter($"platform" === platform && $"version" >= version).
filter($"editor_sid".isNotNull).
//filter($"editor_sid" === "0000462f-3730-4bd9-8993-daf6c34af4a2_1527937894727").
select(
  $"timestamp",
  $"platform", 
  $"editor_sid",
  $"mask_category",
  (
    when($"mask_category".in("recent", "default"), $"default_name").
    when($"mask_category" === "shop_premium",$"package_id").
    otherwise("-")
  ).as("category_name") 
).
withColumn("previous", lag($"category_name", 1).over(Window.partitionBy($"editor_sid").orderBy($"timestamp"))).
filter($"previous".isNull || ($"previous" !== $"category_name")).
drop($"previous").
groupBy($"platform", $"mask_category", $"category_name").count().
orderBy($"platform", $"mask_category", $"count".desc).
show(100, false)



/////////////// Mask_try -> Mask_apply



val from = DateTime.parse("2018-06-01")
val to = DateTime.parse("2018-06-07")
val platform = "apple"
val version = "620"


val mask_try = getEvent("com.picsart.studio", "edit_mask_try", from, to).
filter($"platform" === platform && $"version" >= version).
select(
  to_date($"timestamp").as("date"),
  $"device_id",
  $"editor_sid",
  $"mask_category",
  (
    when($"mask_category" === "default", $"mask_name").
    when($"mask_category" === "recent" && $"mask_type" === "shop_premium", $"package_id").
    when($"mask_category" === "recent" && $"mask_type" === "default", $"mask_name").
    when($"mask_category" === "shop_premium", $"package_id").
    otherwise("-")
  ).as("category_name")
).filter($"category_name".isNotNull)



mask_try.
groupBy($"date",$"mask_category",$"category_name").agg(countDistinct($"device_id").as("device")).
groupBy($"mask_category",$"category_name").agg(avg($"device")).show(100,false)





val mask_apply = getEvent("com.picsart.studio", "edit_mask_apply", from, to).
filter($"platform" === platform && $"version" >= version).
select(
  to_date($"timestamp").as("date"),
  $"device_id",
  $"editor_sid",
  $"mask_category",
  (
    when($"mask_category" === "default", $"mask_name").
    when($"mask_category" === "recent" && $"mask_type"=== "shop_premium", $"package_id").
    when($"mask_category" === "recent" && $"mask_type" === "default", $"mask_name").
    when($"mask_category" === "shop_premium", $"package_id").
    otherwise("-")
  ).as("category_name")
).filter($"category_name".isNotNull)





val try_apply = mask_try.as("a").
join(mask_apply.as("b"), 
  $"a.editor_sid" === $"b.editor_sid" && 
  $"a.category_name" === $"b.category_name", "left"
).
select(
  $"a.date".as("try_date"),
  $"a.device_id".as("try_dv"),
  $"b.device_id".as("apply_dv"),
  $"a.editor_sid".as("try_sid"),
  $"b.editor_sid".as("apply_sid"),
  $"a.mask_category".as("try_mask_category"),
  $"a.category_name".as("try_category_name")
).
groupBy($"try_date", $"try_mask_category",$"try_category_name").
agg(
  countDistinct($"try_sid").as("try_sid"),
  countDistinct($"apply_sid").as("apply_sid")
).
groupBy($"try_mask_category",$"try_category_name").
agg(
  avg($"try_sid").as("try_sid"),
  avg($"apply_sid").as("apply_sid")
)




///////////////////subscription


val sub_offer_open = getEvent("com.picsart.studio", "subscription_offer_open", from, to).
filter($"platform" === platform && $"version" >= version && $"source".like("editor_add_mask%")).
select($"sub_sid").distinct()


val sub_button_click = getEvent("com.picsart.studio", "subscription_button_click", from, to).
filter($"platform" === platform && $"version" >= version ).
select($"sub_sid").distinct()


val sub_done = getEvent("com.picsart.studio", "subscription_done", from, to).
filter($"platform" === platform && $"version" >= version).
select($"sub_sid").distinct()



val sub = sub_offer_open.as("a").join(sub_button_click.as("b"), $"a.sub_sid" === $"b.sub_sid", "left").
select(
  $"a.sub_sid".as("offer_open_sid"),
  $"b.sub_sid".as("button_sid")
).as("c").join(sub_done.as("d"), $"c.button_sid" === $"d.sub_sid", "left").
select(
  $"c.offer_open_sid",
  $"c.button_sid",
  $"d.sub_sid".as("done_sid")
).agg(
    countDistinct($"offer_open_sid"),
    countDistinct($"button_sid"),
    countDistinct($"done_sid")
  )

////////////////stickiness DAU/MAU



val mask_apply_1 = getEvent("com.picsart.studio", "edit_mask_apply", from, to).
filter($"platform" === platform && $"version" <= version).
select($"device_id", $"editor_sid", to_date($"timestamp").as("apply_date"))

mask_apply_1.groupBy($"apply_date").
agg(
  countDistinct($"device_id").as("apply_dau")
).
agg(
  avg($"apply_dau")
).show(false)



mask_apply_1.
agg(
  countDistinct($"device_id").as("wau")
).show(false)











