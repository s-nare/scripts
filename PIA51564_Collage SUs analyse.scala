

val to = DateTime.parse("2018-04-29")
val from = DateTime.parse("2018-04-23")

val toX = DateTime.parse("2018-04-22")
val fromX = DateTime.parse("2018-03-22")
val done  = getEvent("com.picsart.studio","editor_done_click",fromX,toX).
    select($"device_id",to_date($"timestamp").as("date"),$"platform").
    groupBy($"device_id",$"platform").agg(countDistinct($"date").as("active_days")).
    withColumn("cohort",
        when($"active_days" >= 13, lit("active")).
        when($"active_days" <= 3 && $"active_days" >= 1, lit("passive")).
        otherwise(lit("-"))
    ).
    drop($"active_days").
    filter(!($"cohort" === "-"))

val grid_click = getEvent("com.picsart.studio","collage_grid_apply",from,to).//filter($"click_type"==="grid").
	select(
		$"platform",
		$"device_id",
		to_date($"timestamp").as("date"),
		$"collage_name",
		$"collage_session_id",
		$"layout").
	as("a").
	join(
		done.as("b"),
		$"a.device_id"=== $"b.device_id" && 
		$"a.platform"=== $"b.platform", "inner").
	select(
		$"a.platform",
		$"collage_name",
		$"collage_session_id",
		$"a.layout",
		$"a.device_id",
		$"a.date",
		$"b.cohort").
	withColumn("new_collage", split($"collage_name","\\/")).
			select($"device_id",$"collage_session_id",$"date",$"platform",$"collage_name", $"layout", $"cohort",
				$"new_collage".getItem(0).as("col1"),
				$"new_collage".getItem(1).as("col2")).drop("new_collage").
	withColumn("new_collage_name", 
				when($"platform"==="android",$"col2"). 
				when($"platform"==="apple", $"col1").
				otherwise("-"))




///stickiness by collage_name///////////////////////////////////////////////////////////////////////////////////////
val total = grid_click.groupBy($"platform",$"new_collage_name", $"layout", $"cohort").agg(countDistinct($"device_id").as("total_devices"))
val daily =	grid_click.groupBy($"platform",$"new_collage_name", $"layout", $"cohort",$"date").agg(countDistinct($"device_id").as("a"), countDistinct($"collage_session_id").as("sessions")).
			groupBy($"platform",$"new_collage_name", $"layout", $"cohort").agg(avg($"a").as("avg_devices"), avg($"sessions").as("grid_sessions"))
		
	
val result = total.as("a").join(daily.as("b"),
	$"a.platform"=== $"b.platform" && 
	$"a.new_collage_name"=== $"b.new_collage_name" && 
	$"a.layout"=== $"b.layout" && 
	$"a.cohort" === $"b.cohort","inner").
select($"a.platform", 
	$"a.new_collage_name",
	$"a.layout",
	$"a.cohort",
	$"a.total_devices", ($"b.avg_devices"/ $"a.total_devices").as("stickiness"), $"grid_sessions").
orderBy($"platform", 
	$"cohort",
	$"new_collage_name",
	$"layout")


result.show(5000, false)


val album = getEvent("com.picsart.studio","create_collage_item_click", from, to).
    filter(($"item"==="grid") && ($"create_session_id".isNotNull)).
    select($"create_session_id").distinct()


val photo_click = getEvent("com.picsart.studio","photo_chooser_photo_click",from,to).
filter($"pc_sid".isNotNull).
    select($"source", $"pc_sid", $"position".cast("int"), $"platform", $"device_id").
    withColumn("position_group",
        when($"position" >= 0 && $"position" <= 100, $"position").
        when($"position" > 100 && $"position" <= 200, lit("101-200")).
        when($"position" > 200 && $"position" <= 300, lit("201-300")).
        when($"position" > 300 && $"position" <= 400, lit("301-400")).
        when($"position" > 400 && $"position" <= 500, lit("401-500")).
        when($"position" > 500 && $"position" <= 1000, lit("501-1000")).
        when($"position" > 1000 && $"position" <= 5000, lit("1000-5000")).
        when($"position" > 5000 && $"position" <= 10000, lit("5001-10000")).
        when($"position" > 10000, lit("10000+")).
        otherwise(lit("-"))
    ).
    select($"source", $"pc_sid", $"position_group", $"platform", $"device_id")
   



val all = photo_click.as("a").
	join(album.as("b"),$"a.pc_sid"=== $"b.create_session_id","inner").
	select($"source", $"position_group", $"platform", $"device_id", $"a.pc_sid").as("c").
	join(done.as("d"), $"c.device_id" === $"d.device_id", "inner" ).
		select($"position_group",$"source",$"platform",$"cohort",$"c.pc_sid").
		groupBy($"source", $"platform", $"cohort", $"position_group").agg(countDistinct($"pc_sid").as("pc_sessions"))

all.orderBy($"source", $"platform", $"cohort", $"position_group").show(5000,false)





















