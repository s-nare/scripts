val from = DateTime.parse("2018-06-26")
val to = DateTime.parse("2018-07-26")


val app_open = getEvent("com.picsart.draw", "app_open", from, to).
filter($"platform" === "apple").
select($"device_id", to_date($"timestamp").as("date")).distinct()


val color_draw = getDevices.filter($"app" === "com.picsart.draw").select($"device_id", $"os_version").distinch()

val join= app_open.as("a").join(color_draw.as("b"), $"a.device_id" === $"b.device_id", "inner").
select($"b.os_version", $"b.device_id").
groupBy($"os_version").agg(countDistinct($"device_id").as("devices")).
orderBy($"devices".desc)