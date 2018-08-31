val toX = yesterday()//DateTime.parse("2018-07-29")
val fromX  = toX.minusDays(40)
val version_condition = "platform = 'apple' AND LENGTH(version) = 3 AND (version <= '799' OR version >= '200')"
val platform = "apple"

var countries = Array("BR","CA","CN","FR","DE","IN","ID","IT","JP","MY","MX","KR","ES","TR","GB","US","VN", "AU","AE","AT","CH","FI","HK","NL","NO","PR","PT","SE","SG", "JP", "CN", "US", "MX", "KR", "AM", "RU", "VN", "BR", "DE", "IT", "IN", "ID", "TR", "ES", "AR", "MY", "UK", "PH", "FR", "CO", "CA")
val p1 = getEvent("com.picsart.studio", "tabbar_click", fromX, toX).
    filter(version_condition).filter($"tab" === "main_menu").
    select(to_date($"timestamp").as("date"), $"session_id".as("ss"), $"device_id".as("dd"), $"timestamp".as("tt"), upper($"country_code").as("country"), $"version".as("vv"), $"create_session_id".as("ss2")).
    withColumn("cc", when($"country".isin(countries:_*), $"country").otherwise("other"))
val p2 = getEvent("com.picsart.studio", "create_item_click", fromX, toX).
    filter(version_condition).filter($"item" === "edit").
    select($"session_id".as("ss"), $"device_id".as("dd"), $"timestamp".as("tt"), upper($"country_code").as("country"), $"version".as("vv"), $"create_session_id".as("ss2")).
    withColumn("cc", when($"country".isin(countries:_*), $"country").otherwise("other"))
val p3 = getEvent("com.picsart.studio", "photo_chooser_open", fromX, toX).
    filter(version_condition).
    select($"session_id".as("ss"), $"device_id".as("dd"), $"timestamp".as("tt"), upper($"country_code").as("country"), $"version".as("vv"), $"pc_sid".as("ss2")).
    withColumn("cc", when($"country".isin(countries:_*), $"country").otherwise("other"))
val p4 = getEvent("com.picsart.studio", "photo_chooser_photo_click", fromX, toX).
    filter(version_condition).
    select($"session_id".as("ss"), $"device_id".as("dd"), $"timestamp".as("tt"), upper($"country_code").as("country"), $"version".as("vv"), $"pc_sid".as("ss2")).
    withColumn("cc", when($"country".isin(countries:_*), $"country").otherwise("other"))
val p5 = getEvent("com.picsart.studio", "editor_open", fromX, toX).
    filter(version_condition).
    select($"session_id".as("ss"), $"device_id".as("dd"), $"timestamp".as("tt"), upper($"country_code").as("country"), $"version".as("vv"), $"create_session_id".as("ss2")).
    withColumn("cc", when($"country".isin(countries:_*), $"country").otherwise("other"))

p1.as("a").
    join(p2.as("b"), $"a.ss2" === $"b.ss2" && $"a.tt" < $"b.tt", "left").
    select($"a.date", $"a.ss", $"a.dd", $"a.cc", $"a.vv", $"a.ss2".as("p1"), $"b.ss2".as("p2"), $"b.tt").as("a").
        join(p3.as("b"), $"a.p2" === $"b.ss2" && $"a.tt" < $"b.tt", "left").
        select($"a.date", $"a.ss", $"a.dd", $"a.cc", $"a.vv", $"a.p1", $"a.p2", $"b.ss2".as("p3"), $"b.tt").as("a").
            join(p4.as("b"), $"a.p3" === $"b.ss2" && $"a.tt" < $"b.tt", "left").
            select($"a.date", $"a.ss", $"a.dd", $"a.cc", $"a.vv", $"a.p1", $"a.p2", $"a.p3", $"b.ss2".as("p4"), $"b.tt").as("a").
                    join(p5.as("b"), $"a.p4" === $"b.ss2" && $"a.tt" < $"b.tt", "left").
                    select($"a.date", $"a.ss", $"a.dd", $"a.cc", $"a.vv", $"a.p1", $"a.p2", $"a.p3", $"a.p4", $"b.ss2".as("p5")).
    groupBy($"date", $"cc", $"vv").agg(
        countDistinct($"p1").as("p1"),
        countDistinct($"p2").as("p2"),
        countDistinct($"p3").as("p3"),
        countDistinct($"p4").as("p4"),
        countDistinct($"p5").as("p5")
    ).
    orderBy($"date", $"cc", $"vv").
    show(10000, false)







