import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}
val formatter = DateTimeFormat.forPattern("ddMMyyyy")

//// defining super users

val from  = DateTime.parse("2018-07-01")
val to  = DateTime.parse("2018-07-31")

val countries = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/user/amber/csv/country_code.csv").
select(lower($"country_code").as("country_code"), $"country_name", $"continent")
val users = getUsers

val active_users = getActiveDevices(from, to).
filter($"app" === "com.picsart.studio").as("a").
	join(users.as("b"), $"a.user_id" === $"b.userId", "left").
	select(
		$"a.user_id", $"a.device_id", lower($"a.country_code").as("country_code"), 
		$"b.gender".as("gen_1"), 
		$"b.connections.gender".as("gen_2"),
		$"b.birthDate".as("birth_1")
	).as("a").join(countries.as("b"), $"a.country_code" === $"b.country_code", "inner").
	select(
		$"a.user_id", $"a.device_id",
		$"a.country_code",
		$"b.country_name",
		$"b.continent",
		$"a.gen_1",
		$"a.gen_2",
		$"a.birth_1",
		when($"a.country_code".isin("in","id","us", "ch","mx","ca"), $"country_name").
		when($"continent".isin(""))
	)



////////////////////////

.join(active_users.as("b"), $"a.userId" === $"b.user_id", "inner").
select(
	$"a.userId",$"a.resolvedLocation.country", 
	$"a.gender".as("gender_1"), 
	$"a.birthDate".as("birthDate_1"), 
	$"a.connections.gender".as("gender-2"), 
	$"connections.minAge".as("minAge"),
	$"connections.maxAge".as("maxAge")
)


//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 val mau = getEvent("com.picsart.studio", "app_open", from, to).
 select($"user_id").distinct()




val users = getUsers.
filter($"resolvedLocation.country" === "United States").
select(
	$"userId", to_date($"birthDate").as("birthDate"),
	$"gender".as("gen_1"),
	$"connections.gender".as("gen_2"),
	$"connections.minAge", 
	$"connections.maxAge"
).
withColumn("gen_first",
	when($"gen_1"=== 0, null).
	otherwise($"gen_1")
).
withColumn("gen_second",
	when($"gen_2"=== 0, null).
	otherwise($"gen_2")
).
withColumn("minAge_1",
	when($"minAge"=== 0, null).
	otherwise($"minAge")
).
withColumn("maxAge_1",
	when($"maxAge"=== 0, null).
	otherwise($"maxAge")
).
drop($"gen_1").
drop($"gen_2").
drop($"minAge").
drop($"maxAge").
filter($"birthDate" >= "1900-01-01" || $"birthDate".isNull).
withColumn("birthDate_1", 
	when($"birthDate".isNotNull, lit("00-00-00")).
	otherwise($"birthDate")
).
drop($"birthDate")


val valod = users.as("a").join(mau.as("b"), $"a.userId" === $"b.user_id", "inner")

valod.groupBy($"birthDate_1",$"minAge_1",$"maxAge_1").count()


valod.groupBy($"gen_first", $"gen_second").count()






