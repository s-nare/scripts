

//// experiment_participate

val from = DateTime.parse("2018-05-07")
val to = DateTime.parse("2018-05-13")

val exp = getEvent("com.picsart.studio","experiment_participate",from,to).
    filter($"platform"==="android" && $"experiment_id"==="376a").
    select($"device_id", $"variant", to_date($"timestamp").as("exp_date")).
    groupBy($"variant", $"device_id").agg(min($"exp_date").as("exp_date"))

exp.groupBy($"variant").agg(countDistinct($"device_id").as("devices")).orderBy($"variant").show(false)
exp.groupBy($"exp_date").pivot("variant").agg(countDistinct($"device_id")).orderBy($"exp_date").show(100, false)

// cohort_definition /////////////////////////////////////////////////////////////////////////////////////////////////////////////

val fromX = DateTime.parse("2018-04-06")
val toX = DateTime.parse("2018-05-06")

val done  = getEvent("com.picsart.studio","editor_done_click", fromX, toX).
    filter($"platform" === "android").
    select($"device_id", to_date($"timestamp").as("date")).
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
    drop($"b.device_id").show(100,false)

users.groupBy($"variant").pivot("cohort").agg(countDistinct($"device_id").as("devices")).orderBy($"variant").show(false)

// ED DAU / EO DAU && ED / ED DAU ////////////////////////////////////////////////////////////////////////////////////////////////

val from = DateTime.parse("2018-05-07")
val to = DateTime.parse("2018-05-13")


val editor_open = getEvent("com.picsart.studio", "editor_open", from, to).
    filter($"platform" === "android").
    select($"device_id", to_date($"timestamp").as("date")).distinct

val editor_done = getEvent("com.picsart.studio", "editor_done_click", from, to).
    filter($"platform" === "apple").
    groupBy($"device_id", to_date($"timestamp").as("date")).count

val ed_open_per_cohort = users.as("a").
    join(editor_open.as("b"), ($"a.device_id" === $"b.device_id") && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id")

val ed_done_open_per_cohort = ed_open_per_cohort.as("a").
    join(editor_done.as("b"), $"a.device_id" === $"b.device_id" && $"a.date" === $"b.date", "left").
    select($"a.variant", $"a.cohort", $"a.date", $"a.device_id".as("open_d"), $"b.device_id".as("done_d"), $"b.count").
    groupBy($"variant", $"date", $"cohort").agg(
        countDistinct($"open_d").as("open_dau"),
        countDistinct($"done_d").as("done_dau"),
        sum($"count").as("dones")).
    orderBy($"variant", $"cohort", $"date")

// Tool Apply Stikiness ////////////////////////////////////////////////////////////////////////////////////////////////

/// for original

val from = DateTime.parse("2018-04-11")
val to = DateTime.parse("2018-04-30")

val effect_apply = getEvent("com.picsart.studio", "effect_apply", from, to).
    filter($"platform" === "apple").
    filter($"category_name" === "corrections").
    select($"device_id", to_date($"timestamp").as("date")).distinct

val effect_apply_per_cohort = users.as("a").
    join(effect_apply.as("b"), ($"a.device_id" === $"b.device_id") && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id").
    groupBy($"variant", $"cohort", $"date").agg(countDistinct($"device_id").as("dau")).
    groupBy($"variant", $"cohort").agg(avg($"dau"))
    
val effect_apply_per_cohort_whole_period = users.as("a").
    join(effect_apply.as("b"), ($"a.device_id" === $"b.device_id") && ($"a.exp_date" <= $"b.date"), "inner").
    groupBy($"variant", $"cohort").agg(countDistinct($"a.device_id"))

//// for beautify

val from = DateTime.parse("2018-04-11")
val to = DateTime.parse("2018-04-30")

val edit_beautify_apply = getEvent("com.picsart.studio", "edit_beautify_apply", from, to).
    filter($"platform" === "apple").
    select($"device_id", to_date($"timestamp").as("date")).distinct

val edit_beautify_apply_cohort = users.as("a").
    join(edit_beautify_apply.as("b"), ($"a.device_id" === $"b.device_id") && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id").
    groupBy($"variant", $"cohort", $"date").agg(countDistinct($"device_id").as("dau")).
    groupBy($"variant", $"cohort").agg(avg($"dau"))
    
val edit_beautify_apply_per_cohort_whole_period = users.as("a").
    join(edit_beautify_apply.as("b"), ($"a.device_id" === $"b.device_id") && ($"a.exp_date" <= $"b.date"), "inner").
    groupBy($"variant", $"cohort").agg(countDistinct($"a.device_id"))

// Tool Open Distribution ///////////////////////////////////////////////////////////////////////////////////////////////////////

// for original

val from = DateTime.parse("2018-04-11")
val to = DateTime.parse("2018-04-30")

val effect_category_open = getEvent("com.picsart.studio", "effect_category_open", from, to).
    filter($"platform" === "apple").
    filter($"category_name" === "corrections").
    groupBy($"device_id", to_date($"timestamp").as("date")).count

val effect_category_open_per_cohort = users.as("a").
    join(effect_category_open.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id", $"count").
    groupBy($"variant", $"cohort", $"date", $"count".as("apply_count")).agg(countDistinct($"device_id").as("users")).
    groupBy($"variant", $"apply_count").pivot("cohort").agg(avg($"users").as("users")).
    orderBy($"variant", $"apply_count")

// for beautify

val from = DateTime.parse("2018-04-11")
val to = DateTime.parse("2018-04-30")

val edit_beautify_open = getEvent("com.picsart.studio", "edit_beautify_open", from, to).
    filter($"platform" === "apple").
    groupBy($"device_id", to_date($"timestamp").as("date")).count

val edit_beautify_open_per_cohort = users.as("a").
    join(edit_beautify_open.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id", $"count").
    groupBy($"variant", $"cohort", $"date", $"count".as("apply_count")).agg(countDistinct($"device_id").as("users")).
    groupBy($"variant", $"apply_count").pivot("cohort").agg(avg($"users").as("users")).
    orderBy($"variant", $"apply_count")

// Tool Apply Distribution //////////////////////////////////////////////////////////////////////////////////////////////////////

// for original

val from = DateTime.parse("2018-04-11")
val to = DateTime.parse("2018-04-30")

val effect_apply = getEvent("com.picsart.studio", "effect_apply", from, to).
    filter($"platform" === "apple").
    filter($"category_name" === "corrections").
    groupBy($"device_id", to_date($"timestamp").as("date")).count

val effect_apply_per_cohort = users.as("a").
    join(effect_apply.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id", $"count").
    groupBy($"variant", $"cohort", $"date", $"count".as("apply_count")).agg(countDistinct($"device_id").as("users")).
    groupBy($"variant", $"apply_count").pivot("cohort").agg(avg($"users").as("users")).
    orderBy($"variant", $"apply_count")

// for beautify

val from = DateTime.parse("2018-04-11")
val to = DateTime.parse("2018-04-30")

val edit_beautify_apply = getEvent("com.picsart.studio", "edit_beautify_apply", from, to).
    filter($"platform" === "apple").
    groupBy($"device_id", to_date($"timestamp").as("date")).count

val edit_beautify_apply_per_cohort = users.as("a").
    join(edit_beautify_apply.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id", $"count").
    groupBy($"variant", $"cohort", $"date", $"count".as("apply_count")).agg(countDistinct($"device_id").as("users")).
    groupBy($"variant", $"apply_count").pivot("cohort").agg(avg($"users").as("users")).
    orderBy($"variant", $"apply_count")

// TD DAU / TO DAU //////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// original

val from = DateTime.parse("2018-04-11")
val to = DateTime.parse("2018-04-30")

val effect_category_open = getEvent("com.picsart.studio", "effect_category_open", from, to).
    filter($"platform" === "apple").
    filter($"category_name" === "corrections").
    select(to_date($"timestamp").as("date"), $"device_id").distinct

val effect_category_open_per_cohort = users.as("a").
    join(effect_category_open.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id").distinct

val effect_apply = getEvent("com.picsart.studio", "effect_apply", from, to).
    filter($"platform" === "apple").
    filter($"category_name" === "corrections").
    select($"device_id").distinct

val td_dau_to_dau = effect_category_open_per_cohort.as("a").
    join(effect_apply.as("b"), $"a.device_id" === $"b.device_id", "left").
    select($"a.variant", $"a.cohort", $"a.date", $"a.device_id".as("open_id"), $"b.device_id".as("apply_id")).
    groupBy($"variant", $"date", $"cohort").agg(
        countDistinct($"open_id").as("open_id"),
        countDistinct($"apply_id").as("apply_id")).
    groupBy($"variant", $"cohort").
        agg(avg($"open_id").as("open_id"), avg($"apply_id").as("apply_id")).
    orderBy($"variant", $"cohort")

//// beautify

val from = DateTime.parse("2018-04-11")
val to = DateTime.parse("2018-04-30")

val edit_beautify_open = getEvent("com.picsart.studio", "edit_beautify_open", from, to).
    filter($"platform" === "apple").
    select(to_date($"timestamp").as("date"), $"device_id").distinct

val edit_beautify_open_per_cohort = users.as("a").
    join(edit_beautify_open.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id").distinct

val edit_beautify_apply = getEvent("com.picsart.studio", "edit_beautify_apply", from, to).
    filter($"platform" === "apple").
    select($"device_id").distinct

val td_dau_to_dau = edit_beautify_open_per_cohort.as("a").
    join(edit_beautify_apply.as("b"), $"a.device_id" === $"b.device_id", "left").
    select($"a.variant", $"a.cohort", $"a.date", $"a.device_id".as("open_id"), $"b.device_id".as("apply_id")).
    groupBy($"variant", $"date", $"cohort").agg(
        countDistinct($"open_id").as("open_id"),
        countDistinct($"apply_id").as("apply_id")).
    groupBy($"variant", $"cohort").
        agg(avg($"open_id").as("open_id"), avg($"apply_id").as("apply_id")).
    orderBy($"variant", $"cohort")

// TD / TD DAU //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// original

val from = DateTime.parse("2018-04-11")
val to = DateTime.parse("2018-04-30")

val effect_apply = getEvent("com.picsart.studio", "effect_apply", from, to).
    filter($"platform" === "apple").
    filter($"category_name" === "corrections").
    groupBy(to_date($"timestamp").as("date"), $"device_id").count

val effect_apply_per_cohort = users.as("a").
    join(effect_apply.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id", $"count").
    groupBy($"variant", $"cohort").agg(countDistinct($"device_id").as("dau"), sum($"count").as("events"))

//// beautify

val from = DateTime.parse("2018-04-11")
val to = DateTime.parse("2018-04-30")

val edit_beautify_apply = getEvent("com.picsart.studio", "edit_beautify_apply", from, to).
    filter($"platform" === "apple").
    groupBy(to_date($"timestamp").as("date"), $"device_id").count

val edit_beautify_apply_per_cohort = users.as("a").
    join(edit_beautify_apply.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id", $"count").
    groupBy($"variant", $"cohort").agg(countDistinct($"device_id").as("dau"), sum($"count").as("events"))

// Apply -> ED //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// original

val from = DateTime.parse("2018-04-11")
val to = DateTime.parse("2018-04-30")

val effect_apply = getEvent("com.picsart.studio", "effect_apply", from, to).
    filter($"platform" === "apple").
    filter($"category_name" === "corrections").
    select($"device_id", to_date($"timestamp").as("date"), $"editor_sid").distinct

val effect_apply_per_cohort = users.as("a").
    join(effect_apply.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"editor_sid").distinct

val editor_done_click = getEvent("com.picsart.studio", "editor_done_click", from, to).
    filter($"platform" === "apple").
    select($"editor_sid").distinct

val apply_done = effect_apply_per_cohort.as("a").
    join(editor_done_click.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
    groupBy($"variant", $"cohort").
    agg(countDistinct($"a.editor_sid").as("apply_sid"), countDistinct($"b.editor_sid").as("done_id"))

//// beautify

val edit_beautify_apply = getEvent("com.picsart.studio", "edit_beautify_apply", from, to).
    filter($"platform" === "apple").
    select($"device_id", to_date($"timestamp").as("date"), $"editor_sid").distinct

val edit_beautify_apply_per_cohort = users.as("a").
    join(edit_beautify_apply.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"editor_sid").distinct

val editor_done_click = getEvent("com.picsart.studio", "editor_done_click", from, to).
    filter($"platform" === "apple").
    select($"editor_sid").distinct

val apply_done = edit_beautify_apply_per_cohort.as("a").
    join(editor_done_click.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
    groupBy($"variant", $"cohort").
    agg(countDistinct($"a.editor_sid").as("apply_sid"), countDistinct($"b.editor_sid").as("done_id"))

// ED -> Apply //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// original

val from = DateTime.parse("2018-04-11")
val to = DateTime.parse("2018-04-30")

val editor_done_click = getEvent("com.picsart.studio", "editor_done_click", from, to).
    filter($"platform" === "apple").
    select($"device_id", to_date($"timestamp").as("date"), $"editor_sid").distinct

val ed_per_cohort = users.as("a").
    join(editor_done_click.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"editor_sid").distinct

val effect_apply = getEvent("com.picsart.studio", "effect_apply", from, to).
    filter($"platform" === "apple").
    filter($"category_name" === "corrections").
    select($"editor_sid").distinct

val apply_done = ed_per_cohort.as("a").
    join(effect_apply.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
    groupBy($"variant", $"cohort").
    agg(countDistinct($"a.editor_sid").as("apply_sid"), countDistinct($"b.editor_sid").as("done_id"))

//// beautify

val from = DateTime.parse("2018-04-11")
val to = DateTime.parse("2018-04-30")

val editor_done_click = getEvent("com.picsart.studio", "editor_done_click", from, to).
    filter($"platform" === "apple").
    select($"device_id", to_date($"timestamp").as("date"), $"editor_sid").distinct

val ed_per_cohort = users.as("a").
    join(editor_done_click.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"editor_sid").distinct

val edit_beautify_apply = getEvent("com.picsart.studio", "edit_beautify_apply", from, to).
    filter($"platform" === "apple").
    select($"editor_sid").distinct

val apply_done = ed_per_cohort.as("a").
    join(edit_beautify_apply.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
    groupBy($"variant", $"cohort").
    agg(countDistinct($"a.editor_sid").as("apply_sid"), countDistinct($"b.editor_sid").as("done_id"))

// EA DAU / EO DAU && EA / EO DAU ///////////////////////////////////////////////////////////////////////////////////////////////

val from = DateTime.parse("2018-04-11")
val to = DateTime.parse("2018-04-30")

val edit_item_open = getEvent("com.picsart.studio", "edit_item_open", from, to).
    filter($"item" === "effects").
    filter($"platform" === "apple").
    select($"device_id", to_date($"timestamp").as("date")).distinct

val effect_apply = getEvent("com.picsart.studio", "effect_apply", from, to).
    filter($"platform" === "apple").
    groupBy($"device_id", to_date($"timestamp").as("date")).count

val effect_open_per_cohort = users.as("a").
    join(effect_open.as("b"), ($"a.device_id" === $"b.device_id") && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id")

val effect_apply_open_per_cohort = effect_open_per_cohort.as("a").
    join(effect_apply.as("b"), $"a.device_id" === $"b.device_id" && $"a.date" === $"b.date", "left").
    select($"a.variant", $"a.cohort", $"a.date", $"a.device_id".as("open_d"), $"b.device_id".as("done_d"), $"b.count").
    groupBy($"variant", $"date", $"cohort").agg(
        countDistinct($"open_d").as("open_dau"),
        countDistinct($"done_d").as("done_dau"),
        sum($"count").as("dones")).
    groupBy($"variant", $"cohort").
    agg(avg($"open_dau").as("open_dau"), avg($"done_dau").as("apply_dau"), avg($"dones").as("done")).
    orderBy($"variant", $"cohort")

////// funnel ///////////////////////////////////////////////////////////////////////////////////////////////

val from = DateTime.parse("2018-04-11")
val to = DateTime.parse("2018-04-30")

val edit_item_open = getEvent("com.picsart.studio", "edit_item_open", from, to).
    filter($"platform" === "apple").
    filter($"item" === "effects").
    select($"device_id", to_date($"timestamp").as("date"), $"editor_sid").distinct

val edit_item_open_per_cohort = users.as("a").
    join(edit_item_open.as("b"), ($"a.device_id" === $"b.device_id") && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"b.editor_sid").distinct

val popup_open = getEvent("com.picsart.studio", "popup_open", from, to).
    filter($"platform" === "apple").
    filter($"id" === "editor_home_beutify_v1").
    select($"source_sid", $"timestamp").distinct

val beautify_open = getEvent("com.picsart.studio", "edit_item_open", from, to).
    filter($"platform" === "apple").
    filter($"item" === "beautify").
    select($"editor_sid", $"timestamp").distinct

edit_item_open_per_cohort.as("a").
    join(popup_open.as("b"), $"a.editor_sid" === $"b.source_sid", "left").
    select($"variant", $"cohort", $"date", $"editor_sid".as("effects_open"), $"source_sid".as("popup_open"), $"b.timestamp").as("c").
        join(beautify_open.as("d"), $"c.popup_open" === $"d.editor_sid" && $"c.timestamp" <= $"d.timestamp", "left").
        select($"variant", $"cohort", $"date", $"effects_open", $"popup_open", $"d.editor_sid".as("beautify_open")).
        groupBy($"variant", $"cohort", $"date").agg(countDistinct($"effects_open").as("effects_open"), countDistinct($"popup_open").as("popup_open"), countDistinct($"beautify_open").as("beautify_open")).
        groupBy($"variant", $"cohort").agg(avg($"effects_open").as("effects_open"), avg($"popup_open").as("popup_open"), avg($"beautify_open").as("beautify_open"))

