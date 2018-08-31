

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
    drop($"b.device_id")




users.select($"variant",$"cohort",$"a.device_id",$"exp_date").
    groupBy($"variant",$"cohort",$"exp_date").agg(countDistinct($"device_id").as("devices")).
    groupBy($"variant",$"cohort").agg(avg($"devices"))


// ED DAU / EO DAU && ED / ED DAU ////////////////////////////////////////////////////////////////////////////////////////////////

val from = DateTime.parse("2018-05-07")
val to = DateTime.parse("2018-05-13")


val editor_open = getEvent("com.picsart.studio", "editor_open", from, to).
    filter($"platform" === "android").
    select($"device_id", to_date($"timestamp").as("date")).distinct

val editor_done = getEvent("com.picsart.studio", "editor_done_click", from, to).
    filter($"platform" === "android").
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
    orderBy($"variant", $"cohort", $"date").
    groupBy($"variant", $"cohort").
    agg(
        avg($"open_dau").as("open_dau"), 
        avg($"done_dau").as("done_dau"),
        avg($"dones").as("dones"),
        (avg($"done_dau")/avg($"open_dau")).as("done_dau/open_dau"),
        (avg($"dones")/avg($"done_dau")).as("done/done_dau")
    )

// Tool Apply Stikiness ////////////////////////////////////////////////////////////////////////////////////////////////

/// for original

val from = DateTime.parse("2018-05-07")
val to = DateTime.parse("2018-05-13")

val effect_apply = getEvent("com.picsart.studio", "effect_apply", from, to).
    filter($"platform" === "android").
    filter($"category_name" === "corrections").
    select($"device_id", to_date($"timestamp").as("date"),$"editor_sid").distinct

val effect_apply_per_cohort = users.filter($"variant"==="original").as("a").
    join(effect_apply.as("b"), ($"a.device_id" === $"b.device_id") && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id", $"b.editor_sid")


    effect_apply_per_cohort.
    	groupBy($"variant", $"cohort", $"date").agg(countDistinct($"device_id").as("dau")).
   		groupBy($"variant", $"cohort").agg(avg($"dau")).show(false)

    effect_apply_per_cohort.
    	groupBy($"variant", $"cohort").agg(countDistinct($"a.device_id")).show(false)
    


//// for beautify

val from = DateTime.parse("2018-05-07")
val to = DateTime.parse("2018-05-13")

val edit_beautify_apply = getEvent("com.picsart.studio", "edit_beautify_apply", from, to).
    filter($"platform" === "android").
    select($"device_id", to_date($"timestamp").as("date")).distinct

val edit_beautify_apply_cohort = users.filter($"variant"==="beautify").as("a").
    join(edit_beautify_apply.as("b"), ($"a.device_id" === $"b.device_id") && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id")

    edit_beautify_apply_cohort.
    	groupBy($"variant", $"cohort", $"date").agg(countDistinct($"device_id").as("dau")).
    	groupBy($"variant", $"cohort").agg(avg($"dau")).show(false)


    edit_beautify_apply_cohort.
    	groupBy($"variant", $"cohort").agg(countDistinct($"a.device_id")).show(false)



// TD DAU / TO DAU //////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// original

val from = DateTime.parse("2018-05-07")
val to = DateTime.parse("2018-05-13")

val effect_category_open = getEvent("com.picsart.studio", "effect_category_open", from, to).
    filter($"platform" === "android").
    filter($"category_name" === "corrections").
    select(to_date($"timestamp").as("date"), $"device_id").distinct

val effect_category_open_per_cohort = users.filter($"variant"==="original").as("a").
    join(effect_category_open.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id").distinct

val effect_apply = getEvent("com.picsart.studio", "effect_apply", from, to).
    filter($"platform" === "android").
    filter($"category_name" === "corrections").
    select($"device_id", to_date($"timestamp").as("date")).
    groupBy($"device_id", $"date").count().
    select($"device_id", $"date", $"count".as("applies"))

val td_dau_to_dau = effect_category_open_per_cohort.as("a").
    join(effect_apply.as("b"), $"a.device_id" === $"b.device_id" && $"a.date" === $"b.date", "left").
    select($"a.variant", $"a.cohort", $"a.date", $"a.device_id".as("open_id"), $"b.device_id".as("apply_id"), $"b.applies")




    td_dau_to_dau.
    groupBy($"variant", $"date", $"cohort").agg(
        countDistinct($"open_id").as("open_id"),
        countDistinct($"apply_id").as("apply_id"),
        sum($"applies").as("applies")).
    groupBy($"variant", $"cohort").
        agg(avg($"open_id").as("open_id"), avg($"apply_id").as("apply_id"),avg($"applies"),(avg($"apply_id")/avg($"open_id")).as("apply_DAU / open_DAU")).
    orderBy($"variant", $"cohort").show(false)

//// beautify

val from = DateTime.parse("2018-05-07")
val to = DateTime.parse("2018-05-13")

val edit_beautify_open = getEvent("com.picsart.studio", "edit_beautify_open", from, to).
    filter($"platform" === "android").
    select(to_date($"timestamp").as("date"), $"device_id").distinct

val edit_beautify_open_per_cohort = users.filter($"variant"==="beautify").as("a").
    join(edit_beautify_open.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id").distinct

val edit_beautify_apply = getEvent("com.picsart.studio", "edit_beautify_apply", from, to).
    filter($"platform" === "android").
    select($"device_id",to_date($"timestamp").as("date")).
    groupBy($"device_id", $"date").count().
    select($"device_id", $"date", $"count".as("applies"))

val td_dau_to_dau = edit_beautify_open_per_cohort.as("a").
    join(edit_beautify_apply.as("b"), $"a.device_id" === $"b.device_id" && $"a.date"=== $"b.date", "left").
    select($"a.variant", $"a.cohort", $"a.date", $"a.device_id".as("open_id"), $"b.device_id".as("apply_id"), $"b.applies")



    td_dau_to_dau.
    groupBy($"variant", $"date", $"cohort").agg(
        countDistinct($"open_id").as("open_id"),
        countDistinct($"apply_id").as("apply_id"),
        sum($"applies").as("applies")).
    groupBy($"variant", $"cohort").
        agg(avg($"open_id").as("open_id"), avg($"apply_id").as("apply_id"),avg($"applies"),(avg($"apply_id")/avg($"open_id")).as("apply_DAU / open_DAU")).
    orderBy($"variant", $"cohort").show(false)



// Apply -> ED //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// original

val from = DateTime.parse("2018-05-07")
val to = DateTime.parse("2018-05-13")


val effect_apply = getEvent("com.picsart.studio", "effect_apply", from, to).
    filter($"platform" === "android").
    filter($"category_name" === "corrections").
    select($"device_id", to_date($"timestamp").as("date"), $"editor_sid").distinct

val effect_apply_per_cohort = users.filter($"variant"==="original").as("a").
    join(effect_apply.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"editor_sid").distinct

val editor_done_click = getEvent("com.picsart.studio", "editor_done_click", from, to).
    filter($"platform" === "android").
    select($"editor_sid").distinct

val apply_done = effect_apply_per_cohort.as("a").
    join(editor_done_click.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
    groupBy($"variant", $"cohort").
    agg(
    	countDistinct($"a.editor_sid").as("apply_sid"), 
    	countDistinct($"b.editor_sid").as("done_id"), 
    	(countDistinct($"b.editor_sid")/countDistinct($"a.editor_sid")) .as("apply->done")
    )

//// beautify

val edit_beautify_apply = getEvent("com.picsart.studio", "edit_beautify_apply", from, to).
    filter($"platform" === "android").
    select($"device_id", to_date($"timestamp").as("date"), $"editor_sid").distinct

val edit_beautify_apply_per_cohort = users.filter($"variant"==="beautify").as("a").
    join(edit_beautify_apply.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"editor_sid").distinct

val editor_done_click = getEvent("com.picsart.studio", "editor_done_click", from, to).
    filter($"platform" === "android").
    select($"editor_sid").distinct

val apply_done = edit_beautify_apply_per_cohort.as("a").
    join(editor_done_click.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
    groupBy($"variant", $"cohort").
    agg(
    	countDistinct($"a.editor_sid").as("apply_sid"), 
    	countDistinct($"b.editor_sid").as("done_id"), 
    	(countDistinct($"b.editor_sid")/countDistinct($"a.editor_sid")) .as("apply->done")
    )


// ED -> Apply //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// original

val from = DateTime.parse("2018-05-07")
val to = DateTime.parse("2018-05-13")

val editor_done_click = getEvent("com.picsart.studio", "editor_done_click", from, to).
    filter($"platform" === "android").
    select($"device_id", to_date($"timestamp").as("date"), $"editor_sid").distinct

val ed_per_cohort = users.filter($"variant"==="original").as("a").
    join(editor_done_click.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"editor_sid").distinct

val effect_apply = getEvent("com.picsart.studio", "effect_apply", from, to).
    filter($"platform" === "android").
    filter($"category_name" === "corrections").
    select($"editor_sid").distinct

val apply_done = ed_per_cohort.as("a").
    join(effect_apply.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
    groupBy($"variant", $"cohort").
    agg(
    	countDistinct($"a.editor_sid").as("done_sid"), 
    	countDistinct($"b.editor_sid").as("apply_sid"),
    	(countDistinct($"b.editor_sid")/countDistinct($"a.editor_sid")).as("ed->apply")
    )

//// beautify

val from = DateTime.parse("2018-05-07")
val to = DateTime.parse("2018-05-13")

val editor_done_click = getEvent("com.picsart.studio", "editor_done_click", from, to).
    filter($"platform" === "android").
    select($"device_id", to_date($"timestamp").as("date"), $"editor_sid").distinct

val ed_per_cohort = users.filter($"variant"==="beautify").as("a").
    join(editor_done_click.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"editor_sid").distinct

val edit_beautify_apply = getEvent("com.picsart.studio", "edit_beautify_apply", from, to).
    filter($"platform" === "android").
    select($"editor_sid").distinct

val apply_done = ed_per_cohort.as("a").
    join(edit_beautify_apply.as("b"), $"a.editor_sid" === $"b.editor_sid", "left").
    
    groupBy($"variant", $"cohort").
    agg(
    	countDistinct($"a.editor_sid").as("done_sid"), 
    	countDistinct($"b.editor_sid").as("apply_sid"),
		(countDistinct($"b.editor_sid")/countDistinct($"a.editor_sid")).as("ed->apply")
    )



// Effect_Apply DAU / EO DAU && EA / EO DAU ///////////////////////////////////////////////////////////////////////////////////////////////

val from = DateTime.parse("2018-05-07")
val to = DateTime.parse("2018-05-13")

val edit_item_open = getEvent("com.picsart.studio", "edit_item_open", from, to).
    filter($"item" === "effects").
    filter($"platform" === "android").
    select($"device_id", to_date($"timestamp").as("date")).distinct

val effect_apply = getEvent("com.picsart.studio", "effect_apply", from, to).
    filter($"platform" === "android").
    groupBy($"device_id", to_date($"timestamp").as("date")).count

val effect_open_per_cohort = users.as("a").
    join(edit_item_open.as("b"), ($"a.device_id" === $"b.device_id") && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id")

val effect_apply_open_per_cohort = effect_open_per_cohort.as("a").
    join(effect_apply.as("b"), $"a.device_id" === $"b.device_id" && $"a.date" === $"b.date", "left").
    select($"a.variant", $"a.cohort", $"a.date", $"a.device_id".as("open_d"), $"b.device_id".as("apply_d"), $"b.count").
    groupBy($"variant", $"date", $"cohort").agg(
        countDistinct($"open_d").as("open_dau"),
        countDistinct($"apply_d").as("apply_dau"),
        sum($"count").as("applies")).
    groupBy($"variant", $"cohort").
    agg(avg($"open_dau").as("open_dau"), avg($"apply_dau").as("apply_dau"), avg($"applies").as("applies")).
    orderBy($"variant", $"cohort")



////// funnel ///////////////////////////////////////////////////////////////////////////////////////////////

val from = DateTime.parse("2018-05-07")
val to = DateTime.parse("2018-05-13")

val edit_item_open = getEvent("com.picsart.studio", "edit_item_open", from, to).
    filter($"platform" === "android").
    filter($"item" === "effects").
    select($"device_id", to_date($"timestamp").as("date"), $"editor_sid").distinct

val edit_item_open_per_cohort = users.as("a").
    join(edit_item_open.as("b"), ($"a.device_id" === $"b.device_id") && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"b.editor_sid").distinct

val popup_open = getEvent("com.picsart.studio", "popup_open", from, to).
    filter($"platform" === "android").
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

///////////////////////////
// Tool Open Distribution ///////////////////////////////////////////////////////////////////////////////////////////////////////

// for original


val from = DateTime.parse("2018-05-07")
val to = DateTime.parse("2018-05-13")

val effect_category_open = getEvent("com.picsart.studio", "effect_category_open", from, to).
    filter($"platform" === "android").
    filter($"category_name" === "corrections").
    groupBy($"device_id", to_date($"timestamp").as("date")).count

val effect_category_open_per_cohort = users.filter($"variant"==="original").as("a").
    join(effect_category_open.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id", $"count").
    groupBy($"variant", $"cohort", $"date", $"count".as("open_count")).agg(countDistinct($"device_id").as("users")).
    groupBy($"variant", $"open_count").pivot("cohort").agg(avg($"users").as("users")).
    orderBy($"variant", $"open_count")

// for beautify
val from = DateTime.parse("2018-05-07")
val to = DateTime.parse("2018-05-13")

val edit_beautify_open = getEvent("com.picsart.studio", "edit_beautify_open", from, to).
    filter($"platform" === "android").
    groupBy($"device_id", to_date($"timestamp").as("date")).count

val edit_beautify_open_per_cohort = users.filter($"variant"==="beautify").as("a").
    join(edit_beautify_open.as("b"), $"a.device_id" === $"b.device_id" && ($"a.exp_date" <= $"b.date"), "inner").
    select($"variant", $"cohort", $"date", $"a.device_id", $"count").
    groupBy($"variant", $"cohort", $"date", $"count".as("open_count")).agg(countDistinct($"device_id").as("users")).
    groupBy($"variant", $"open_count").pivot("cohort").agg(avg($"users").as("users")).
    orderBy($"variant", $"open_count")



