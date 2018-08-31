val tools : Array[(Int, String, (String, String), (String, String))] = Array(
        (0, "Add Photo", ("edit_item_open", "item = 'add_photo'"), ("edit_photo_apply", "")),
        (1, "Adjust", ("edit_item_open", "item = 'tool_adjust'"), ("tool_adjust_apply", "")),
        (2, "Beautify", ("edit_item_open", "item = 'beautify'"), ("edit_beautify_apply", "")),
        (3, "Border", ("edit_item_open", "item = 'border'"), ("edit_border_apply", "")),
        (4, "Brush", ("edit_brush_try", ""), ("edit_brush_apply", "")),
        (5, "Callout", ("edit_item_open", "item = 'callout'"), ("edit_callout_apply", "")),
        (6, "Camera", ("camera_open", ""), ("camera_close", "action = 'done'")),
        (7, "Clone", ("edit_item_open", "item = 'tool_clone'"), ("tool_clone_apply", "")),
        (8, "Collage Frame", ("create_collage_frame_preview_next", ""), ("collage_frame_apply", "")),
        (9, "Collage FS", ("collage_free_style_open", ""), ("collage_free_style_background_apply", "")),
        (10, "Collage Grid", ("create_collage_grid_click", ""), ("collage_grid_apply", "")),
        (11, "Crop", ("tool_crop_open", ""), ("tool_crop_apply", "")),
        (12, "Curves", ("edit_item_open", "item = 'tool_curves'"), ("tool_curves_apply", "")),
        (13, "Cutout", ("edit_item_open", "item = 'tool_cutout'"), ("tool_cutout_apply", "")),
        (14, "Dispersion", ("edit_item_open", "item = 'tool_dispersion'"), ("tool_dispersion_apply", "")),
        (15, "Draw", ("draw_open", "source = 'editor'"), ("draw_done", "source = 'editor' AND exit_action = 'save'")),
        (16, "Effects", ("edit_item_open", "item = 'effects'"), ("effect_apply", "")),
        (17, "Enhance", ("edit_item_open", "item = 'tool_enhance'"), ("tool_enhance_apply", "")),
        (18, "Fliprotate", ("edit_item_open", "item = 'tool_fliprotate'"), ("tool_fliprotate_apply", "")),
        (19, "Frame", ("edit_item_open", "item = 'frame'"), ("edit_frame_apply", "")),
        (20, "Free Crop", ("edit_item_open", "item = 'tool_free_crop'"), ("tool_free_crop_apply", "")),
        (21, "Lensflare", ("edit_item_open", "item = 'lensflare'"), ("edit_lensflare_apply", "")),
        (22, "Mask", ("edit_item_open", "item = 'mask'"), ("edit_mask_apply", "")),
        (23, "Motion", ("edit_item_open", "item = 'tool_motion'"), ("tool_motion_apply", "")),
        (24, "Perspective", ("edit_item_open", "item = 'tool_perspective'"), ("tool_perspective_apply", "")),
        (25, "Quick Brush", ("edit_item_open", "item = 'quick_brush'"), ("quick_brush_page_close", "exit_action = 'apply'")),
        (26, "Resize", ("edit_item_open", "item = 'tool_resize'"), ("tool_resize_apply", "")),
        (27, "Selection", ("edit_item_open", "item = 'tool_selection'"), ("tool_selection_apply", "")),
        (28, "Shape Crop", ("edit_item_open", "item = 'tool_shape_crop'"), ("tool_shape_crop_apply", "")),
        (29, "Shape Mask", ("edit_item_open", "item = 'shape_mask'"), ("edit_shape_mask_apply", "")),
        (30, "Square Fit", ("edit_item_open", "item LIKE '%square_fit'"), ("edit_square_fit_apply", "")),
        (31, "Stamp", ("edit_item_open", "item = 'stamp'"), ("edit_stamp_apply", "")),
        (32, "Sticker", ("edit_item_open", "item = 'sticker'"), ("edit_sticker_apply", "source != 'camera'")),
        (33, "Stretch", ("edit_item_open", "item = 'tool_stretch'"), ("tool_stretch_apply", "")),
        (34, "Text", ("edit_item_open", "item = 'text'"), ("edit_text_apply", "")),
        (35, "Tilt Shift", ("edit_item_open", "item = 'tool_tilt_shift'"), ("tool_tilt_shift_apply", ""))
)
def editor_tools(dd : DateTime, tools_array : Array[(Int, String, (String, String), (String, String))], output : String) : Unit = {
    import scala.util.Try
    import org.joda.time.format.DateTimeFormat
    import org.joda.time.{DateTime, Days}
    import java.sql.Date
    import scala.util.control.Breaks._
    import org.apache.hadoop.fs.{Path, FileSystem}
    import org.apache.hadoop.conf.Configuration
    def event_exists(day:org.joda.time.DateTime, event_name:String):Boolean = Try(getEvent("com.picsart.studio", event_name, day, day)).isSuccess
    def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
    
    val measure = System.nanoTime

    val fs = FileSystem.get(new Configuration())
    val psql_table = "kpi_editor"
    val ed_temp = "/user/kpi/KPI_Editor_ed_temp"
    var top_countries = Array("ar","br","ca","cn","co","fr","de","in","id","it","jp","my","mx","ph","ru","kr","es","tr","gb","us","vn")
    val countries = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").
            load("/user/amber/csv/country_code.csv").select($"country_name", lower($"country_code").as("country_code"))
            
    var opens : org.apache.spark.sql.DataFrame = null
    var applies : org.apache.spark.sql.DataFrame = null
    var num : Int = 0
    var tool : String = null
    var open_event : String = ""
    var open_condition : String = ""
    var apply_event : String = ""
    var apply_condition : String = ""
    var ed_sid : String = ""
    
    //val opens_eio = getEvent("com.picsart.studio", "edit_item_open", dd, dd).filter(($"platform" === "android" && length($"version").in(3,9)) or ($"platform" === "apple" && length($"version") === 3))
    getEvent("com.picsart.studio","editor_done_click", dd, dd)
        .filter(($"platform" === "android" && length($"version").in(3,9)) or ($"platform" === "apple" && length($"version") === 3))
        .select($"platform", lower($"country_code").as("c"), $"editor_sid")
        .withColumn("country_code", when($"c".isin(top_countries:_*), $"c").otherwise("-")).as("c")
            .join(countries.as("d"), $"c.country_code" === $"d.country_code", "left")
            .select($"c.platform", $"c.editor_sid", when($"d.country_name".isNull, lit("Other")).otherwise($"d.country_name").as("country"))
            .distinct()
            .write.mode(org.apache.spark.sql.SaveMode.Overwrite).parquet(ed_temp)
    val ed_done = sqlContext.read.parquet(ed_temp + "/*").persist()
    for (data <- tools_array) {
        val measure2 = System.nanoTime
        num = data._1
        tool = data._2
        open_event = data._3._1
        open_condition = data._3._2
        apply_event = data._4._1
        apply_condition = data._4._2
        println("STRT - " + dd.toString().slice(0, 10) + " - " + (num + 1) + "/" + tools_array.size + " - " + tool)
        /*
        if (open_event == "edit_item_open") { opens = opens_eio } else {
            if (event_exists(dd, open_event)) {
                opens = getEvent("com.picsart.studio", open_event, dd, dd).filter(($"platform" === "android" && length($"version").in(3,9)) or ($"platform" === "apple" && length($"version") === 3))
            }
        }
        
        if (opens == null) {
            println("!!! Open Event for " + tool + " doesn't exist for " + dd.toString().slice(0, 10))
        } else {
            if (open_condition != "") { opens = opens.filter(open_condition) }
            val open_dau = opens
                .select($"platform", lower($"country_code").as("c"), $"device_id")
                .withColumn("country_code", when($"c".isin(top_countries:_*), $"c").otherwise("-"))
                .as("c")
                    .join(countries.as("d"), $"c.country_code" === $"d.country_code", "left")
                    .select($"c.platform", $"c.device_id", when($"d.country_name".isNull, lit("Other")).otherwise($"d.country_name").as("country"))
                    .groupBy($"platform", $"country").agg(countDistinct($"device_id").as("value"))
                    .withColumn("parameter", lit("Daily Open Devices"))
                    .withColumn("scope", lit(tool))
                    .withColumn("day", lit(dd.toString().slice(0, 10)))
                    .select($"day" cast "date", $"platform", $"country", $"scope", $"parameter", $"value")
            if (output == "show") {
                open_dau.orderBy($"day", $"scope", $"parameter", $"platform", $"country").show(1, false)
            } else if (output == "publish") {
                publish(open_dau, psql_table, true)
            }
        }
        */
        if (!event_exists(dd, apply_event)) { println("!!! Apply Event for " + tool + " doesn't exist for " + dd.toString().slice(0, 10)) }
        else {
            applies = getEvent("com.picsart.studio", apply_event, dd, dd)
                .filter(($"platform" === "android" && length($"version").in(3,9)) or ($"platform" === "apple" && length($"version") === 3))
            if (apply_condition != "") { applies = applies.filter(apply_condition) }
            if (hasColumn(applies, "editor_sid")) { ed_sid = "editor_sid" } else { ed_sid = "platform" }
            applies = applies
                .select($"platform", lower($"country_code").as("c"), $"device_id", col(ed_sid).as("editor_sid"))
                .withColumn("country_code", when($"c".isin(top_countries:_*), $"c").otherwise("-")).as("c")
                .join(countries.as("d"), $"c.country_code" === $"d.country_code", "left")
                .select($"c.platform", $"c.device_id", $"c.editor_sid", when($"d.country_name".isNull, lit("Other")).otherwise($"d.country_name").as("country"))
            /*        
            val apply_dau = applies
                    .groupBy($"platform", $"country").agg(countDistinct($"device_id").as("value"))
                    .withColumn("parameter", lit("Daily Apply Devices"))
                    .withColumn("scope", lit(tool))
                    .withColumn("day", lit(dd.toString().slice(0, 10)))
                    .select($"day" cast "date", $"platform", $"country", $"scope", $"parameter", $"value")
            */
            val applies_per_appliers = applies
                    .groupBy($"platform", $"country").agg((count($"device_id")/countDistinct($"device_id")).as("value"))
                    .withColumn("parameter", lit("Applies per Apply DAU"))
                    .withColumn("scope", lit(tool))
                    .withColumn("day", lit(dd.toString().slice(0, 10)))
                    .select($"day" cast "date", $"platform", $"country", $"scope", $"parameter", $"value")
            if (output == "show") {
                //apply_dau.orderBy($"day", $"scope", $"parameter", $"platform", $"country").show(1, false)
                applies_per_appliers.orderBy($"day", $"scope", $"parameter", $"platform", $"country").show(1,false)
            } else if (output == "publish") {
                //publish(apply_dau, psql_table, true)
                publish(applies_per_appliers, psql_table, true)
            }
            if (ed_sid == "editor_sid") {
                 val ed_in_apply = applies.select($"platform", $"editor_sid", $"country").as("a")
                        .join(ed_done.as("b"),$"a.editor_sid" === $"b.editor_sid", "left")
                        .groupBy($"a.platform", $"a.country").agg((countDistinct($"b.editor_sid")/countDistinct($"a.editor_sid")).as("value"))
                        .withColumn("parameter", lit("ED in Apply"))
                        .withColumn("scope", lit(tool))
                        .withColumn("day", lit(dd.toString().slice(0, 10)))
                        .select($"day" cast "date", $"platform", $"country", $"scope", $"parameter", $"value")
                val apply_in_ed = ed_done.as("a")
                        .join(applies.as("b"),$"a.editor_sid" === $"b.editor_sid", "left")
                        .groupBy($"a.platform", $"a.country").agg((countDistinct($"b.editor_sid")/countDistinct($"a.editor_sid")).as("value"))
                        .withColumn("parameter", lit("Apply in ED"))
                        .withColumn("scope", lit(tool))
                        .withColumn("day", lit(dd.toString().slice(0, 10)))
                        .select($"day" cast "date", $"platform", $"country", $"scope", $"parameter", $"value")
                if (output == "show") {
                    ed_in_apply.orderBy($"day", $"scope", $"parameter", $"platform", $"country").show(1,false)
                    apply_in_ed.orderBy($"day", $"scope", $"parameter", $"platform", $"country").show(1,false)
                } else if (output == "publish") {
                    publish(ed_in_apply, psql_table, true)
                    publish(apply_in_ed, psql_table, true)
                }
            }
        }
        //println("DONE - " + dd.toString().slice(0, 10) + " - " + (num + 1) + "/" + tools_array.size + " - " + tool)
        val duration2 = (System.nanoTime - measure2) / 1e9d
        println("DONE - " + dd.toString().slice(0, 10) + " - " + (num + 1) + "/" + tools_array.size + " - " + tool + ": " + duration2.round + "secs")
    }
    fs.delete(new Path(ed_temp))
    val duration = (System.nanoTime - measure) / 1e9d
    slackMessage("KPIs Calcs, Editor Tool, " + dd.toString().slice(0, 10) + " >> (" + "daily" + ",  " + output + ", " + duration.round + "secs)", "hakob")
}
/*
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}
import java.sql.Date
import scala.util.control.Breaks._
val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
var dd = today().minusDays(5)
editor_tools(dd, tools, "publish")
*/



import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}
import java.sql.Date
import scala.util.control.Breaks._
var to = DateTime.parse("2018-05-23")
var dd = to
var limit = 5
var i = 0


while(i<limit){
    println(dd)
    //editor_tools(dd, tools, "publish")
    i+=1
    dd = to.minusDays(i)
}