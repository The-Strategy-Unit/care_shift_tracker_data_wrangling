#' Elective to non-elective admissions ratio.
#'
#' @param geography The geography column to group the data by.
#' @param connection The ODBC connection.
#' @param age An integer for the age cutoff.
#' @param start The minimum date for the query.
#'
#' @returns A dataframe with the elective to non-elective admissions ratio by
#' month and geography.
get_elective_non_elective_ratio <- function(geography,
                                            age = age_cutoff,
                                            start = start_date,
                                            connection = con) {
  ccg_to_icb_join <- join_to_map_ccg_to_icb(geography)
  
  query <- "
    SELECT
      convert(varchar(7), Discharge_Date, 120) AS date,
      geography,
      SUM(CASE WHEN Admission_Method IN ('11', '12', '13') THEN 1 ELSE 0 END) AS elective,
      SUM(CASE WHEN Admission_Method LIKE '2%' THEN 1 ELSE 0 END) AS non_elective

    FROM Reporting_MESH_APC.APCE_Core_Monthly_Snapshot AS apce
      
    ccg_to_icb_join

    WHERE Last_Episode_In_Spell_Indicator = '1'
      AND Discharge_Date >= 'start_date'
      AND Age_at_End_of_Episode_SUS >= age_cutoff

    GROUP BY convert(varchar(7), Discharge_Date, 120), geography
  " |>
    stringr::str_replace_all(c(
      "geography" = geography,
      "age_cutoff" = age,
      "start_date" = start,
      "ccg_to_icb_join" = ccg_to_icb_join
    ))
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names() |>
    dplyr::mutate(
      value = janitor::round_half_up(elective / non_elective, 2),
      indicator = "elective_non_elective_ratio"
    ) |>
    dplyr::select(indicator, 
                  snakecase::to_snake_case(geography), 
                  date, 
                  numerator = elective,
                  denominator = non_elective,
                  value)

  return(wrangled)
}

join_to_map_ccg_to_icb <- function(geography){
  
  join <- if(stringr::str_detect(geography, "icb")){
    "LEFT JOIN [Internal_Reference].[CCGToICB_1] AS ref
      ON apce.der_postcode_ccg_code = ref.org_code"
  } else {
    ""
  }
  
  return(join)
}
