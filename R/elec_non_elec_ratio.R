# Functions for the elective to non elective ratio indicator: 
# `elec_non_elec_ratio`.

#' The number of elective and non elective admissions by LSOA/GP and month.
#'
#' @param age The minimum age cutoff.
#' @param start The minimum date for the query.
#' @param connection The ODBC connection.
#' @param sub_geography Either `"lsoa"` or `"gp"`.
#'
#' @returns A dataframe with the number of elective and non elective admissions
#' by LSOA/GP code and month.
get_elective_non_elective_admissions_sub_geography <- function(
    sub_geography, 
    age, 
    start, 
    connection) {
  sub_geography_column <- get_subgeography_column(sub_geography)
  
  query <- "
    SELECT
      convert(varchar(7), Discharge_Date, 120) AS date,
      sub_geography_column,
      SUM(CASE WHEN Admission_Method IN ('11', '12', '13') THEN 1 ELSE 0 END) AS elective,
      SUM(CASE WHEN Admission_Method LIKE '2%' THEN 1 ELSE 0 END) AS non_elective

    FROM Reporting_MESH_APC.APCE_Core_Monthly_Snapshot AS apce

    WHERE Last_Episode_In_Spell_Indicator = '1'
      AND Discharge_Date >= 'start_date'
      AND Age_at_End_of_Episode_SUS >= age_cutoff
      AND LEFT(Der_Postcode_LSOA_2021_Code, 1) = 'E'

    GROUP BY
      convert(varchar(7), Discharge_Date, 120),
      sub_geography_column
  " |>
    stringr::str_replace_all(
      c(
        "age_cutoff" = age,
        "start_date" = start,
        "sub_geography_column" = sub_geography_column
      )
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

#' Elective to non-elective admissions ratio by month and geography.
#'
#' @param data The number of elective and non elective admissions by 2021 LSOA
#' code and month.
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#'
#' @returns A dataframe with the elective to non-elective admissions ratio by
#' month and geography.
get_elective_non_elective_ratio <- function(data, geography) {
  geography_column <- get_geography_column(geography)
  
  wrangled <- data |>
    dplyr::summarise(
      elective = sum(elective, na.rm = TRUE),
      non_elective = sum(non_elective, na.rm = TRUE),
      .by = c(date, !!rlang::sym(geography_column))
    ) |>
    dplyr::mutate(ratio = elective / non_elective, 
                  indicator = "elec_non_elec_ratio") |>
    dplyr::filter(!is.na(!!rlang::sym(geography_column))) |>
    dplyr::select(
      indicator,
      !!rlang::sym(geography) := !!rlang::sym(geography_column),
      date,
      numerator = elective,
      denominator = non_elective,
      ratio
    )
  
  return(wrangled)
}