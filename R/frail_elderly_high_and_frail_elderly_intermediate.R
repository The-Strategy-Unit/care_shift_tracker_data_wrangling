# Functions for the elective to non elective ratio indicator: 
# `frail_elderly_high` and `frail_elderly_intermediate`.

#' Used in `get_frailty_beddays_sub_geography()` to get the number of bed days 
#' and diagnosis code at the patient level.
#'
#' @param sub_geography Either `"lsoa"` or `"gp"`.
#' @param start The minimum date for the query.
#' @param connection The ODBC connection.
#'
#' @returns A patient and spell level dataframe with the number of bed days and 
#' diagnosis for each discharge linked to their previous discharges, by month of 
#' and subgeography.
get_frailty_beddays_data <- function(
    sub_geography, 
    start, 
    connection){
  
  sub_geography_column <- get_subgeography_column(sub_geography)
  
  query <- "
  WITH patients AS (
    SELECT
      APCE_Ident,
      Der_Pseudo_NHS_Number,
      convert(varchar(7), Discharge_Date, 120) AS date,
      sub_geography_column,
      Discharge_Date,
      DATEDIFF(day, Admission_Date, Discharge_Date) AS Spelldur

    FROM [Reporting_MESH_APC].[APCE_Core_Monthly_Snapshot]

    WHERE
      Last_Episode_In_Spell_Indicator = '1' AND
      Discharge_Date >= 'start_date' AND
      Der_Age_at_CDS_Activity_Date >= 75 AND
      LEFT(Admission_Method, 1) = '2'
      )

  SELECT 
    b.APCE_Ident,
    b.Der_Pseudo_NHS_Number, 
    b.date,
    b.sub_geography_column,
    b.Discharge_Date,
    b.Spelldur,
    a.diagnosis

    FROM patients AS b

    LEFT JOIN
       (SELECT
       	Der_Pseudo_NHS_Number,
      	LEFT(Der_Primary_Diagnosis_Code, 3) AS diagnosis,
        Discharge_Date

        FROM [Reporting_MESH_APC].[APCE_Core_Monthly_Snapshot]

        WHERE Last_Episode_In_Spell_Indicator = '1'
          AND LEFT(Admission_Method, 1) = '2'
          ) AS a
    ON b.Der_Pseudo_NHS_Number = a.Der_Pseudo_NHS_Number
      AND a.Discharge_Date BETWEEN DATEADD(year, -2, b.Discharge_Date) AND b.Discharge_Date
  " |>
    stringr::str_replace_all(
      c("start_date" = start,
        "sub_geography_column" = sub_geography_column
      )
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

#' The number of bed days for frailty admissions LSOA/GP and month.
#'
#' @param scores The `frailty_risk_scores.csv`
#' @param sub_geography Either `"lsoa"` or `"gp"`.
#' @param start The minimum date for the query.
#' @param connection The ODBC connection.
#'
#' @returns A dataframe with the number of bed days for high and intermediate 
#' frailty emergency admissions by month and subgeography.
get_frailty_beddays_sub_geography <- function(sub_geography, 
                                              start, 
                                              connection,
                                              scores){
  data <- get_frailty_beddays_data(sub_geography, 
                                   start, 
                                   connection)
  
  sub_geography_column <- get_subgeography_column(sub_geography) |>
    snakecase::to_snake_case()
  
  wrangled <- data |>
    dplyr::left_join(scores, by = c("diagnosis" = "icd10")) |>
    dplyr::filter(!is.na(score)) |>
    dplyr::distinct() |>
    dplyr::summarise(
      total_score = sum(score, na.rm = TRUE),
      .by = c(
        apce_ident,
        date,
        der_pseudo_nhs_number,
        !!rlang::sym(sub_geography_column),
        spelldur
      )
    ) |>
    dplyr::filter(total_score >= 5) |>
    dplyr::mutate(
      indicator = ifelse(
        total_score >= 5 &
          total_score <= 15,
        "frail_elderly_intermediate",
        "frail_elderly_high"
      )
    ) |>
    dplyr::summarise(beddays = sum(spelldur),
                     .by = c(date,
                             indicator,
                             !!rlang::sym(sub_geography_column)))
  
  return(wrangled)
}

get_frailty_beddays_geography <- function(data, geography, lookup) {
  geography_column <- get_geography_column(geography)
  
  wrangled <- data |>
    join_to_geography_lookup(geography, lookup) |>
    dplyr::summarise(
      beddays = sum(beddays, na.rm = TRUE),
      .by = c(date, !!rlang::sym(geography_column), indicator)
    ) |>
    dplyr::filter(!is.na(!!rlang::sym(geography_column))) |>
    dplyr::select(
      indicator,
      !!rlang::sym(geography) := !!rlang::sym(geography_column),
      date,
      beddays
    )
  
  return(wrangled)
}

get_frailty_indicators <- function(data, population, geography, latest_population_year) {
  wrangled <- data |>
    dplyr::mutate(
      year = stringr::str_sub(date, start = 1, end = 4),
      population_year = ifelse(year > latest_population_year, 
                               latest_population_year, 
                               year)
    ) |>
    dplyr::left_join(population, by = c(geography, "population_year")) |>
    dplyr::mutate(
      beddays_per_100000_pop = (beddays * 100000 / population_size) |>
        janitor::round_half_up()
    ) |>
    dplyr::select(
      indicator,
      date,
      !!rlang::sym(geography),
      beddays,
      population_size,
      beddays_per_100000_pop
    )
  
  return(wrangled)
}
