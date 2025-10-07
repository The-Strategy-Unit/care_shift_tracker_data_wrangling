# Functions for the older people with frailty admissions indicators:
# `frail_elderly_high` and `frail_elderly_intermediate`.

#' Calculate the number of admissions/beddays by 100,000 population.
#'
#' @param data A dataframe of the number of bed days for frailty admissions and
#' their population by ICB/LA/PCN by month.
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#' @param activity_type Either `"admissions"` or `"beddays"`.
#'
#' @returns A dataframe with a column added for the number of admissions/beddays
#' by 100,000 population.
get_activity_per_100000_pop <- function(data, geography, activity_type) {
  wrangled <- data |>
    dplyr::filter(!is.na(population_size)) |>
    dplyr::filter(!!rlang::sym(activity_type) >= 0) |> # why negative one? !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    PHEindicatormethods::phe_rate(x = !!rlang::sym(activity_type),
                                  n = population_size,
                                  multiplier = 100000) |>
    dplyr::mutate(dplyr::across(c(value, lowercl, uppercl), 
                                ~janitor::round_half_up(.)),
                  indicator = glue::glue("{indicator}_per_pop_{activity_type}")) |>
    dplyr::select(
      indicator,
      date,
      !!rlang::sym(geography),
      numerator = !!rlang::sym(activity_type),
      denominator = population_size,
      value,
      lowercl,
      uppercl
    )
  
  return(wrangled)
}

#' Used in `get_frailty_sub_geography()` to get the number of admssions/beddays
#' and diagnosis code at the patient level.
#'
#' @param sub_geography Either `"lsoa"` or `"gp"`.
#' @param start The minimum date for the query.
#' @param connection The ODBC connection.
#'
#' @returns A patient and spell level dataframe with the number of 
#' admissions/beddays and diagnosis for each discharge linked to their previous
#' discharges, by month of and subgeography.
get_frailty_data <- function(sub_geography, start, connection) {
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
    stringr::str_replace_all(c(
      "start_date" = start,
      "sub_geography_column" = sub_geography_column
    ))
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

#' The number of admissions/beddays for frailty admissions ICB/LA/PCN and month.
#'
#' @param data The number of bed days for frailty admissions LSOA/GP and month.
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#' @param lookup The subgeography to the geography lookup.
#'
#' @returns A dataframe with the number of admissions/beddays for high and 
#' intermediate frailty emergency admissions by month and geography.
get_frailty_geography <- function(data, geography, lookup) {
  geography_column <- get_geography_column(geography)
  
  wrangled <- data |>
    join_to_geography_lookup(geography, lookup) |>
    dplyr::summarise(
      admissions = sum(admissions, na.rm = TRUE),
      beddays = sum(beddays, na.rm = TRUE),
      .by = c(date, !!rlang::sym(geography_column), indicator)
    ) |>
    dplyr::filter(!is.na(!!rlang::sym(geography_column))) |>
    dplyr::select(
      indicator,
      !!rlang::sym(geography) := !!rlang::sym(geography_column),
      date,
      admissions,
      beddays
    )
  
  return(wrangled)
}

#' The number of admssions/beddays for frailty admissions LSOA/GP and month.
#'
#' @param scores The `frailty_risk_scores.csv`
#' @param sub_geography Either `"lsoa"` or `"gp"`.
#' @param start The minimum date for the query.
#' @param connection The ODBC connection.
#'
#' @returns A dataframe with the number of admssions/beddays for high and 
#' intermediate frailty emergency admissions by month and subgeography.
get_frailty_sub_geography <- function(sub_geography, 
                                              start, 
                                              connection, 
                                              scores) {
  data <- get_frailty_data(sub_geography, start, connection)
  
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
    dplyr::summarise(admissions = dplyr::n(),
                     beddays = sum(spelldur),
                     .by = c(date, 
                             indicator, 
                             !!rlang::sym(sub_geography_column)))
  
  return(wrangled)
}

#' Get the frailty indicators.
#'
#' @param data The number of admissions/beddays for frailty admissions by ICB/LA/PCN and
#' month.
#' @param population The population data by month and geography.
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#' @param latest_population_year The latest year that population data is
#' available for.
#' @param activity_type Either `"admissions"` or `"beddays"`.
#'
#' @returns A dataframe of the frailty indicators.
get_frailty_indicators <- function(data,
                                   population,
                                   geography,
                                   latest_population_year,
                                   activity_type) {
  wrangled <- data |>
    join_to_population_data(population, geography, latest_population_year) |>
    get_activity_per_100000_pop(geography, activity_type)
  
  return(wrangled)
}
