# Functions for the older people with frailty admissions indicators:
# `frail_elderly_per_pop_admissions` and
# `frail_elderly_per_pop_beddays`.

#' Used in `get_frailty_sub_geography()` to get the number of admssions/beddays
#' and diagnosis code at the patient level.
#'
#' @param start The minimum date for the query.
#' @param lag The maximum date for the query.
#' @param connection The ODBC connection.
#'
#' @returns A patient and spell level dataframe with the number of 
#' admissions/beddays and diagnosis for each discharge linked to their previous
#' discharges, by month and subgeography.
get_frailty_data <- function(start, lag, connection) {
  
  query <- "
  WITH patients AS (
    SELECT
      APCE_Ident,
      Der_Pseudo_NHS_Number,
      convert(varchar(7), Discharge_Date, 120) AS date,
    	Der_Postcode_LSOA_2011_Code,
    	GP_Practice_SUS,
      Discharge_Date,
    	Sex,
      CASE WHEN Der_Age_At_CDS_Activity_Date BETWEEN 0 AND 4 THEN '0-4'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 5 AND 9 THEN '5-9'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 10 AND 14 THEN '10-14'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 15 AND 19 THEN '15-19'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 20 AND 24 THEN '20-24'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 25 AND 29 THEN '25-29'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 30 AND 34 THEN '30-34'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 35 AND 39 THEN '35-39'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 40 AND 44 THEN '40-44'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 45 AND 49 THEN '45-49'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 50 AND 54 THEN '50-54'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 55 AND 59 THEN '55-59'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 60 AND 64 THEN '60-64'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 65 AND 69 THEN '65-69'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 70 AND 74 THEN '70-74'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 75 AND 79 THEN '75-79'
           WHEN Der_Age_At_CDS_Activity_Date >=80 THEN '80+'
           ELSE NULL
           END AS age_range,
      DATEDIFF(day, Admission_Date, Discharge_Date) AS Spelldur

    FROM [Reporting_MESH_APC].[APCE_Core_Monthly_Snapshot]

    WHERE
      Last_Episode_In_Spell_Indicator = '1' AND
      Discharge_Date >= 'start_date' AND
      Discharge_Date < 'lag_date' AND
      Der_Age_at_CDS_Activity_Date >= 75 AND
      LEFT(Admission_Method, 1) = '2'
      )

  SELECT
    b.APCE_Ident,
    b.Der_Pseudo_NHS_Number,
    b.date,
    b.Der_Postcode_LSOA_2011_Code,
    b.GP_Practice_SUS,
    b.Discharge_Date,
    b.sex,
    b.age_range,
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
    stringr::str_replace_all(c("start_date" = start,
                               "lag_date" = lag))
  
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
    tidy_data_for_indicator_wrangling(geography)
  
  return(wrangled)
}

#' Turn frailty episode level indicator data into sub-geography level.
#'
#' @param data Data for the frailty indicators at episode level.
#' @param sub_geography Either `"lsoa"` or `"gp"`.
#'
#' @returns A dataframe of the number of admissions/beddays for the frailty
#' indicators at LSOA/GP level by month.
get_frailty_sub_geography <- function(data,
                                      sub_geography) {
  sub_geography_column <- get_subgeography_column(sub_geography) |>
  snakecase::to_snake_case()
  
  wrangled <- data |>
    dplyr::summarise(admissions = dplyr::n(), 
                     beddays = sum(spelldur),
                     .by = c(date,
                             indicator,
                             !!rlang::sym(sub_geography_column)))
  
  return(wrangled)
}

#' Frailty episodes data with frailty risk scores.
#' 
#' @param data The dataframe `frailty_episodes`.
#' @param scores The `frailty_risk_scores.csv`
#'
#' @returns A dataframe of high and intermediate frailty risk episodes.
get_frailty_with_risk_scores <- function(data, 
                                         scores) {
  
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
        der_postcode_lsoa_2011_code,
        gp_practice_sus,
        age_range,
        sex,
        spelldur
      )
    ) |>
    dplyr::filter(total_score >= 5) |>
    # Had originally separated this indicator into intermediate and high, but 
    # because of the small numbers in the high group, combined these into one
    # indicator instead:
    # dplyr::mutate(
    #   indicator = ifelse(
    #     total_score >= 5 &
    #       total_score <= 15,
    #     "frail_elderly_intermediate",
    #     "frail_elderly_high"
    #   )
    # )
    dplyr::mutate(indicator = "frail_elderly") 
  
  return(wrangled)
}
 