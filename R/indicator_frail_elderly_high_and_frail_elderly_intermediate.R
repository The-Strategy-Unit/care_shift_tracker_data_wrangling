# Functions for the older people with frailty admissions indicators:
# `frail_elderly_high_per_pop_admissions`, 
# `frail_elderly_high_per_pop_beddays`, 
# `frail_elderly_intermediate_per_pop_admissions`and 
# `frail_elderly_intermediate_per_pop_beddays`.

#' Used in `get_frailty_sub_geography()` to get the number of admssions/beddays
#' and diagnosis code at the patient level.
#'
#' @param start The minimum date for the query.
#' @param connection The ODBC connection.
#'
#' @returns A patient and spell level dataframe with the number of 
#' admissions/beddays and diagnosis for each discharge linked to their previous
#' discharges, by month and subgeography.
get_frailty_data <- function(start, connection) {
  
  query <- "
  WITH patients AS (
    SELECT
      APCE_Ident,
      Der_Pseudo_NHS_Number,
      convert(varchar(7), Discharge_Date, 120) AS date,
    	Der_Postcode_LSOA_2011_Code,
    	GP_Practice_SUS,
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
    b.Der_Postcode_LSOA_2011_Code,
    b.GP_Practice_SUS,
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
    stringr::str_replace_all(c("start_date" = start))
  
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
    ) 
  
  return(wrangled)
}
 