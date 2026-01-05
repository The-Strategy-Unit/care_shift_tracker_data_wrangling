# Functions for redirection and substitution indicators:
# `redirection_age_sex_standardised_admissions` and 
# `redirection_age_sex_standardised_beddays`.

#' Aggregate sub-geography level to geography level.
#'
#' @param data A data frame of admissions/beddays by LSOA/GP code and month for 
#' an indicator.
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#'
#' @returns A dataframe with the data aggregated to geography level and also by
#' age range and sex.
aggregate_indicator_to_geography_level_by_age_sex <- function(data,
                                                              geography, 
                                                              indicator_name) {
  geography_column <- get_geography_column(geography)
  
  wrangled <- data |>
    dplyr::summarise(
      admissions = sum(admissions, na.rm = TRUE),
      beddays = sum(beddays, na.rm = TRUE),
      .by = c(date, age_range, sex, !!rlang::sym(geography_column))
    ) |>
    dplyr::mutate(indicator = indicator_name) |>
    tidy_data_for_indicator_wrangling(geography)
  
  return(wrangled)
}

#' The number of end of life admissions/beddays by LSOA/GP and month.
#'
#' @param age The minimum age cutoff.
#' @param start The minimum date for the query.
#' @param lag The maximum date for the query.
#' @param connection The ODBC connection.
#'
#' @returns A dataframe with the number of end of life admissions/beddays 
# 'by LSOA/GP code and month.
get_end_of_life_episodes <- function(age, 
                                     start, 
                                     lag,
                                     connection) {
  query <- "
  	SELECT
  		APCE_Ident,
    	Der_Postcode_LSOA_2011_Code,
    	GP_Practice_SUS,
    	convert(varchar(7), Discharge_Date, 120) AS date,
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
  
  	FROM [Reporting_MESH_APC].[APCE_Core_Monthly_Snapshot] AS a
  
  	WHERE 
      Last_Episode_In_Spell_Indicator = '1' AND
  		Discharge_Date >= 'start_date' AND
  		Discharge_Date < 'lag_date' AND
  		Der_Age_at_CDS_Activity_Date >= age_cutoff AND
  		Discharge_Method = '4' AND
  		DATEDIFF(day, Admission_Date, Discharge_Date) <= 14 AND
  		(Der_Diagnosis_All LIKE '%,[V-Y]%'
  		OR LEFT(Der_Diagnosis_All, 1) LIKE '[V-X]')
  " |>
    stringr::str_replace_all(
      c("age_cutoff" = age,
        "start_date" = start,
        "lag_date" = lag
      )
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

#' Get the IMD decile by date and LSOA.
#'
#' @param data A dataframe with columns for date and LSOA.
#' @param lookup The IMD to LSOA lookup.
#' @param earliest_imd_year The earliest IMD year from the IMD to LSOA lookup.
#' @param latest_available_imd A dataframe of the latest available IMD decile for each LSOA.
#'
#' @returns The dataframe with a column added for the IMD decile.
get_imd_from_lsoa <- function(data, 
                              lookup, 
                              earliest_imd_year, 
                              latest_available_imd) {
  wrangled <- data |>
    dplyr::mutate(
      imd_year = lubridate::ymd(date, truncated = 1),
      imd_year = dplyr::case_when(
        imd_year >= as.Date("2025-04-01") ~ "2025-12-31",
        imd_year >= as.Date("2019-04-01") ~ "2019-12-31",
        imd_year >= as.Date("2015-04-01") ~ "2015-12-31",
        imd_year >= as.Date("2010-04-01") ~ "2010-12-31",
        imd_year >= as.Date("2007-04-01") ~ "2007-12-31",
        .default = "Other"
      ),
      # If IMD year is not in the imd lookup, use earliest available IMD year:
      imd_year = ifelse(imd_year < earliest_imd_year,
                        as.character(earliest_imd_year),
                        imd_year) |>
        as.Date()
    ) |>
    dplyr::left_join(lookup,
                     by = c("imd_year" = "effective_snapshot_date",
                            "der_postcode_lsoa_2011_code" = "lsoa_code")) |>
    # If IMD decile is missing, use the latest available IMD decile:
    dplyr::left_join(latest_available_imd,
                     by = c("der_postcode_lsoa_2011_code" = "lsoa_code")) |>
    dplyr::mutate(imd_decile = ifelse(is.na(imd_decile),
                                      latest_imd_decile,
                                      imd_decile)) |>
    dplyr::select(-latest_imd_decile)
  
  return(wrangled)
}

#' Turn episode level indicator data into sub-geography level.
#'
#' @param data Data for an indicator at episode level.
#' @param sub_geography Either `"lsoa"` or `"gp"`.
#'
#' @returns A dataframe of the number of admissions/beddays for the indicator
#' at LSOA/GP level by month and also by age range and sex.
get_indicator_at_sub_geography_level_by_age_sex <- function(data, 
                                                            sub_geography) {
  sub_geography_column <- get_subgeography_column(sub_geography) |>
    snakecase::to_snake_case()
  
  wrangled <- data |>
    unique() |>
    dplyr::summarise(admissions = dplyr::n(),
                     beddays = sum(spelldur, na.rm = TRUE),
                     .by = c(date, 
                             age_range, 
                             sex, 
                             !!rlang::sym(sub_geography_column)))
  
  return(wrangled)
}

#' Get the redirection/substitution indicators standardised by age and sex for
#' a geography.
#'
#' @param data The redirection/substitution indicators at sub-geography level.
#' @param population Population data at the geography of interest by age and 
#' sex.
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#' @param latest_population_year The last year that population data is available
#' for.
#' @param activity_type Either `"admissions"` or `"beddays"`.
#' @param standard_pop England 2021 census population data.
#'
#' @returns A dataframe of the redirection/substitution indicators standardised 
#' by age and sex for a geography.
get_indicators_age_sex_standardised_rates <- function(data,
                                                      population,
                                                      geography,
                                                      latest_population_year,
                                                      activity_type,
                                                      standard_pop) {
  wrangled <- data |>
    join_to_population_data_by_age_sex(population, geography, latest_population_year) |>
    dplyr::filter(!is.na(population_size),
                  population_size > 0) |>
    # There were <5 patients with a discharge date before their admission date
    # in one indicator at GP level. So the line below is to exclude these rows:
    dplyr::filter(!!rlang::sym(activity_type) >= 0) |> 
    dplyr::left_join(standard_pop, 
                     by = c("age_range", "sex")) |>
    dplyr::group_by(date, !!rlang::sym(geography)) |>
    dplyr::rename(x = !!rlang::sym(activity_type)) |>
    PHEindicatormethods::calculate_dsr(x = x,
                                       n = population_size,
                                       stdpop = pop) |>
    dplyr::ungroup() |>
    dplyr::mutate(dplyr::across(c(value, lowercl, uppercl), 
                                ~janitor::round_half_up(.)),
                  indicator = glue::glue("redirection_age_sex_standardised_{activity_type}")) |>
    dplyr::select(
      indicator,
      date,
      !!rlang::sym(geography),
      numerator = total_count,
      denominator = total_pop,
      value,
      lowercl,
      uppercl
    )
  
  return(wrangled)
}

#' Join a dataframe at geography level to its population data.
#'
#' @param data A dataframe at geography level.
#' @param population The population data by month and geography.
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#' @param latest_population_year The latest year that population data is
#' available for.
#'
#' @returns A dataframe containing `data` joined to the `population` data.
join_to_population_data_by_age_sex <- function(data,
                                               population,
                                               geography,
                                               latest_population_year) {
  wrangled <- if (geography == "pcn") {
    data  |>
      dplyr::left_join(population, by = c(geography, "date", "age_range", "sex"))
  } else {
    data |>
      dplyr::mutate(
        year = stringr::str_sub(date, start = 1, end = 4),
        population_year = ifelse(
          year > latest_population_year,
          as.character(latest_population_year),
          as.character(year)
        )
      ) |>
      dplyr::left_join(population, by = c(geography, "population_year", "age_range", "sex"))
  }
  
  return(wrangled)
}