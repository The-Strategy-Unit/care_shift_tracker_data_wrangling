# General functions.

## Column names ----------------------------------------------------------------
#' Get the table column name related to the geography.
#'
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#'
#' @returns A string.
get_geography_column <- function(geography) {
  if (geography == "icb") {
    "icb24cdh"
  } else if (geography == "la") {
    "lad24cd"
  } else if (geography == "pcn") {
    "pcn_code"
  } else {
    "ERROR - please choose a geography: icb, la, pcn"
  }
}

#' Get the table column name related to the sub-geography.
#'
#' @param sub_geography The geography of interest: `"lsoa"`or `"gp"`.
#'
#' @returns A string.
get_subgeography_column <- function(sub_geography) {
  column <- if (sub_geography == "lsoa") {
    "Der_Postcode_LSOA_2011_Code"
  } else if (sub_geography == "gp") {
    "GP_Practice_SUS"
  }
  
  return(column)
}

## Joins -----------------------------------------------------------------------
#' Join a dataframe at sub-geography level to geography lookup.
#'
#' @param data A dataframe at sub-geography level
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#' @param lookup The lookup between sub-geography and geography.
#'
#' @returns A dataframe containing `data` joined to the `lookup`.
join_to_geography_lookup <- function(data, geography, lookup) {
  wrangled <- if (geography == "pcn") {
    data |>
      dplyr::left_join(lookup,
                       by = c("gp_practice_sus" = "partner_organisation_code"))
  } else {
    data |>
      dplyr::left_join(
        lookup |>
          dplyr::select(-dplyr::any_of("geometry")),
        by = c("der_postcode_lsoa_2021_code" = "lsoa21cd")
      )
  }
  
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
join_to_population_data <- function(data,
                                    population,
                                    geography,
                                    latest_population_year) {
  wrangled <- if (geography == "pcn") {
    data  |>
      dplyr::left_join(population, by = c(geography, "date"))
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
      dplyr::left_join(population, by = c(geography, "population_year"))
  }
  
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

## Indicator wrangling ---------------------------------------------------------
#' Aggregate sub-geography level to geography level.
#'
#' @param data A data frame of admissions/beddays by LSOA/GP code and month for 
#' an indicator.
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#'
#' @returns A dataframe with the data aggregated to geography level.
aggregate_indicator_to_geography_level <- function(data,
                                                   geography, 
                                                   indicator_name) {
  geography_column <- get_geography_column(geography)
  
  wrangled <- data |>
    dplyr::summarise(
      admissions = sum(admissions, na.rm = TRUE),
      beddays = sum(beddays, na.rm = TRUE),
      .by = c(date, !!rlang::sym(geography_column))
    ) |>
    dplyr::mutate(indicator = indicator_name) |>
    tidy_data_for_indicator_wrangling(geography)
  
  return(wrangled)
}

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

#' Episode level data for an emergency related indicator.
#'
#' @param age The minimum age cutoff.
#' @param start The minimum date for the query.
#' @param lag The maximum date for the query.
#' @param where The SQl where clause to filter the data to a particular indicator.
#' @param connection The ODBC connection.
#'
#' @returns A dataframe of episode level data for an emergency related indicator.
get_emergency_indicator_episodes <- function(age, 
                                             start, 
                                             lag,
                                             where,
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
  		LEFT(a.Admission_Method, 1) = '2' AND
  		(where_clause)
  " |>
    stringr::str_replace_all(
      c("age_cutoff" = age,
        "start_date" = start,
        "lag_date" = lag,
        "where_clause" = where
      )
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

#' Turn episode level indicator data into sub-geography level.
#'
#' @param data Data for an indicator at episode level.
#' @param sub_geography Either `"lsoa"` or `"gp"`.
#'
#' @returns A dataframe of the number of admissions/beddays for the indicator
#' at LSOA/GP level by month.
get_indicator_at_sub_geography_level <- function(data, sub_geography) {
  sub_geography_column <- get_subgeography_column(sub_geography) |>
    snakecase::to_snake_case()
  
  wrangled <- data |>
    unique() |>
    dplyr::summarise(admissions = dplyr::n(),
                     beddays = sum(spelldur, na.rm = TRUE),
                     .by = c(date, !!rlang::sym(sub_geography_column)))
  
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

#' Weight the indicators by 100,000 population.
#'
#' @param data The number of admissions/beddays for an indicator by ICB/LA/PCN 
#' and month.
#' @param population The population data by month and geography.
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#' @param latest_population_year The latest year that population data is
#' available for.
#' @param activity_type Either `"admissions"` or `"beddays"`.
#'
#' @returns The dataframe with columns added for weighting by population.
get_indicators_per_pop <- function(data,
                                   population,
                                   geography,
                                   latest_population_year,
                                   activity_type) {
  activity_type_rename <- if(activity_type == "attenders"){
    ""
  } else {
    glue::glue("_{activity_type}")
  }
  
  wrangled <- data |>
    join_to_population_data(population, geography, latest_population_year) |>
    dplyr::filter(!is.na(population_size),
                  population_size > 0) |>
    # There were <5 patients with a discharge date before their admission date
    # in one indicator at GP level. So the line below is to exclude these rows:
    dplyr::filter(!!rlang::sym(activity_type) >= 0) |> 
    PHEindicatormethods::phe_rate(x = !!rlang::sym(activity_type),
                                  n = population_size,
                                  multiplier = 100000) |>
    dplyr::mutate(dplyr::across(c(value, lowercl, uppercl), 
                                ~janitor::round_half_up(.)),
                  indicator = glue::glue("{indicator}_per_pop{activity_type_rename}")) |>
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

#' Tidy data to use `get_indicators_per_pop()`.
#'
#' @param data A dataframe of data that will be used to create the indicator.
#' @param geography The column geography of interest: `"icb"`, `"la"` or `"pcn"`.
#'
#' @returns A dataframe ready to feed into `get_indicators_per_pop()`.
tidy_data_for_indicator_wrangling <- function(data, geography){
  geography_column <- get_geography_column(geography)
  
  wrangled <- data |>
    dplyr::filter(!is.na(!!rlang::sym(geography_column))) |>
    dplyr::select(
      indicator,
      !!rlang::sym(geography) := !!rlang::sym(geography_column),
      date,
      dplyr::any_of(c("age_range", "sex")),
      admissions,
      beddays
    )
  
  return(wrangled)
}

# Other ------------------------------------------------------------------------
#' Scrape excel file from URL.
#'
#' @param url The URL of the excel file to download.
#' @param sheet The sheet number.
#' @param skip The number of lines to skip.
#'
#' @returns A dataframe.
scrape_xls <- function(url, sheet = 1, skip = 0) {
tmp <- tempfile(fileext = "")

download.file(url = url,
              destfile = tmp,
              mode = "wb")

data <- readxl::read_excel(path = tmp,
                           sheet = sheet,
                           skip = skip) |>
  janitor::clean_names()

return(data)

}






#' Transform LSOA 2011 data to LSOA 2021 data.
#'
#' @param data A dataframe with a column of LSOA11 codes. 
#' @param lookup A dataframe of LSOA11 codes mapped to LSOA21 codes.
#' @param value_column A string for the name of the column that may need to be
#' adjusted.
#'
#' @returns A dataframe with a column of LSOA21 codes and with the value 
#' column's numbers adjusted according to LSOA splits.
recode_lsoa11_as_lsoa21 <- function(data, lookup, value_column) {
  
  wrangled <- data |>
    # first map 11 to 21 codes:
    dplyr::left_join(lookup, 
                     by = c("der_postcode_lsoa_2011_code" = "lsoa11cd")) |>
    # then adjust numbers according to splits in LSOA:
    dplyr::mutate(
      number = dplyr::n(),
      value_column = !!rlang::sym(value_column) / number,
      .by = c(der_postcode_lsoa_2011_code, date)
    ) |>
    dplyr::rename(der_postcode_lsoa_2021_code = lsoa21cd)
  
  if(value_column == "admissions") {
    wrangled <- wrangled |>
      dplyr::mutate(beddays = beddays / number)
  }
  
  return(wrangled)
}

#' Writes indicator data to parquet files.
#'
#' @param data The indicator data for ICB/LA/PCN.
#' @param lookup A lookup of ICB/LA/PCN codes and names.
#' @param geography The column geography of interest: `"icb"`, `"la"` or `"pcn"`.
#'
#' @returns Parquet files saved in the data folder of the app repo.
write_indicator_to_parquet <- function(data, lookup, geography) {
  wrangled <- data |>
    dplyr::left_join(lookup |>
                       dplyr::select(-dplyr::any_of("geography")),
                     geography) |>
    dplyr::select(indicator,
                  !!rlang::sym(geography),
                  !!rlang::sym(glue::glue("{geography}_name")),
                  date,
                  numerator,
                  denominator,
                  value,
                  lowercl,
                  uppercl,
                  frequency) |>
    dplyr::mutate(date = lubridate::ymd(date, truncated = 1)) 
  
  wrangled |>
    arrow::write_parquet(glue::glue("../care_shift_tracker_app/data/indicators_{geography}.parquet"))
}

#' Gets reference data.
#'
#' @param lookup The ICB/LA/PCN lookup.
#' @param geo The column geography of interest: `"icb"`, `"la"` or `"pcn"`.
#'
#' @returns A reference file of ICB/LA/PCN codes and names.
get_ref_by_geography <- function(lookup, geo) {
  ref <- lookup |>
    dplyr::select(code = !!rlang::sym(geo), 
                  name = glue::glue("{geo}_name")) |>
    unique() |>
    dplyr::mutate(geography = geo)
  
  return(ref)
}