# General functions.
#
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
    "Der_Postcode_LSOA_2021_Code"
  } else if (sub_geography == "gp") {
    "GP_Practice_SUS"
  }
  
  return(column)
}

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
      admissions,
      beddays
    )
  
  return(wrangled)
}