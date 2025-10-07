# Functions to wrangle population data.

#' Get GP populations after 2017-04-01.
#'
#' @param age_band A string containing the 5 year age bands to filter for.
#' @param start The minimum date for the query.
#' @param connection The ODBC connection.
#'
#' @returns A dataframe of GP populations by month.
get_population_gp_post_2017_04_01 <- function(age_band, start, connection) {
  query <- "
    SELECT
        Org_Code AS gp,
        convert(varchar(7), Effective_Snapshot_Date, 120) AS date,
        SUM(Number_Of_Patients) AS population_size

      FROM [UKHF_Demography].[No_Of_Patients_Regd_At_GP_Prac_Regions_5Yr_AgeBand1_1]

      WHERE Age_Band IN age_modified_bands
        AND Org_Type = 'GP'
        AND Effective_Snapshot_Date >= 'start_date'
        AND Sex != 'ALL'

      GROUP BY
        Org_Code,
        convert(varchar(7), Effective_Snapshot_Date, 120)
  " |>
    stringr::str_replace_all(c("age_modified_bands" = stringr::str_replace_all(age_band, "-", "_"), 
                               "start_date" = start))
  
  data <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(data)
}

#' Get GP populations before 2017-04-01.
#' Note: this transforms quarterly to monthly data by assuming all months in a 
#' quarter have the same data as the first month in the quarter.
#'
#' @param age_band A string containing the 5 year age bands to filter for.
#' @param start The minimum date for the query.
#' @param connection The ODBC connection.
#'
#' @returns A dataframe of GP populations by month.
get_population_gp_pre_2017_04_01 <- function(age_band, start, connection) {
  query <- "
      SELECT
        GP_Practice_Code AS gp,
        convert(varchar(7), Effective_Snapshot_Date, 120) AS date,
        SUM(Size) AS population_size

      FROM [UKHF_Demography].[No_Of_Patients_Regd_At_GP_Practice1_1]

      WHERE Age_Band IN age_bands
        AND Effective_Snapshot_Date >= 'start_date'

      GROUP BY
        GP_Practice_Code,
        convert(varchar(7), Effective_Snapshot_Date, 120)
  
  " |>
    stringr::str_replace_all(c("age_bands" = age_band,
                               "start_date" = start))
  
  data <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names() |>
    dplyr::mutate(date = lubridate::ym(date)) 
  
  # We want monthly data, but this is quarterly. So we use the data from the 
  # first month in a quarter as the data for each month in that quarter:
  data <- data |>
    rbind(data |>
            dplyr::mutate(date = date + months(1)),
          data |>
            dplyr::mutate(date = date + months(2))) |>
    dplyr::mutate(date = stringr::str_sub(date, start = 1, end = 7))
  
  return(data)
}

#' Get ICB / LA populations from LSOA populations.
#'
#' @param data A dataframe of LSOA populations.
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#'
#' @returns A dataframe of ICB / LA populations by year.
get_population_higher_geography_from_lsoa <- function(data, geography) {
  geography_column <- get_geography_column(geography)
  
  wrangled <- data |>
    dplyr::summarise(
      population_size = sum(population_size_amended),
      .by = c(effective_snapshot_date, {{geography_column}})
    ) |>
    dplyr::filter(!is.na(!!rlang::sym(geography_column))) |>
    dplyr::mutate(population_year = as.character(
      lubridate::year(effective_snapshot_date))) |>
    dplyr::select(
      population_year,
      !!rlang::sym(geography) := !!rlang::sym(geography_column),
      population_size
    )
  
  return(wrangled)
}

#' Get LSOA populations.
#'
#' @param age The minimum age cutoff.
#' @param start The minimum date for the query.
#' @param connection The ODBC connection.
#'
#' @returns A dataframe of LSOA populations by year.
get_population_lsoa <- function(age, start, connection) {
  query <- "SELECT
        Area_code,
        Effective_Snapshot_Date,
        SUM(Size) AS population_size

      FROM [UKHF_Demography].[ONS_Population_Estimates_For_LSOAs_By_Year_Of_Age1_1]

      WHERE Age >= 'age_cutoff'
        AND LEFT(Area_code, 1) = 'E'
        AND Effective_Snapshot_Date >= 'start_date'

      GROUP BY
        Area_code,
        Effective_Snapshot_Date" |>
    stringr::str_replace_all(c("age_cutoff" = as.character(age), 
                               "start_date" = start))
  
  data <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(data)
}

#' Get PCN populations from the GP populations.
#' Note: there are two distinct GP population datasets (pre and post 
#' 2017-04-01). It does not matter which order they are provided in.
#'
#' @param population_gp1 The first dataframe of gp populations.
#' @param population_gp2 The second dataframe of gp populations.
#' @param lookup The gp to pcn lookup.
#'
#' @returns A dataframe of PCN populations by month.
get_population_pcn <- function(population_gp1, population_gp2, lookup){
  population_gp <- population_gp1 |>
    rbind(population_gp2)
  
  data <-  population_gp |>
    dplyr::left_join(lookup, by = c("gp" = "partner_organisation_code")) |>
    dplyr::summarise(population_size = sum(population_size),
                     .by = c(date, pcn_code)) |>
    dplyr::filter(!is.na(pcn_code)) |>
    dplyr::rename(pcn = pcn_code)
  
  return(data)
  
}