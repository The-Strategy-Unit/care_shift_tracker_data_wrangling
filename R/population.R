# Functions to wrangle population data.

#' Get ICB / LA populations from LSOA populations.
#'
#' @param data A dataframe of LSOA populations.
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#'
#' @returns A dataframe of ICB / LA populations by year.
get_higher_geography_population_from_lsoa <- function(data, geography) {
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

#' Get PCN populations.
#'
#' @param age The minimum age cutoff.
#' @param start The minimum date for the query.
#' @param connection The ODBC connection.
#'
#' @returns A dataframe of PCN populations by year.
get_population_pcn <- function(age, start, connection) {
  query <- "
    SELECT
        Org_Code AS pcn,
        convert(varchar(7), Effective_Snapshot_Date, 120) AS date,
        SUM(Number_Of_Patients) AS population_size

      FROM [UKHF_Demography].[No_Of_Patients_Regd_At_GP_Prac_Regions_Single_Age1_1]

      WHERE Age >= 'age_cutoff'
        AND Org_Type = 'PCN'
        AND Effective_Snapshot_Date >= 'start_date'

      GROUP BY
        Org_Code,
        convert(varchar(7), Effective_Snapshot_Date, 120)" |>
    stringr::str_replace_all(c("age_cutoff" = as.character(age), 
                               "start_date" = start))
  
  data <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(data)
}
