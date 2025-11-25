# General functions.

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