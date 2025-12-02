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
pin_indicators <- function(data, lookup, geography, board) {
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
  
  pin_name <- glue::glue("{Sys.getenv('NHNIP_CARE_SHIFT_TRACKER_BOARD_OWNER')}/nhnip-care-shift-tracker-indicators_{geography}")
  
  pins::pin_write(
    board,
    x = wrangled,
    name = pin_name,
    type = "parquet",
    versioned = TRUE
  )
  
  return(wrangled)
}




pin_ref_indicator <- function(data, board) {
  wrangled <- data |>
    dplyr::select(indicator) |>
    unique() |>
    dplyr::mutate(
      theme = dplyr::case_when(
        indicator %in% c("frequent_attenders_adult_ambulance_per_pop",
                         "frail_elderly_intermediate_per_pop_beddays",
                         "frail_elderly_high_per_pop_beddays",
                         "falls_related_admissions_per_pop_beddays"
        ) ~ "Frailty and vulnerability",
        indicator %in% c("ambulatory_care_conditions_acute_per_pop_beddays",
                         "ambulatory_care_conditions_chronic_per_pop_beddays",
                         "readmission_within_28_days_per_pop_beddays",
                         "redirection_age_sex_standardised_beddays"
        ) ~ "Avoidable or preventable hospital use",
        indicator %in% c("raid_ae_per_pop_beddays",
                         "elec_non_elec_ratio_beddays",
                         "delayed_discharge_percent_beddays"
        ) ~ "System flows and pathways",
        indicator %in% c("acute_bedshare_percent",
                         "workforce_acute_perc"
        ) ~ "Balancing resources",
        .default = "Other"
      ),
      indicator_title = indicator |>
        stringr::str_replace_all(
          c("perc" = "percent",
            "percentent" = "percent",
            "per_pop" = "per 100,000 population",
            "raid_ae" = "mental health admissions via ED",
            "elec_" = "elective_",
            "elecelective_" = "elective_",
            "beddays" = "(beddays)",
            "_" = " ")
        ) |>
        stringr::str_to_sentence() |>
        stringr::str_replace("via ed", "via ED")
    ) |>
    dplyr::arrange(theme, indicator) 
  
  pin_name <- glue::glue("{Sys.getenv('NHNIP_CARE_SHIFT_TRACKER_BOARD_OWNER')}/nhnip-care-shift-tracker-ref-indicator")
  
  pins::pin_write(
    board,
    x = wrangled,
    name = pin_name,
    type = "parquet",
    versioned = TRUE
  )
  
  return(pin_name)
}

pin_ref_geography <- function(ref_icb, ref_la, ref_pcn, board) {
  
  wrangled <- rbind(ref_icb, ref_la, ref_pcn) |>
    dplyr::arrange(geography, name) |>
    dplyr::mutate(shortname = name |>
                    stringr::str_replace_all(c("NHS " = "",
                                               " Integrated Care Board" = "",
                                               " PCN" = "")))
  
  pin_name <- glue::glue("{Sys.getenv('NHNIP_CARE_SHIFT_TRACKER_BOARD_OWNER')}/nhnip-care-shift-tracker-ref-geography")
  
  pins::pin_write(
    board,
    x = wrangled,
    name = pin_name,
    type = "parquet",
    versioned = TRUE
  )
  
  return(pin_name)
}