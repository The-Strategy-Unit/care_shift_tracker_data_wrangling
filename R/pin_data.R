# Functions that pin data. Includes some wrangling to create reference files.

#' Pins indicator data as parquet files.
#'
#' @param data The indicator data for ICB/LA/PCN.
#' @param lookup A lookup of ICB/LA/PCN codes and names.
#' @param geography The column geography of interest: `"icb"`, `"la"` or `"pcn"`.
#' @param board The pins board.
#'
#' @returns The data that was pinned.
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

pin_perc_change <- function(data, board) {
  
  pin_name <- glue::glue("{Sys.getenv('NHNIP_CARE_SHIFT_TRACKER_BOARD_OWNER')}/nhnip-care-shift-tracker-perc_change")
  
  pins::pin_write(
    board,
    x = data,
    name = pin_name,
    type = "parquet",
    versioned = TRUE
  )
  
  return(data)
}

#' Pins reference data for indicators.
#'
#' @param data The object `indicators_icb`.
#' @param board The pins board.
#'
#' @returns The name of the pin.
pin_ref_indicator <- function(data, board) {
  wrangled <- data |>
    dplyr::select(indicator, frequency) |>
    unique() |>
    dplyr::mutate(
      theme = dplyr::case_when(
        indicator %in% c("frequent_attenders_adult_ambulance_per_pop",
                         "frail_elderly_per_pop_beddays",
                         "falls_related_admissions_per_pop_beddays"
        ) ~ "Frailty and vulnerability",
        indicator %in% c("ambulatory_care_conditions_acute_per_pop_beddays",
                         "ambulatory_care_conditions_chronic_per_pop_beddays",
                         "readmission_within_28_days_per_pop_beddays",
                         "redirection_age_sex_imd_standardised_beddays"
        ) ~ "Avoidable or preventable hospital use",
        indicator %in% c("raid_ae_per_pop_beddays",
                         "elec_non_elec_ratio_beddays",
                         "delayed_discharge_percent_beddays"
        ) ~ "System flows and pathways",
        indicator %in% c("acute_bedshare_percent",
                         "workforce_acute_perc",
                         "costs_community_ratio",
                         "beddays_nonacute_percent"
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
            "elderly" = "older people",
            "standardised" = "standardised_rate",
            "_" = " ")
        ) |>
        stringr::str_to_sentence() |>
        stringr::str_replace_all(
          c("via ed" = "via ED",
            " age " = " age range, ",
            " imd " = " and IMD quintile "
            ))
    ) |>
    dplyr::arrange(frequency, theme, indicator) 
  
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

#' Pins reference data for geographies.
#'
#' @param ref_icb Reference information for ICBs.
#' @param ref_la Reference information for LAs.
#' @param ref_pcn Reference information for PCNs.
#' @param pcn_to_icb Map between PCNs and ICBs.
#' @param board The pins board.
#'
#' @returns The name of the pin.
pin_ref_geography <- function(ref_icb, ref_la, ref_pcn, pcn_to_icb, board) {
  
  wrangled <- rbind(ref_icb, ref_la, ref_pcn) |>
    dplyr::arrange(geography, name) |>
    dplyr::mutate(shortname = name |>
                    stringr::str_replace_all(c("NHS " = "",
                                               " Integrated Care Board" = "",
                                               " PCN" = ""))) |>
    dplyr::left_join(pcn_to_icb, by = c("code" = "pcn"))
  
  pin_name <- glue::glue("{Sys.getenv('NHNIP_CARE_SHIFT_TRACKER_BOARD_OWNER')}/nhnip-care-shift-tracker-ref-geography")
  
  pins::pin_write(
    board,
    x = wrangled,
    name = pin_name,
    type = "parquet",
    versioned = TRUE
  )
  
  return(wrangled)
}