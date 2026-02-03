# Functions that pin data. Includes some wrangling to create reference files.

#' Pins indicator data as parquet files.
#'
#' @param data The indicator data for ICB/LA/NH.
#' @param lookup A lookup of ICB/LA/NH codes and names.
#' @param geography The column geography of interest: `"icb"`, `"la"` or `"nh"`.
#' @param board The pins board.
#'
#' @returns The contents of the pin.
pin_indicators <- function(data, lookup, geography, board) {
  
  wrangled <- data |>
    dplyr::arrange(frequency, indicator, date) |>
    dplyr::left_join(lookup |>
                       dplyr::select(-dplyr::any_of("geography")),
                     geography) |>
    transform_outliers() |>
    dplyr::select(indicator,
                  !!rlang::sym(geography),
                  !!rlang::sym(glue::glue("{geography}_name")),
                  date,
                  numerator,
                  denominator,
                  value,
                  lowercl,
                  uppercl,
                  frequency,
                  outlier,
                  value_outliers_transformed
                  ) |>
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
#' @returns The contents of the pin.
pin_ref_indicator <- function(data, board) {
  wrangled <- data |>
    dplyr::select(indicator, frequency) |>
    unique() |>
    dplyr::mutate(
      theme = dplyr::case_when(
        indicator %in% c("frequent_attenders_adult_ambulance_per_pop",
                         "frail_elderly_per_pop_beddays",
                         "falls_related_admissions_per_pop_beddays",
                         "readmission_within_28_days_per_pop_beddays"
        ) ~ "Frailty and vulnerability",
        indicator %in% c("ambulatory_care_conditions_acute_per_pop_beddays",
                         "ambulatory_care_conditions_chronic_per_pop_beddays",
                         "redirection_age_sex_imd_standardised_beddays",
                         "zero_los_admissions_with_no_procedures_per_pop"
        ) ~ "Avoidable or preventable hospital use",
        indicator %in% c("raid_ae_per_pop_beddays",
                         "elec_to_non_elec_admissions_ratio_beddays",
                         "delayed_discharge_percent_beddays",
                         "virtual_ward_suitable_admissions_ari_per_pop_beddays"
        ) ~ "System flows and pathways",
        indicator %in% c("workforce_in_acute_setting_percent",
                         "community_to_acute_costs_ratio",
                         "bed_days_in_nonacute_beds_percent"
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
            "nonacute" = "non-acute",
            "ari" = "(ARI)",
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
  
  return(wrangled)
}

#' Pins reference data for geographies.
#'
#' @param ref_icb Reference information for ICBs.
#' @param ref_la Reference information for LAs.
#' @param ref_nh Reference information for NHs.
#' @param board The pins board.
#'
#' @returns The contents of the pin.
pin_ref_geography <- function(ref_icb, ref_la, ref_nh, board) {
  
  wrangled <- rbind(ref_icb, ref_la, ref_nh) |>
    dplyr::arrange(geography, name) |>
    dplyr::mutate(shortname = name |>
                    stringr::str_replace_all(c("NHS " = "",
                                               " Integrated Care Board" = ""))) 
  
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

#' Pins a lookup between PCN and NH.
#'
#' @param pcn_to_nh_lookup A dataframe mapping PCNs to NHs.
#' @param gp_to_pcn_lookup A dataframe with columns of PCN codes and names.
#' @param board The pins board.
#'
#' @returns The contents of the pin.
pin_ref_pcn_to_nh <- function(pcn_to_nh_lookup, gp_to_pcn_lookup, board) {
  
  pcns <- gp_to_pcn_lookup |> 
    dplyr::select(pcn_code, pcn_name) |> 
    unique()
  
  wrangled <- pcn_to_nh_lookup |>
    dplyr::rename(pcn_code = pcn) |>
    dplyr::left_join(pcns, "pcn_code") |>
    dplyr::arrange(nnhip_site, pcn_name) |>
    dplyr::select(pcn_code, pcn_name, nnhip_code, nnhip_site)
  
  pin_name <- glue::glue("{Sys.getenv('NHNIP_CARE_SHIFT_TRACKER_BOARD_OWNER')}/nhnip-care-shift-tracker-ref-pcn_to_nh")
  
  pins::pin_write(
    board,
    x = wrangled,
    name = pin_name,
    type = "parquet",
    versioned = TRUE
  )

  return(wrangled)
}