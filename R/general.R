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
#' @param data A dataframe as sub-geography level
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#' @param lookup The lookup between sub-geography and geography.
#'
#' @returns A dataframe containing `data` joined to the `lookup`.
join_to_geography_lookup <- function(data, geography, lookup){
  wrangled <- if(geography == "pcn") {
    data |>
      dplyr::left_join(lookup, 
                       by = c("gp_practice_sus" = "partner_organisation_code"))
  } else {
    data |>
      dplyr::left_join(lookup |>
                         dplyr::select(-geometry), 
                       by = c("der_postcode_lsoa_2021_code" = "lsoa21cd"))
  }
  
  return(wrangled)
}
