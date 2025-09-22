# General functions.
# 
#' Get the table column name related to the geography.
#'
#' @param geography  The geography of interest: `"icb"`, `"la"` or `"pcn"`.
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