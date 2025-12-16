#' Get the percentage change between current data and 12 months ago for each 
#' indicator.
#'
#' @param data A dataframe of indicator data over time for a geography. 
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#'
#' @returns A dataframe of the latest value, value of 12 months ago and the 
#' percentage change by indicator and geography.
get_perc_change <- function(data, geography) {
  data <- data |>
    dplyr::rename(code = !!rlang::sym(geography),
                  name = !!rlang::sym(glue::glue("{geography}_name")))
  
  data_latest <- data |>
    dplyr::filter(date == max(date, na.rm = TRUE), 
                  .by = indicator) |>
    dplyr::select(indicator, 
                  code,
                  name, 
                  latest_date = date,
                  latest_value = value)
  
  data_twelve_months_ago <- data |>
    dplyr::filter(date == (max(date, na.rm = TRUE) - lubridate::years(1)), 
                  .by = indicator) |>
    dplyr::select(indicator, 
                  code, 
                  twelve_months_ago = value)
  
  data_change <- data_latest |>
    dplyr::left_join(data_twelve_months_ago,
                     by = c("indicator", "code")) |>
    dplyr::mutate(
      perc_change = ((latest_value - twelve_months_ago) * 100 / 
                       twelve_months_ago) |>
        janitor::round_half_up(2),
      geography = geography
    ) 
  
  return(data_change)
}