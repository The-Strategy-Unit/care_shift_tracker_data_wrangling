# Functions for the elective to non elective ratio indicator:
# `elec_non_elec_ratio_admissions` and `elec_to_non_elec_admissions_ratio_beddays`.

#' The number of elective and non elective admissions/beddays by LSOA/GP and 
#' month.
#' 
#' @param sub_geography Either `"lsoa"` or `"gp"`.
#' @param age The minimum age cutoff.
#' @param start The minimum date for the query.
#' @param lag The maximum date for the query.
#' @param connection The ODBC connection.
#'
#' @returns A dataframe with the number of elective and non elective 
#' admissions/beddays by LSOA/GP code and month.
get_elective_non_elective_sub_geography <- function(sub_geography, 
                                                    age, 
                                                    start, 
                                                    lag,
                                                    connection) {
  sub_geography_column <- get_subgeography_column(sub_geography)
  
  query <- "
    SELECT date,
      sub_geography_column,
      admission_type,
      COUNT(DISTINCT apce_ident) As admissions,
      SUM(case
          when spelldur = 0 OR Patient_Classification in ('2','3','4') then 0.5
          else spelldur end) as beddays

    FROM (SELECT
            apce_ident,
            Patient_Classification,
            convert(varchar(7), Discharge_Date, 120) AS date,
            sub_geography_column,
            CASE  WHEN Admission_Method IN ('11', '12', '13') THEN 'elective'
                  WHEN Admission_Method LIKE '2%' THEN 'non_elective'
                  ELSE 'other' END AS admission_type,
            DATEDIFF(day, Admission_Date, Discharge_Date) AS Spelldur
      
          FROM Reporting_MESH_APC.APCE_Core_Monthly_Snapshot AS apce
      
          WHERE Last_Episode_In_Spell_Indicator = '1'
            AND Discharge_Date >= 'start_date'
            AND Discharge_Date < 'lag_date'
            AND Der_Age_at_CDS_Activity_Date >= age_cutoff
          ) as sub
    
    WHERE admission_type IN ('elective', 'non_elective')
    
    GROUP BY
      date,
      sub_geography_column,
      admission_type
  " |>
    stringr::str_replace_all(
      c("age_cutoff" = age,
        "start_date" = start,
        "lag_date" = lag,
        "sub_geography_column" = sub_geography_column
      )
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

#' Elective to non-elective admissions/beddays ratio by month and geography.
#'
#' @param data The number of elective and non elective admissions by LA, ICB or
#' PCN and month.
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#' @param activity_type Either `"admissions"` or `"beddays"`.
#'
#' @returns A dataframe with the elective to non-elective admissions/beddays 
#' ratio by month and geography.
get_elective_non_elective_ratio <- function(data, geography, activity_type) {
  geography_column <- get_geography_column(geography)
  
  wrangled <- data |>
    tidyr::pivot_wider(names_from = admission_type,
                       values_from = !!rlang::sym(activity_type)) |>
    dplyr::summarise(
      elective = sum(elective, na.rm = TRUE),
      non_elective = sum(non_elective, na.rm = TRUE),
      .by = c(date, !!rlang::sym(geography_column))
    ) |>
    dplyr::mutate(indicator = glue::glue("elec_to_non_elec_admissions_ratio_{activity_type}")) |>
    dplyr::filter(
      !is.na(!!rlang::sym(geography_column)),
      non_elective > 0, # denominator cannot be 0
      elective >= 0 # numbers should be positive
      ) |>
    PHEindicatormethods::phe_rate(x=elective, n=non_elective, multiplier = 1) |>
    dplyr::select(
      indicator,
      !!rlang::sym(geography) := !!rlang::sym(geography_column),
      date,
      numerator = elective,
      denominator = non_elective,
      value,
      lowercl,
      uppercl
    )
  
  return(wrangled)
}