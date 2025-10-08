# Functions for Mental Health Admissions via ED indicators:
# `raid_ae_per_pop_admissions` and 
# `raid_ae_per_pop_beddays`.

#' Mental Health Admissions via ED admissions/beddays by LSOA/GP code and month.
#'
#' @param age The minimum age cutoff.
#' @param start The minimum date for the query.
#' @param connection The ODBC connection.
#' @param sub_geography Either `"lsoa"` or `"gp"`.
#'
#' @returns A dataframe with the number of Mental Health Admissions via ED 
#' admissions/beddays by LSOA/GP code and month.
get_raid_ae_sub_geography <- function(sub_geography, 
                                      age, 
                                      start, 
                                      connection) {
  sub_geography_column <- get_subgeography_column(sub_geography)
  
  query <- "
    SELECT
    	date,
    	sub_geography_column,
    	COUNT(DISTINCT apce_ident) As admissions,
    	SUM(spelldur) as beddays
    
    FROM (
      SELECT
      	APCE_Ident,
      	sub_geography_column,
    		convert(varchar(7), Discharge_Date, 120) AS date,
      	DATEDIFF(day, Admission_Date, Discharge_Date) AS Spelldur
      
      FROM [Reporting_MESH_APC].[APCE_Core_Monthly_Snapshot]
      
      WHERE 
    		Discharge_Date >= 'start_date' AND
    		Age_at_End_of_Episode_SUS >= age_cutoff AND
    		LEFT(Der_Postcode_LSOA_2021_Code, 1) = 'E' AND
        Last_Episode_In_Spell_Indicator = '1' AND
      	Der_Provider_Code = 'RK9' AND
        Der_Financial_Year = '2024/25' AND
        Admission_Method = '21' AND
        Discharge_Method !='4' AND
        Der_Primary_Diagnosis_Code LIKE 'F%' AND
          (Der_Procedure_Count = 0 OR Der_Procedure_All IS NULL) 
    	 ) AS Sub
    
    GROUP BY 
    	date,
    	sub_geography_column
  " |>
    stringr::str_replace_all(
      c(
        "age_cutoff" = age,
        "start_date" = start,
        "sub_geography_column" = sub_geography_column
      )
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

#' Mental Health Admissions via ED admissions/beddays by geography and month.
#'
#' @param data The number of Mental Health Admissions via ED admissions/beddays 
#' by LSOA/GP code and month.
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#'
#' @returns A dataframe with the Mental Health Admissions via ED 
#' admissions/beddays by month and geography.
get_raid_ae_geography <- function(data, geography) {
  geography_column <- get_geography_column(geography)
  
  wrangled <- data |>
    dplyr::summarise(
      admissions = sum(admissions),
      beddays = sum(beddays),
      .by = c(date, !!rlang::sym(geography_column))
    ) |>
    dplyr::mutate(indicator = "raid_ae") |>
    dplyr::filter(!is.na(!!rlang::sym(geography_column))) |>
    dplyr::select(
      indicator,
      !!rlang::sym(geography) := !!rlang::sym(geography_column),
      date,
      admissions,
      beddays
    )
  
  return(wrangled)
}
