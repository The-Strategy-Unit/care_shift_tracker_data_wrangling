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
    		Der_Age_at_CDS_Activity_Date >= age_cutoff AND
        Last_Episode_In_Spell_Indicator = '1' AND
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
      c("age_cutoff" = age,
        "start_date" = start,
        "sub_geography_column" = sub_geography_column
      )
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}


