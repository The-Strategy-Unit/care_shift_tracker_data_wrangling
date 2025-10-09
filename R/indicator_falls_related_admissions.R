# Functions for emergency hospital admissions/beddays due to falls in people 
# over 65 indicators: `falls_related_admissions_per_pop_admissions` and 
# `falls_related_admissions_per_pop_beddays`.

#' Emergency hospital admissions/beddays due to falls in people over 65 by 
#' LSOA/GP code and month.
#'
#' @param start The minimum date for the query.
#' @param connection The ODBC connection.
#' @param sub_geography Either `"lsoa"` or `"gp"`.
#'
#' @returns A dataframe with the number of emergency hospital admissions/beddays
#' due to falls in people over 65 by LSOA/GP code and month.
get_falls_related_admissions_sub_geography <- function(sub_geography, 
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
        Last_Episode_In_Spell_Indicator = '1' AND
      	Discharge_Date >= 'start_date' AND
    		LEFT(Der_Postcode_LSOA_2021_Code, 1) = 'E' AND
        Der_Age_at_CDS_Activity_Date >= 65 AND
        LEFT(Admission_Method, 1) = '2' AND
        LEFT(Der_Primary_Diagnosis_Code, 4) = 'R296'
    	 ) AS Sub
    
    GROUP BY 
    	date,
    	sub_geography_column
  " |>
    stringr::str_replace_all(
      c("start_date" = start,
        "sub_geography_column" = sub_geography_column
      )
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}
