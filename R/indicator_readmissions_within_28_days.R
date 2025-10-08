# Functions for emergency readmissions within 28 days indicators:
# `readmission_within_28_days_per_pop_admissions` and 
# `readmission_within_28_days_per_pop_beddays`.

#' Emergency readmissions within 28 days admissions/beddays by LSOA/GP code and 
#' month.
#'
#' @param age The minimum age cutoff.
#' @param start The minimum date for the query.
#' @param connection The ODBC connection.
#' @param sub_geography Either `"lsoa"` or `"gp"`.
#'
#' @returns A dataframe with the number of emergency readmissions within 28 days 
#' admissions/beddays by LSOA/GP code and month.
get_readmission_within_28_days_sub_geography <- function(sub_geography, 
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
    		a.APCE_Ident,
    		sub_geography_column,
    		convert(varchar(7), Discharge_Date, 120) AS date,
    		DATEDIFF(day, a.Admission_Date, a.Discharge_Date) AS Spelldur
    
    	FROM [Reporting_MESH_APC].[APCE_Core_Monthly_Snapshot]  a
    
    	WHERE 
        Last_Episode_In_Spell_Indicator = '1' AND
    		Discharge_Date >= 'start_date' AND
    		Age_at_End_of_Episode_SUS >= age_cutoff AND
    		LEFT(Der_Postcode_LSOA_2021_Code, 1) = 'E' AND
    		LEFT(a.Admission_Method, 1) = '2' AND
    		a.Der_Pseudo_NHS_number IS NOT NULL AND
    		(a.Spell_Core_HRG!= 'PB03Z' OR Spell_Core_HRG IS NULL) AND NOT
    		(Treatment_Function_Code = '424') AND
    		EXISTS (
    			SELECT 1
    
    			FROM [Reporting_MESH_APC].[APCE_Core_Monthly_Snapshot]  b
    
    			WHERE
      
    			 a.Der_Pseudo_NHS_Number = b.Der_Pseudo_NHS_Number AND
    			 DATEDIFF(DD, b.Discharge_Date, a.Admission_Date) BETWEEN 0 AND 28 AND
    			 (b.Admission_Date < a.Admission_Date OR
    			  b.Discharge_Date < a.Discharge_Date) AND  
    			  a.APCE_Ident != b.APCE_Ident 
    		 )
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



