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
        (LEFT(Der_Primary_Diagnosis_Code, 4) = 'R296' OR
          ((Der_Primary_Diagnosis_Code LIKE 'S%' OR 
            Der_Primary_Diagnosis_Code LIKE 'T%') AND
            Der_Diagnosis_All LIKE '%W[01]%') OR   ----explicit_fractures
          ((Der_Diagnosis_All LIKE '%M48[45]%' OR  
          Der_Diagnosis_All LIKE '%M80[01234589]%' OR  
          Der_Diagnosis_All LIKE '%S22[01]%' OR 
          Der_Diagnosis_All LIKE '%S32[012347]%' OR     
          Der_Diagnosis_All LIKE '%S42[234]%' OR     
          Der_Diagnosis_All LIKE '%S52%' OR     
          Der_Diagnosis_All LIKE '%S620%' OR    
          Der_Diagnosis_All LIKE '%S72[012348]%' OR  
          Der_Diagnosis_All LIKE '%T08X%' ) AND 
          Der_Diagnosis_All NOT LIKE '%[VWXY]%') ----implicit_fractures
        )
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
