# Functions for emergency readmissions within 28 days indicators:
# `readmission_within_28_days_per_pop_admissions` and 
# `readmission_within_28_days_per_pop_beddays`.

#' Emergency readmissions within 28 days admissions/beddays by LSOA/GP code and 
#' month.
#'
#' @param age The minimum age cutoff.
#' @param start The minimum date for the query.
#' @param connection The ODBC connection.
#'
#' @returns A dataframe with the number of emergency readmissions within 28 days 
#' admissions/beddays by LSOA/GP code and month.
get_readmission_within_28_days_episodes <- function(age, 
                                                    start, 
                                                    connection) {
  query <- "
  	SELECT
  		APCE_Ident,
    	Der_Postcode_LSOA_2021_Code,
    	GP_Practice_SUS,
    	convert(varchar(7), Discharge_Date, 120) AS date,
    	Sex,
      CASE WHEN Der_Age_At_CDS_Activity_Date BETWEEN 0 AND 4 THEN '0-4'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 5 AND 9 THEN '5-9'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 10 AND 14 THEN '10-14'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 15 AND 19 THEN '15-19'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 20 AND 24 THEN '20-24'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 25 AND 29 THEN '25-29'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 30 AND 34 THEN '30-34'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 35 AND 39 THEN '35-39'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 40 AND 44 THEN '40-44'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 45 AND 49 THEN '45-49'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 50 AND 54 THEN '50-54'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 55 AND 59 THEN '55-59'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 60 AND 64 THEN '60-64'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 65 AND 69 THEN '65-69'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 70 AND 74 THEN '70-74'
           WHEN Der_Age_At_CDS_Activity_Date BETWEEN 75 AND 79 THEN '75-79'
           WHEN Der_Age_At_CDS_Activity_Date >=80 THEN '80+'
           ELSE NULL
           END AS age_range,
      DATEDIFF(day, Admission_Date, Discharge_Date) AS Spelldur
  
  	FROM [Reporting_MESH_APC].[APCE_Core_Monthly_Snapshot]  a
  
  	WHERE 
      Last_Episode_In_Spell_Indicator = '1' AND
  		Discharge_Date >= 'start_date' AND
  		Der_Age_at_CDS_Activity_Date >= age_cutoff AND
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
  " |>
    stringr::str_replace_all(
      c("age_cutoff" = age,
        "start_date" = start
      )
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

