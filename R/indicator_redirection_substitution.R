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
get_other_redirection_indicators_sub_geography <- function(
    age, 
    start, 
    zero_los_no_procedure,
    medicines_related_admissions,
    connection) {
  
  query <- "
    SELECT
    	APCE_Ident,
    	Der_Postcode_LSOA_2021_Code,
    	GP_Practice_SUS,
    	convert(varchar(7), Discharge_Date, 120) AS date,
      DATEDIFF(day, Admission_Date, Discharge_Date) AS Spelldur
    
    FROM [Reporting_MESH_APC].[APCE_Core_Monthly_Snapshot]
    
    WHERE 
      Last_Episode_In_Spell_Indicator = '1' AND
      Discharge_Date >= 'start_date' AND
      Der_Age_at_CDS_Activity_Date >= age_cutoff AND
      LEFT(Der_Postcode_LSOA_2021_Code, 1) = 'E' AND
      LEFT(Admission_Method, 1) = '2' AND
      (zero_los_no_procedure) OR
      (medicines_related_admissions)
  " |>
    stringr::str_replace_all(
      c("age_cutoff" = age,
        "start_date" = start,
        "zero_los_no_procedure" = zero_los_no_procedure,
        "medicines_related_admissions" = medicines_related_admissions
      )
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}



