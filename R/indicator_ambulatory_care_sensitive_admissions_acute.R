# Functions for ambulatory care condition indicators:
# `ambulatory_care_conditions_acute_per_pop_admissions`,
# `ambulatory_care_conditions_acute_per_pop_beddays`,
# `ambulatory_care_conditions_chronic_per_pop_admissions` and
# `ambulatory_care_conditions_chronic_per_pop_beddays`.

#' Get the SQL WHERE clause for either acute or chronic ambulatory care 
#' conditions.
#'
#' @param condition Either `"acute"` or `"chronic"`.
#'
#' @returns A string containing part of a SQL WHERE clause to filter the query
#' to either acute or chronic ambulatory care conditions.
get_ambulatory_care_conditions_where_clause <- function(condition) {
  sql <- if (condition == "acute") {
    "(
  ((Der_Diagnosis_All LIKE '||L0[34]%' OR
  Der_Diagnosis_All LIKE '||L08[089]%' OR
  Der_Diagnosis_All LIKE '||L88X%' OR
  Der_Diagnosis_All LIKE '||L980%' ) AND
  (Der_Procedure_All NOT LIKE '%[ABCDEFGHJKLMNOPQRTVW]%' AND
  Der_Procedure_All NOT LIKE '%S[123]%' AND
  Der_Procedure_All NOT LIKE '%S4[1234589]%' AND
  Der_Procedure_All NOT LIKE '%X0[1245]%' ) ) ----   cellulitis
  OR
  (Der_Diagnosis_All LIKE '%R02X%' )  ---gangrene
  OR
  (Der_Diagnosis_All LIKE '||G4[01]%' OR
    Der_Diagnosis_All LIKE '||O15%' OR
    Der_Diagnosis_All LIKE '||R56%' ) ---Convulsions and epilepsy
  OR
  (Der_Diagnosis_All LIKE '||E86X%' OR
    Der_Diagnosis_All LIKE '||K52[289]%') ---dehydration_and_gastroenteritis
  OR
  (Der_Diagnosis_All LIKE '||A690)%' OR
    Der_Diagnosis_All LIKE '||K0[2-68]%' OR
    Der_Diagnosis_All LIKE '||K09[89]%' OR
    Der_Diagnosis_All LIKE '||K1[23]%' )  --- dental_conditions
  OR
  (Der_Diagnosis_All LIKE '||H6[67]%' OR
    Der_Diagnosis_All LIKE '||J0[236]%' OR
    Der_Diagnosis_All LIKE '||J312%' ) --ent_infections
  OR
  (Der_Diagnosis_All LIKE '||N7[034]%') ---pelvic_inflammatory_disease
  OR
  (Der_Diagnosis_All LIKE '||K2[5678][012456]%') ---perforated_bleeding_ulcer
  OR
  (Der_Diagnosis_All LIKE '||N1[012]%' OR
    Der_Diagnosis_All LIKE '||N136%' ) ---pyelonephritis
  )
"
  } else if (condition == "chronic") {
    "(
  ((Der_Diagnosis_All LIKE '||I20%' OR
  Der_Diagnosis_All LIKE '||I24[089]%' ) AND
  (Der_Procedure_All NOT LIKE '||[ABCDEFGHJKLMNOPQRSTVW]%' AND
  Der_Procedure_All NOT LIKE '||X0[1245]%' ) ) OR ----   angina
    (Der_Diagnosis_All LIKE '||J4[56]%') OR ---asthma
  ((Der_Diagnosis_All LIKE '||I110%' OR
  Der_Diagnosis_All LIKE '||I50%' OR
  Der_Diagnosis_All LIKE '||I10%' OR
  Der_Diagnosis_All LIKE '||I119%' OR
  Der_Diagnosis_All LIKE '||J81%' ) AND
  (Der_Procedure_All NOT LIKE '%K[0-4]%' AND
  Der_Procedure_All NOT LIKE '%K5[02567]%' AND
  Der_Procedure_All NOT LIKE '%K6[016789]%'  AND
  Der_Procedure_All NOT LIKE '%K71)%' )  ) OR   -----congestive_heart_failure / hypertension
  (Der_Diagnosis_All LIKE '||J4[12347]%' OR
  (Der_Diagnosis_All LIKE '||J20%' AND
  Der_Diagnosis_All LIKE '%J4[12347]%' )) OR ----   copd
  (Der_Diagnosis_All LIKE '%E1[01234][012345678]%' ) OR ----  diabetes_complications
  (Der_Diagnosis_All LIKE '||D50[189]%') OR ---iron-deficiency_anaemia
    (Der_Diagnosis_All LIKE '||E4[0123]X%' OR
    Der_Diagnosis_All LIKE '||E550%' OR
    Der_Diagnosis_All LIKE '||E643)%') --- nutritional_deficiencies
  )"
  } else {
    "Please choose an ambulatory care condition: 'acute' or 'chronic'."
  }
  
  return(sql)
  
}

#' The number of admissions/beddays for ambulatory care conditions 
#' (acute/chronic) by LSOA/GP code and month.
#'
#' @param age The minimum age cutoff.
#' @param start The minimum date for the query.
#' @param connection The ODBC connection.
#' @param sub_geography Either `"lsoa"` or `"gp"`.
#' @param condition Either `"acute"` or `"chronic"`.
#'
#' @returns A dataframe of the number of admissions/beddays for ambulatory care 
#' conditions (acute/chronic) by LSOA/GP code and month.
get_ambulatory_care_conditions_sub_geography <- function(sub_geography,
                                                         age,
                                                         start,
                                                         condition,
                                                         connection) {
  sub_geography_column <- get_subgeography_column(sub_geography)
  
  where_clause <- get_ambulatory_care_conditions_where_clause(condition)
  
  query <- "
    SELECT
    	date,
    	sub_geography_column,
    	COUNT(DISTINCT apcs_ident) As admissions,
    	SUM(spelldur) as beddays

    FROM (
    	SELECT
    		a.APCS_Ident,
    		sub_geography_column,
    		convert(varchar(7), Discharge_Date, 120) AS date,
    		DATEDIFF(day, a.Admission_Date, a.Discharge_Date) AS Spelldur

    	FROM [Reporting_MESH_APC].[APCS_Core_Monthly_Snapshot]  a

    	WHERE
    		Discharge_Date >= 'start_date' AND
    		Age_at_End_of_Spell_SUS >= age_cutoff AND
    		LEFT(Der_Postcode_LSOA_2021_Code, 1) = 'E' AND
    		LEFT(a.Admission_Method, 1) = '2' AND
    		where_clause
    	 ) AS Sub

    GROUP BY
    	date,
    	sub_geography_column
  " |>
    stringr::str_replace_all(
      c(
        "age_cutoff" = age,
        "start_date" = start,
        "sub_geography_column" = sub_geography_column,
        "where_clause" = where_clause
      )
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}


