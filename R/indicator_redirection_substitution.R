# Functions for emergency readmissions within 28 days indicators:
# `readmission_within_28_days_per_pop_admissions` and 
# `readmission_within_28_days_per_pop_beddays`.

get_end_of_life_episodes <- function(age, 
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
  
  	FROM [Reporting_MESH_APC].[APCE_Core_Monthly_Snapshot] AS a
  
  	WHERE 
      Last_Episode_In_Spell_Indicator = '1' AND
  		Discharge_Date >= 'start_date' AND
  		Der_Age_at_CDS_Activity_Date >= age_cutoff AND
  		Discharge_Method = '4' AND
  		DATEDIFF(day, Admission_Date, Discharge_Date) <= 14 AND
  		(Der_Diagnosis_All LIKE '%,[V-Y]%'
  		OR LEFT(Der_Diagnosis_All, 1) LIKE '[V-X]')
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

get_indicators_age_sex_standardised_rates <- function(data,
                                                      population,
                                                      geography,
                                                      latest_population_year,
                                                      activity_type,
                                                      standard_pop) {
  wrangled <- data |>
    join_to_population_data_by_age_sex(population, geography, latest_population_year) |>
    dplyr::filter(!is.na(population_size),
                  population_size > 0) |>
    # There were <5 patients with a discharge date before their admission date
    # in one indicator at GP level. So the line below is to exclude these rows:
    dplyr::filter(!!rlang::sym(activity_type) >= 0) |> 
    dplyr::left_join(standard_pop, 
                     by = c("age_range", "sex")) |>
    dplyr::group_by(date, !!rlang::sym(geography)) |>
    dplyr::rename(x = !!rlang::sym(activity_type)) |>
    PHEindicatormethods::calculate_dsr(x = x,
                                       n = population_size,
                                       stdpop = pop) |>
    dplyr::ungroup() |>
    dplyr::mutate(dplyr::across(c(value, lowercl, uppercl), 
                                ~janitor::round_half_up(.)),
                  indicator = glue::glue("redirection_age_sex_standardised_{activity_type}")) |>
    dplyr::select(
      indicator,
      date,
      !!rlang::sym(geography),
      numerator = total_count,
      denominator = total_pop,
      value,
      lowercl,
      uppercl
    )
  
  return(wrangled)
}

