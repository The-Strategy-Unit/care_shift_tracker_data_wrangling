# Functions for the A&E frequent attenders (adult. ambulance conveyed) 
# indicator: `frequent_attenders_adult_ambulance_per_pop`.


#' Number of A&E frequent attenders by LSOA/GP code and month.
#'
#' @param sub_geography Either `"lsoa"` or `"gp"`.
#' @param age The minimum age cutoff.
#' @param start The minimum date for the query.
#' @param connection The ODBC connection.
#'
#' @returns A dataframe with the number of A&E frequent attenders by LSOA/GP 
#' code and month.
get_frequent_attenders_adult_ambulance_sub_geography_archived <- function(sub_geography, age, start, connection) {
  sub_geography_column <- if (sub_geography == "lsoa") {
    "Der_Postcode_LSOA_2011_Code"
  } else if (sub_geography == "gp") {
    "GP_Practice_Code"
  }
  
  query <- "
    WITH prior_attendances AS (
	SELECT
		a.AEA_Ident,
		sub_geography_column,
		convert(varchar(7), AEA_Departure_Date, 120) AS date,
		a.Der_Pseudo_NHS_Number,
		a.Arrival_date,
		1 as sample_rate

	FROM [Sus_AEA].[AEA_Core_1] a

	LEFT JOIN (
		SELECT
			Der_Pseudo_NHS_Number,
			Arrival_Date

		FROM [Sus_AEA].[AEA_Core_1]

		WHERE
		 AEA_Attendance_Category = '1' OR
		 AEA_Attendance_Category IS NULL OR 
		 AEA_Attendance_Category = '' 
	 ) b
	 ON
		a.Der_Pseudo_NHS_Number = b.Der_Pseudo_NHS_Number AND
		DATEADD(YY, 1, b.Arrival_Date) >= a.Arrival_Date AND
		b.Arrival_Date < a.Arrival_Date

	WHERE
		AEA_Departure_Date >= 'start_date' AND
		AEA_Departure_Date < '2019-04-01' AND
    Der_Age_at_CDS_Activity_Date >= age_cutoff AND
		a.AEA_Arrival_Mode LIKE '1' AND
		a.AEA_Attendance_Category != '4' AND
		a.Der_Pseudo_NHS_number IS NOT NULL
),

count_table AS(
	SELECT
		AEA_Ident,
		date,
		sub_geography_column,
		sum(sample_rate) as count

	FROM prior_attendances as a

	GROUP BY
		AEA_Ident,
		date,
		sub_geography_column
  )

SELECT
	date,
	sub_geography_column,
	COUNT(DISTINCT AEA_Ident) AS frequent_attenders

FROM count_table

WHERE count>= 3

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

#' Number of A&E frequent attenders by LSOA/GP code and month.
#'
#' @param sub_geography Either `"lsoa"` or `"gp"`.
#' @param age The minimum age cutoff.
#' @param lag The maximum date for the query.
#' @param connection The ODBC connection.
#'
#' @returns A dataframe with the number of A&E frequent attenders by LSOA/GP 
#' code and month.
get_frequent_attenders_adult_ambulance_sub_geography_current <- function(sub_geography, 
                                                                 age, 
                                                                 lag,
                                                                 connection) {
  sub_geography_column <- if (sub_geography == "lsoa") {
    "Der_Postcode_LSOA_2011_Code"
  } else if (sub_geography == "gp") {
    "GP_Practice_Code"
  }
  
  query <- "
    WITH prior_attendances AS (
	SELECT
		a.AEA_Ident,
		sub_geography_column, 
		convert(varchar(7), EC_Departure_Date, 120) AS date,
		a.Der_Pseudo_NHS_Number,
		a.Arrival_date,
		1 as sample_rate

	FROM [Reporting_MESH_ECDS].[AEA_EC_Combined] a

	LEFT JOIN (
		SELECT 
			Der_Pseudo_NHS_Number,
			Arrival_Date
 
		FROM [Reporting_MESH_ECDS].[AEA_EC_Combined] 
 
		WHERE 
		 EC_AttendanceCategory = '1' OR 
		 EC_AttendanceCategory IS NULL OR 
		 EC_AttendanceCategory = '' 
	 ) b 
	 ON  
		a.Der_Pseudo_NHS_Number = b.Der_Pseudo_NHS_Number AND
		DATEADD(YY, 1, b.Arrival_Date) >= a.Arrival_Date AND
		b.Arrival_Date < a.Arrival_Date 

	WHERE 
		EC_Departure_Date >= '2019-04-01' AND
		EC_Departure_Date < 'lag_date' AND
    Der_Age_at_CDS_Activity_Date >= age_cutoff AND
		a.AEA_Arrival_Mode LIKE '1' AND
		a.EC_AttendanceCategory != '4' AND
		a.Der_Pseudo_NHS_number IS NOT NULL
),

count_table AS(
	SELECT 
		AEA_Ident,
		date,
		sub_geography_column,
		sum(sample_rate) as count

	FROM prior_attendances as a

	GROUP BY
		AEA_Ident,
		date,
		sub_geography_column
  )
    
SELECT
	date,
	sub_geography_column,
	COUNT(DISTINCT AEA_Ident) AS frequent_attenders
  
FROM count_table
  
WHERE count>= 3

GROUP BY 
	date,
	sub_geography_column
  " |>
    stringr::str_replace_all(
      c("age_cutoff" = age,
        "lag_date" = lag,
        "sub_geography_column" = sub_geography_column
      )
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

#' Number of A&E frequent attenders by geography and month.
#'
#' @param data The number of A&E frequent attenders by LSOA/GP code and month.
#' @param geography The geography of interest: `"icb"`, `"la"` or `"pcn"`.
#'
#' @returns A dataframe with the number of A&E frequent attenders by month and 
#' geography.
get_frequent_attenders_adult_ambulance_geography <- function(data, geography) {
  geography_column <- get_geography_column(geography)
  
  column_to_sum <- if("frequent_attenders_amended" %in% names(data)){
    "frequent_attenders_amended"
  } else {
    "frequent_attenders"
  }
  
  wrangled <- data |>
    dplyr::summarise(
      attenders = sum(!!rlang::sym(column_to_sum)),
      .by = c(date, !!rlang::sym(geography_column))
    ) |>
    dplyr::mutate(indicator = glue::glue("frequent_attenders_adult_ambulance")) |>
    dplyr::filter(!is.na(!!rlang::sym(geography_column))) |>
    dplyr::select(
      indicator,
      !!rlang::sym(geography) := !!rlang::sym(geography_column),
      date,
      attenders
    )
  
  return(wrangled)
}
