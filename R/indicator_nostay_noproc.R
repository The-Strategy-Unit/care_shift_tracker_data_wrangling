#' Functions for the inpatient admissions with zero LoS and no procedures indicator

#' The admissions by either lsoa or practice by month.
#'
#' @param connection The ODBC connection.
#' @param start beginning of time series cut-off as per targets settings.
#' @param lag end of time series cut-off as per targets lag settings.
#' @returns A dataframe with the number of bed days by lsoa and practice.

get_nostaynoproc_data_lsoa <- function(connection, start, lag) {
  
  query <- "
  SET NOCOUNT ON;
  
    with cte as
      (
      select [Der_Postcode_LSOA_2011_Code] as lsoa_2011, der_financial_year,
      cast(datepart(yyyy,[Discharge_Date]) as varchar) + case when datepart(mm,[Discharge_Date]) < 10 then '-0' else '-' end + cast(datepart(mm,[Discharge_Date]) as varchar) as der_activity_month,
      count(*) as adms

    from [Reporting_MESH_APC].[APCS_Core_Monthly_Snapshot]
      where 1=1
      and Discharge_Date >= 'start_date'
      and Discharge_Date < 'lag_date' --finished admissions before lag date only
      and left([Admission_Method],1) = '2' --emergency admission
      and [Der_Spell_LoS] < 1 --no overnight stay
      and [Der_Procedure_Count] < 1 or Der_Procedure_Count is NULL --no procedures
      and [Der_Age_at_CDS_Activity_Date] >= 65 --proxy for frail
      and Der_Postcode_LSOA_2011_Code is not NULL --no missing geography

    group by [Der_Postcode_LSOA_2011_Code], der_financial_year,
      cast(datepart(yyyy,[Discharge_Date]) as varchar) + case when datepart(mm,[Discharge_Date]) < 10 then '-0' else '-' end + cast(datepart(mm,[Discharge_Date]) as varchar)
      )

    select * from cte
    order by lsoa_2011, der_financial_year, der_activity_month
  " |>
    stringr::str_replace_all(
      c("start_date" = start,
        "lag_date" = lag
      )
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

get_nostaynoproc_data_prac <- function(connection, start, lag) {
  
  query <- "
  SET NOCOUNT ON;
  
    with cte as
      (
      select [GP_Practice_Code] as gp_prac, der_financial_year,
      cast(datepart(yyyy,[Discharge_Date]) as varchar) + case when datepart(mm,[Discharge_Date]) < 10 then '-0' else '-' end + cast(datepart(mm,[Discharge_Date]) as varchar) as der_activity_month,
      count(*) as adms

    from [Reporting_MESH_APC].[APCS_Core_Monthly_Snapshot]
      where 1=1
      and Discharge_Date >= 'start_date'
      and Discharge_Date < 'lag_date' --finished admissions before lag date only
      and left([Admission_Method],1) = '2' --emergency admission
      and [Der_Spell_LoS] < 1 --no overnight stay
      and [Der_Procedure_Count] < 1 or Der_Procedure_Count is NULL --no procedures
      and [Der_Age_at_CDS_Activity_Date] >= 65 --proxy for frail
      and GP_Practice_Code is not NULL --no missing practices

    group by [GP_Practice_Code], der_financial_year,
      cast(datepart(yyyy,[Discharge_Date]) as varchar) + case when datepart(mm,[Discharge_Date]) < 10 then '-0' else '-' end + cast(datepart(mm,[Discharge_Date]) as varchar)
      )

    select * from cte
    order by gp_prac, der_financial_year, der_activity_month
  " |>
    stringr::str_replace_all(
      c("start_date" = start,
        "lag_date" = lag
      )
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

#' Mapping data to geographies, then populations, then calculating the rates
#'
#' @param data The target object with 'raw' data by lsoa (or practice)
#' @param lookup The target object with 2011 to 2021 lookups (lsoa only)
#' @param geog  The target object to assign lsoa to ICB/LAD or practice to PCN
#' @param pop The target object with the 65+ population data in 
#' @param latest_population_year The latest year that population data is
#' available for.
#' @returns A dataframe with the rate of admissions by icb, lad or pcn.
 
# icb
zero_los_no_proc_icb <- function(data,lookup,geog,pop, latest_population_year) {
  
#wrangle and calcs
df <- data |>
  filter(!is.na(lsoa_2011)) |>
  left_join(lookup |> select(lsoa11cd, lsoa21cd),
            by = c("lsoa_2011" = "lsoa11cd"), relationship = "many-to-many") |>
  left_join(geog |>
              select(lsoa21cd, icb24cdh, icb24nm), by = "lsoa21cd") |>
  group_by(icb24cdh, der_financial_year, der_activity_month) |>
  summarise(total = sum(adms)) |>
  ungroup() |>
  dplyr::rename(date = der_activity_month, icb = icb24cdh) |>
  join_to_population_data(pop, "icb", latest_population_year) |>
  PHEindicatormethods::phe_rate(x = total,
                                n = population_size,
                                multiplier = 100000) |>

  dplyr::mutate(dplyr::across(c(value, lowercl, uppercl),
                              ~janitor::round_half_up(.)),
                indicator = "zero_los_admissions_with_no_procedures_per_pop") |>
  dplyr::select(
    indicator,
    date,
    icb,
    numerator = total,
    denominator = population_size,
    value,
    lowercl,
    uppercl)  |>
  dplyr::mutate(frequency = "monthly") |>
  arrange(icb, date)

return(df)
}

# lad
zero_los_no_proc_la <- function(data,lookup,geog,pop, latest_population_year) {
  
  #wrangle and calcs
  df <- data |>
    filter(!is.na(lsoa_2011)) |>
    left_join(lookup |> select(lsoa11cd, lsoa21cd),
              by = c("lsoa_2011" = "lsoa11cd"), relationship = "many-to-many") |>
    left_join(geog |>
                select(lsoa21cd,lad24cd,lad24nm), by = "lsoa21cd") |>
    group_by(lad24cd, der_financial_year, der_activity_month) |>
    summarise(total = sum(adms)) |>
    ungroup() |>
    dplyr::rename(
      la = lad24cd,
      date = der_activity_month) |>
    join_to_population_data(pop, "la", latest_population_year) |>
    PHEindicatormethods::phe_rate(x = total,
                                  n = population_size,
                                  multiplier = 100000) |>
    
    dplyr::mutate(dplyr::across(c(value, lowercl, uppercl),
                                ~janitor::round_half_up(.)),
                  indicator = "zero_los_admissions_with_no_procedures_per_pop") |>
    dplyr::select(
      indicator,
      date,
      la,
      numerator = total,
      denominator = population_size,
      value,
      lowercl,
      uppercl)  |>
    dplyr::mutate(frequency = "monthly") |>
    arrange(la, date)
  
  return(df)
}

# nh
zero_los_no_proc_nh <- function(data,lookup,pop, lookup_pcn_nh) {
  
  #wrangle and calcs
  df <- data |>
    filter(!is.na(gp_prac), der_activity_month >= '2013-04') |>
    left_join(lookup |> select (partner_organisation_code,pcn_code),
              by = c("gp_prac" = "partner_organisation_code"), relationship = "many-to-many") |>
    get_nh_from_pcn(lookup_pcn_nh) |>
    group_by(nnhip_code, der_activity_month) |>
    summarise(total = sum(adms)) |>
    ungroup() |>
    filter(!is.na(nnhip_code)) |>
    left_join(pop, by = c("nnhip_code","der_activity_month" = "date")) |>
    filter(total <= population_size, !is.na(population_size)) |>
    
    PHEindicatormethods::phe_rate(x = total,
                                  n = population_size,
                                  multiplier = 100000) |>
    
    dplyr::mutate(dplyr::across(c(value, lowercl, uppercl),
                                ~janitor::round_half_up(.)),
                  indicator = "zero_los_admissions_with_no_procedures_per_pop") |>
    dplyr::rename(
      nh = nnhip_code,
      date = der_activity_month) |>
    dplyr::select(
      indicator,
      date,
      nh,
      numerator = total,
      denominator = population_size,
      value,
      lowercl,
      uppercl)  |>
    dplyr::mutate(frequency = "monthly") |>
    arrange(nh, date)
  
  return(df)
}
