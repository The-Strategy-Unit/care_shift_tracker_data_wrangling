#' Functions for the inpatient admissions with zero LoS and no procedures indicator

#' The admissions by either lsoa or practce by month.
#'
#' @param connection The ODBC connection.
#' @returns A dataframe with the number of bed days by lsoa and practice.

get_nostaynoproc_data_lsoa <- function(connection) {
  
  query <- "
  SET NOCOUNT ON;
  
    with cte as
      (
      select [Der_Postcode_LSOA_2011_Code] as lsoa_2011,
      der_financial_year, left([Der_Activity_Month],4) + '-' + right([Der_Activity_Month],2) as der_activity_month,
      count(*) as adms

    from [Reporting_MESH_APC].[APCS_Core_Monthly_Snapshot]
      where 1=1
      and Discharge_Date is not NULL --finished admissions only
      and left([Admission_Method],1) = '2' --emergency admission
      and [Der_Spell_LoS] < 1 --no overnight stay
      and [Der_Procedure_Count] < 1 or Der_Procedure_Count is NULL --no procedures
      and [Der_Age_at_CDS_Activity_Date] >= 65 --proxy for frail
      and Der_Postcode_LSOA_2011_Code is not NULL --no missing geography

    group by [Der_Postcode_LSOA_2011_Code],
    der_financial_year, left([Der_Activity_Month],4) + '-' + right([Der_Activity_Month],2)
      )

    select * from cte
    order by lsoa_2011, der_financial_year, der_activity_month
  "
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

get_nostaynoproc_data_prac <- function(connection) {
  
  query <- "
  SET NOCOUNT ON;
  
    with cte as
      (
      select [GP_Practice_Code] as gp_prac,
      der_financial_year, left([Der_Activity_Month],4) + '-' + right([Der_Activity_Month],2) as der_activity_month,
      count(*) as adms

    from [Reporting_MESH_APC].[APCS_Core_Monthly_Snapshot]
      where 1=1
      and Discharge_Date is not NULL --finished admissions only
      and left([Admission_Method],1) = '2' --emergency admission
      and [Der_Spell_LoS] < 1 --no overnight stay
      and [Der_Procedure_Count] < 1 or Der_Procedure_Count is NULL --no procedures
      and [Der_Age_at_CDS_Activity_Date] >= 65 --proxy for frail
      and GP_Practice_Code is not NULL --no missing practices

    group by [GP_Practice_Code],
    der_financial_year, left([Der_Activity_Month],4) + '-' + right([Der_Activity_Month],2)
      )

    select * from cte
    order by gp_prac, der_financial_year, der_activity_month
  "
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

#' Mapping data to geographies, then populations, then calculating the rates
#'
#' @param data The target object with 'raw' data by lsoa (or practice)
#' @param lookup The target object with 2011 to 2021 lookups (lsoa only)
#' @param geog  The target object to assign lsoa to ICB/LAD or practice to PCN
#' @param pop The target object with the unrefined population data in 
#' @returns A dataframe with the rate of admissions by lsoa and practice.
 
# icb
zero_los_no_proc_icb <- function(data,lookup,geog,pop) {
#pre-query wrangles
  pops_base <- pop |>
  filter(age_range %in% c("65-69","70-74","75-79","80+")) |>
  mutate(
    pop_yr_plus = as.numeric(population_year)+1,
    der_financial_year = paste0(population_year,'/',str_sub(as.character(pop_yr_plus), start = -2)))
  group_by(population_year,icb,age_range,sex,der_financial_year) |>
    summarise(population_size = sum(population_size)) |>
    ungroup()

pops_2526 <- pops_base |>
  filter(population_year == "2024") |>
  mutate(population_year = "2025",
         der_financial_year = "2025/26")

pops_final <- bind_rows(pops_base, pops_2526) |>
  group_by(icb, der_financial_year) |>
  summarise(population = sum(population_size)) |>
  ungroup() |>
  filter(!is.na(icb))

#actual wrangle and calcs
df <- data |>
  filter(!is.na(lsoa_2011)) |>
  left_join(lookup |> select (1,2),
            by = c("lsoa_2011" = "lsoa11cd"), relationship = "many-to-many") |>
  left_join(geog |>
              select(1,7,8), by = "lsoa21cd") |>
  group_by(icb24cdh, der_financial_year, der_activity_month) |>
  summarise(total = sum(adms)) |>
  ungroup() |>
  left_join(pops_final, by = c("icb24cdh" = "icb","der_financial_year" = "der_financial_year")) |>
  
  PHEindicatormethods::phe_rate(x = total,
                                n = population,
                                multiplier = 100000) |>
  
  dplyr::mutate(dplyr::across(c(value, lowercl, uppercl), 
                              ~janitor::round_half_up(.)),
                indicator = "zero_los_no_procedures") |>
  dplyr::rename(
    icb = icb24cdh,
    date = der_activity_month) |>
  dplyr::select(
    indicator,
    date,
    icb,
    numerator = total,
    denominator = population,
    value,
    lowercl,
    uppercl)  |>
  dplyr::mutate(frequency = "monthly",
                date = ymd(paste0(date, "-01"))) |>
  arrange(icb, date)

return(df)
}
