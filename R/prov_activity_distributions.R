#' Functions to get the distribution of provider activity by geography and practice to support weighting of
#' KH03 beds indicator data to ICB/LAD/PCN.

#' @param connection The ODBC connection 
#' @returns A dataframe with the provider activity over time distributed by either lsoa or gp practice

get_prov_dist_by_lsoa <- function(connection) {
  
  query <- "
  SET NOCOUNT ON;
  
  with lsoa as (
  select left(der_provider_code,3) as prov_code, der_financial_year, [Der_Postcode_LSOA_2011_Code] as lsoa_2011,
  count(distinct [Der_Pseudo_NHS_Number]) as patients,
  sum(case when der_spell_los = 0 then 0.5 else der_spell_los end) as beddays
  from Reporting_MESH_APC.APCS_Core_Monthly_Snapshot
  where [Der_Postcode_LSOA_2011_Code] is not NULL
  group by left(der_provider_code,3), der_financial_year, [Der_Postcode_LSOA_2011_Code]
  )

  select *
  from lsoa
  where patients != 0
  order by prov_code, der_financial_year, lsoa_2011
  "
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

get_prov_dist_by_practice <- function(connection) {
  
  query <- "
  SET NOCOUNT ON;
  
  with prac as (
  select left(der_provider_code,3) as prov_code, der_financial_year, [GP_Practice_Code] as gp_prac,
  count(distinct [Der_Pseudo_NHS_Number]) as patients,
  sum(case when der_spell_los = 0 then 0.5 else der_spell_los end) as beddays
  from Reporting_MESH_APC.APCS_Core_Monthly_Snapshot
  where [GP_Practice_Code] is not NULL
  group by left(der_provider_code,3), der_financial_year, [GP_Practice_Code]
  )

  select *
  from prac
  where patients != 0
  order by prov_code, der_financial_year, gp_prac
  "
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

get_prov_pats_lsoa <- function(url) {
  
  data <- readr::read_csv(url, col_names = c("prov_code", "der_financial_year", "lsoa_2011", "pats"))
  
  return(data)
}