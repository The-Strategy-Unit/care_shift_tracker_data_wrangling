#' Functions for the workforce ratio indicator

#' The number of clinical staff by provider and 
#' year.
#'
#' @param connection The ODBC connection.
#'
#' @returns A dataframe with the number of beds (KH03) by provider and year.

get_workforce_data <- function(connection, financial_year) {
  
  query <- "
  SET NOCOUNT ON;
  
    select org_code, cluster_group, staff_group, data_type, total, effective_snapshot_date,
    cast(datepart(yyyy, effective_snapshot_date) as varchar) + '/' + cast(right(datepart(yyyy, effective_snapshot_date)+1,2) as varchar) as der_financial_year

    from [UKHF_NHS_Workforce].[Staff_Group_And_Organisation1]
    where 1 = 1
    AND Cluster_Group in ('Acute','Community Provider Trust','Mental Health') --exclude commissioning staff
    AND Data_Type = 'FTE' --better representation than headcount
    AND Staff_Group in ('HCHS doctors','Nurses & health visitors','Midwives','Support to clinical staff','Professionally qualified clinical staff') --different components of clinical staff
    AND [Effective_Snapshot_Date] >= '2012-06-30' --point at which data seems to be consistent volumes across clusters and patient distributions
    AND effective_snapshot_date < 'fy_date'
    AND datepart(mm, [Effective_Snapshot_Date]) = '06' --end of Q1 as proxy for current financial year
    
    ORDER BY org_code, cluster_group, staff_group, effective_snapshot_date
  " |>
    stringr::str_replace_all(c("fy_date" = financial_year))
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

assign_workforce_icb <- function(data, dist_geog) {

  wrangled <- data |>
    # only use combined clinical staff numbers
    filter(staff_group == 'Professionally qualified clinical staff') |>
    # join results to activity distributions
    left_join(dist_geog, by = c("org_code" = "prov_code", "der_financial_year" = "der_financial_year")) |>
    mutate(pats_adj = total*prop_pat) |>
    group_by(icb24cdh, icb24nm, cluster_group, der_financial_year) |>
    summarise(pats = round(sum(pats_adj),4)) |>
    ungroup() |>
    filter(!is.na(icb24cdh)) |>
    arrange(icb24cdh, cluster_group, der_financial_year)

  return(wrangled)
}

assign_workforce_lad <- function(data, dist_geog) {
  
  wrangled <- data |>
    # only use combined clinical staff numbers
    filter(staff_group == 'Professionally qualified clinical staff') |>
    # join results to activity distributions
    left_join(dist_geog, by = c("org_code" = "prov_code", "der_financial_year" = "der_financial_year")) |>
    mutate(pats_adj = total*prop_pat) |>
    group_by(lad24cd, lad24nm, cluster_group, der_financial_year) |>
    summarise(pats = round(sum(pats_adj),4)) |>
    ungroup() |>
    filter(!is.na(lad24cd)) |>
    arrange(lad24cd, cluster_group, der_financial_year)
  
  return(wrangled)
}

assign_workforce_pcn <- function(data, dist_geog) {
  
  wrangled <- data |>
    # only use combined clinical staff numbers
    filter(staff_group == 'Professionally qualified clinical staff') |>
    # join results to activity distributions
    left_join(dist_geog, by = c("org_code" = "prov_code", "der_financial_year" = "der_financial_year")) |>
    mutate(pats_adj = total*prop_pat) |>
    group_by(pcn_code, pcn_name, cluster_group, der_financial_year) |>
    summarise(pats = round(sum(pats_adj),4)) |>
    ungroup() |>
    filter(!is.na(pcn_code)) |>
    arrange(pcn_code, cluster_group, der_financial_year)
  
  return(wrangled)
}