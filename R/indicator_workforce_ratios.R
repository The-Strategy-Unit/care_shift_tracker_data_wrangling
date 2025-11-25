#' Functions for the workforce ratio indicator

#' The number of clinical staff by provider and 
#' year.
#'
#' @param connection The ODBC connection.
#'
#' @returns A dataframe with the number of beds (KH03) by provider and year.

get_workforce_data <- function(connection) {
  
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
    AND datepart(mm, [Effective_Snapshot_Date]) = '06' --end of Q1 as proxy for current financial year
    
    ORDER BY org_code, cluster_group, staff_group, effective_snapshot_date
  "
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

# assign_workforce_icb <- function(data, dist_geog) {
# 
#   wrangled <- data |>
#     filter(!is.na(bed_total)) |>
#     # join results to activity distributions
#     left_join(dist_geog |>
#                 select(1:5,9), by = c("organisation_code" = "prov_code", "der_financial_year" = "der_financial_year")) |>
#     mutate(beds_adj = bed_total*prop_bed) |>
#     group_by(icb24cd, icb24cdh, icb24nm, der_financial_year, org_class) |>
#     summarise(beds = round(sum(beds_adj),4)) |>
#     filter(!is.na(icb24cd)) |>
#     arrange(icb24cd, org_class, der_financial_year)
#   
#   return(wrangled)
# }