#' Functions for the workforce ratio indicator

#' The number of clinical staff by provider and 
#' year.
#'
#' @param connection The ODBC connection.
#'
#' @returns A dataframe with the number of beds (KH03) by provider and year.

get_cost_data <- function(connection, financial_year) {
  
  query <- "
  SET NOCOUNT ON;
  
    with cte as (
      SELECT [Organisation_Code]
      ,cast(datepart(yyyy,[Effective_Snapshot_Date]) as varchar) + '/' + right(cast(datepart(yyyy,[Effective_Snapshot_Date])+1 as varchar),2) as der_financial_year
      ,sum(case when Mapping_Pot = '01_EI' AND Department_Code = 'DC' then Actual_Cost
                when Mapping_Pot = '05_OP' AND Department_Code = 'CL' then Actual_Cost
                when Mapping_Pot = '06_OAS' AND Department_Code in ('Rehab','RP') then Actual_Cost
                else 0 end) as acute_eqv_cost
      ,sum(case when Mapping_Pot = '07_COM' AND Department_Code in ('CHS','DC','Rehab','RP') then Actual_Cost
                else 0 end) as comm_eqv_cost
    FROM [UKHF_National_Cost_Collection].[Unadjusted_Data_v21_1]
    
    WHERE effective_snapshot_date < 'fy_date'

    group by [Organisation_Code]
      ,cast(datepart(yyyy,[Effective_Snapshot_Date]) as varchar) + '/' + right(cast(datepart(yyyy,[Effective_Snapshot_Date])+1 as varchar),2)
      )    

    Select * from cte
    where acute_eqv_cost > 0 OR comm_eqv_cost > 0
    order by [Organisation_Code]
      ,der_financial_year
  " |>
    stringr::str_replace_all(c("fy_date" = financial_year))
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}

assign_costs_icb <- function(data, dist_geog) {
  dist <- dist_geog |>
    filter(!is.na(icb24cdh)) |>
    group_by(prov_code, der_financial_year, icb24cd, icb24cdh, icb24nm) |>
    summarise(pats = sum(pats)) |>
    ungroup() |>
    group_by(prov_code, der_financial_year) |>
    mutate(pat_tot = sum(pats),
          prop_pat = pats/pat_tot) |>
    select(1,2,4,5,8)

  wrangled <- data |>
    # join results to activity distributions
    left_join(dist, by = c("organisation_code" = "prov_code", "der_financial_year" = "der_financial_year"), relationship = "many-to-many") |>
    mutate(acute_cost_adj = acute_eqv_cost*prop_pat,
          comm_cost_adj = comm_eqv_cost*prop_pat) |>
    group_by(icb24cdh, icb24nm, der_financial_year) |>
    summarise(acute_cost = round(sum(acute_cost_adj),4),
          comm_cost = round(sum(comm_cost_adj),4),
          ratio = comm_cost/acute_cost) |>
    ungroup() |>
    filter(!is.na(icb24cdh)) |>
    arrange(icb24cdh, der_financial_year)

  return(wrangled)
}

assign_costs_lad <- function(data, dist_geog) {
  dist <- dist_geog |>
    filter(!is.na(lad24cd)) |>
    group_by(prov_code, der_financial_year, lad24cd, lad24nm) |>
    summarise(pats = sum(pats)) |>
    ungroup() |>
    group_by(prov_code, der_financial_year) |>
    mutate(pat_tot = sum(pats),
           prop_pat = pats/pat_tot) |>
    select(1:4,7)

  wrangled <- data |>
    # join results to activity distributions
    left_join(dist, by = c("organisation_code" = "prov_code", "der_financial_year" = "der_financial_year"),
              relationship = "many-to-many") |>
    mutate(acute_cost_adj = acute_eqv_cost*prop_pat,
           comm_cost_adj = comm_eqv_cost*prop_pat) |>
    group_by(lad24cd, lad24nm, der_financial_year) |>
    summarise(acute_cost = round(sum(acute_cost_adj),4),
              comm_cost = round(sum(comm_cost_adj),4),
              ratio = comm_cost/acute_cost) |>
    ungroup() |>
    filter(!is.na(lad24cd)) |>
    arrange(lad24cd, der_financial_year)

  return(wrangled)
}

assign_costs_pcn <- function(data, lookup, dist_geog) {
  dist <- dist_geog |>
    filter(!is.na(gp_prac)) |>
    left_join(lookup |> select(1,5,6), by = c("gp_prac" = "partner_organisation_code")) |>
    group_by(prov_code, der_financial_year, pcn_code, pcn_name) |>
    summarise(pats = sum(pats)) |>
    ungroup() |>
    group_by(prov_code, der_financial_year) |>
    mutate(pat_tot = sum(pats),
           prop_pat = pats/pat_tot) |>
    select(1:4,7)

  wrangled <- data |>
    # join results to activity distributions
    left_join(dist, by = c("organisation_code" = "prov_code", "der_financial_year" = "der_financial_year"),
              relationship = "many-to-many") |>
    mutate(acute_cost_adj = acute_eqv_cost*prop_pat,
           comm_cost_adj = comm_eqv_cost*prop_pat) |>
    group_by(pcn_code, pcn_name, der_financial_year) |>
    summarise(acute_cost = round(sum(acute_cost_adj),4),
              comm_cost = round(sum(comm_cost_adj),4),
              ratio = comm_cost/acute_cost) |>
    ungroup() |>
    filter(!is.na(pcn_code)) |>
    arrange(pcn_code, der_financial_year)

  return(wrangled)
}