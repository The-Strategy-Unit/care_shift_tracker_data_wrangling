#' Functions for the delay discharge %'s indicator

#' The number of delayed discharge beddays and total spell days by month.
#'
#' @param connection The ODBC connection.
#' @param lag The maximum date for the query.
#'
#' @returns A dataframe with the number of bed days by lsoa and practice.
get_delay_disch_data <- function(connection, lag) {
  
  query <- "
  SET NOCOUNT ON;

    with cte as
    (
      Select a.[Der_Postcode_LSOA_2011_Code] as lsoa_2011, [GP_Practice_Code] as gp_prac,
      cast(datepart(yyyy,[Discharge_Date]) as varchar) + case when datepart(mm,[Discharge_Date]) < 10 then '-0' else '-' end + cast(datepart(mm,[Discharge_Date]) as varchar) as year_mon ,
      count(distinct a.apcs_ident) as spells,
      sum([Der_Spell_LoS]) as spell_los_tot,
      sum(case when [Discharge_Ready_Date] is NULL then 0
		    when [Discharge_Ready_Date] < 'start_date' then 0
		    when [Discharge_Ready_Date] > [Discharge_Date] then 0
	    	when [Discharge_Ready_Date] < [Discharge_Date] then datediff(dd, [Discharge_Ready_Date], [Discharge_Date]) else 0 end) as ddd_tot

      from [Reporting_MESH_APC].[APCS_Core_Monthly_Snapshot] a

      where 1 = 1
      and [Discharge_Date] is not NULL -- completed spells only
      and [Patient_Classification] = '1' -- ordinary admissions (no daycase or regular)
      and [Age_At_Start_of_Spell_SUS] >= 65 -- proxy for frail cohort
      and (a.[Der_Postcode_LSOA_2011_Code] is not NULL
      OR [GP_Practice_Code] is not NULL)
      and cast([Discharge_Date] as date) >= 'start_date' and
          cast([Discharge_Date] as date) < 'lag_date'

      group by a.[Der_Postcode_LSOA_2011_Code], [GP_Practice_Code],
      cast(datepart(yyyy,[Discharge_Date]) as varchar) + case when datepart(mm,[Discharge_Date]) < 10 then '-0' else '-' end + cast(datepart(mm,[Discharge_Date]) as varchar)
      )

    Select * from cte
    order by lsoa_2011, gp_prac, year_mon
  " |>
    stringr::str_replace_all(
      c("start_date" = start,
        "lag_date" = lag)
    )
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names() |>
    filter(ddd_tot <= spell_los_tot)
  
  return(wrangled)
}