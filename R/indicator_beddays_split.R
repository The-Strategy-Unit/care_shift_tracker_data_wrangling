#' Functions for the beddays split %'s indicator

#' The total beddays by provider site by month.
#'
#' @param connection The ODBC connection.
#'
#' @returns A dataframe with the number of bed days by lsoa and practice.

get_epi_bedday_data <- function(connection) {
  
  query <- "
  SET NOCOUNT ON;
  
    declare @startdate Datetime,
		@enddate Datetime;
    set @startdate = '2008-04-01';
    set @enddate = '2025-10-31';

    with cte as
    (
    select left(Provider_Code,3) as prov_code, left([Der_Provider_Site_Code],5) as prov_site_code, [Der_Postcode_LSOA_2011_Code] as lsoa_2011, [GP_Practice_Code] as gp_prac,
    left([Der_Activity_Month],4) + '-' + right([Der_Activity_Month],2) as der_activity_month,
    case when [Patient_Classification] in ('3','4') then 0.5
		    when [Der_Episode_LoS] = 0 then 0.5
		    else [Der_Episode_LoS] end as los

    from [Reporting_MESH_APC].[APCE_Core_Monthly_Snapshot]
    where 1=1
      and [Episode_Start_Date] between @startdate and @enddate --total time series
      and [Age_on_Admission] >= 65 -- proxy for frail
      and left([Admission_Method],1) in ('1') --elective only
      and [Patient_Classification] in ('1','2','3') -- exclude regular night and mother & baby
      and [Episode_End_Date] is not NULL -- FCE's only
    )

    select prov_code, prov_site_code, lsoa_2011, gp_prac, der_activity_month,
    sum(los) as sum_los
    from cte
    group by prov_code, prov_site_code, lsoa_2011, gp_prac, [Der_Activity_Month]
    order by prov_code, prov_site_code, lsoa_2011, gp_prac, [Der_Activity_Month]
  "
  
  wrangled <- DBI::dbGetQuery(connection, query) |>
    janitor::clean_names()
  
  return(wrangled)
}