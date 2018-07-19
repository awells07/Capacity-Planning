use DSDWDev;

select top 100 * from dbo.AW_CapacityPlanning_June30end;


/* 
    Step 1: Create truncated list of task types and smaller table from original 
*/
IF OBJECT_ID('tempdb.dbo.#temp_task_type', 'U') IS NOT NULL DROP TABLE #temp_task_type;
select a.task_type, b.task_type_truncated, task_date,
       case when a.work_type2_temp in ('xHOLIDAY', 'xJury Duty', 'xPTO') then 'PTO et al' else a.work_type2_temp end as work_type2, a.name, a.hours, last_time_entry, 
       dateadd(month,-1,last_time_entry) as lagged_time_entry
into #temp_task_type
from (select *, cast([Task Date] as date) as task_date, replace(project_initiative,'*','') as task_type, replace([Work Type],'*','') as work_type2_temp 
	  from dbo.AW_CapacityPlanning_June30end
	  where cast([Task Date] as date) between '2017-07-07' and '2018-06-29') a
inner join 
    dbo.AW_CapacityPlanning_task_type_lu b
on a.task_type=b.task_type
inner join
     (select distinct name, max(cast([Task Date] as date)) as last_time_entry
	  from dbo.AW_CapacityPlanning_June30end
	  where cast([Task Date] as date) between '2017-07-07' and '2018-06-29'
	  group by name) e
on a.name=e.name
where a.work_type2_temp not in ('xUnwanted Entry', 'xTrivial', 'xNOT LISTED');
	create index cp_task_work_idxTT on #temp_task_type(task_type_truncated);
		select * from #temp_task_type;
				--select distinct work_type2 from #temp_task_type order by 1;
/* end */


/* 
   Step 2: Task by Work Type aggregation - helps to answer question of: What Work Types are 
           associated with a given Task.
*/
/* Step 2a: Option A: Summary by truncated project name and work type*/
IF OBJECT_ID('tempdb.dbo.#aw_capacityplanning_task_x_worktype_summary_trunc', 'U') IS NOT NULL DROP TABLE #aw_capacityplanning_task_x_worktype_summary_trunc;
select *, row_number() OVER(PARTITION BY task_type_truncated ORDER BY task_x_worktype_hours desc) AS task_worktype_rank
into #aw_capacityplanning_task_x_worktype_summary_trunc
from
(
select distinct a.task_type_truncated, work_type2,
                count(distinct name) as distinct_associates, sum(hours) as task_x_worktype_hours,
				round(sum(hours) / count(distinct name),2) as hours_per_assoc_x_task_work_types			
from (select *, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY hours) OVER (PARTITION BY task_type_truncated) AS hours_outlier 
      from #temp_task_type) a
where hours > 0 and hours < hours_outlier and work_type2 is not NULL
group by a.task_type_truncated, work_type2
) a
order by 1 desc, row_number() OVER(PARTITION BY task_type_truncated ORDER BY task_x_worktype_hours desc);
	create index cp_task_work_idxT on #aw_capacityplanning_task_x_worktype_summary_trunc(task_type_truncated);
	create index cp_task_work_idxW on #aw_capacityplanning_task_x_worktype_summary_trunc(work_type2);
		select * from #aw_capacityplanning_task_x_worktype_summary_trunc; /* 697 rows */

/* summary by work type only */ 
IF OBJECT_ID('tempdb.dbo.#aw_capacityplanning_worktype_summary_trunc', 'U') IS NOT NULL DROP TABLE #aw_capacityplanning_worktype_summary_trunc;
select *, row_number() OVER(PARTITION BY work_type2 ORDER BY worktype_hours desc) AS worktype_rank
into #aw_capacityplanning_worktype_summary_trunc
from
(
select distinct work_type2,
                count(distinct name) as distinct_associates_x_worktype, sum(hours) as worktype_hours,
				round(sum(hours) / count(distinct name),2) as hours_per_assoc_x_work_types			
from (select *, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY hours) OVER (PARTITION BY work_type2) AS hours_outlier 
      from #temp_task_type) a
where hours > 0 and hours < hours_outlier and work_type2 is not NULL
group by work_type2
) a
order by 1 desc, row_number() OVER(PARTITION BY work_type2 ORDER BY worktype_hours desc);
	create index cp_task_work_idxW on #aw_capacityplanning_worktype_summary_trunc(work_type2);
		select * from #aw_capacityplanning_worktype_summary_trunc; /* 697 rows */


/* Step 2b: Option B: Full Project Name 
IF OBJECT_ID('tempdb.dbo.#aw_capacityplanning_task_x_worktype_summary', 'U') IS NOT NULL DROP TABLE #aw_capacityplanning_task_x_worktype_summary;
select *, row_number() OVER(PARTITION BY task_type ORDER BY task_x_worktype_hours desc) AS task_worktype_rank
into #aw_capacityplanning_task_x_worktype_summary
from
(
select distinct a.task_type, work_type2,
                count(distinct name) as distinct_associates, sum(hours) as task_x_worktype_hours,
				round(sum(hours) / count(distinct name),2) as hours_per_assoc_x_task_work_types				
from (select *, PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY hours) OVER (PARTITION BY task_type) AS hours_outlier 
      from #temp_task_type) a
where hours > 0 and hours < hours_outlier and work_type2 is not NULL
group by a.task_type, work_type2
) a
order by 1 desc, row_number() OVER(PARTITION BY task_type ORDER BY task_x_worktype_hours desc);
	create index cp_task_work_idxT on #aw_capacityplanning_task_x_worktype_summary(task_type);
	create index cp_task_work_idxW on #aw_capacityplanning_task_x_worktype_summary(work_type2);
		select * from #aw_capacityplanning_task_x_worktype_summary; *//* 1447 rows */
/* end */


/* 
   Step 3: Create table summarized by associate and work type - answers question regarding where people are spending their time by work type - 
           can be used to 'infer' an associate's expertise
*/
IF OBJECT_ID('tempdb.dbo.#aw_capacityplanning_name_work_summary', 'U') IS NOT NULL DROP TABLE #aw_capacityplanning_name_work_summary;
select *, row_number() OVER(PARTITION BY name ORDER BY total_hours_x_worktype desc) AS worktype_rank
into #aw_capacityplanning_name_work_summary
from
(
select distinct name, work_type2, sum(hours) as total_hours_x_worktype
from #temp_task_type
group by name, work_type2
) a
order by 1, row_number() OVER(PARTITION BY name ORDER BY total_hours_x_worktype desc);
	create index cp_assocs_idxN on #aw_capacityplanning_name_work_summary(name);
	create index cp_assocs_idxW on #aw_capacityplanning_name_work_summary(work_type2);
		select * from #aw_capacityplanning_name_work_summary order by name, worktype_rank;
/* end */


/* 
   Step 4: Create table summarized by associate and task type - answers question regarding where people are spending their time by task type - 
           can be used to 'infer' an associate's expertise
*/
IF OBJECT_ID('tempdb.dbo.#aw_capacityplanning_name_task_summary', 'U') IS NOT NULL DROP TABLE #aw_capacityplanning_name_task_summary;
select *, row_number() OVER(PARTITION BY name ORDER BY total_hours_x_tasktype desc) AS tasktype_rank
into #aw_capacityplanning_name_task_summary
from
(
select distinct name, task_type_truncated, sum(hours) as total_hours_x_tasktype
from #temp_task_type
group by name, task_type_truncated
) a
order by 1, row_number() OVER(PARTITION BY name ORDER BY total_hours_x_tasktype desc);
	create index cp_assocs_idxN on #aw_capacityplanning_name_task_summary(name);
	create index cp_assocs_idxT on #aw_capacityplanning_name_task_summary(task_type_truncated);
		select * from #aw_capacityplanning_name_task_summary order by name, tasktype_rank;
/* end */


/* Step 5: Create summary of most recent 1 month of work by associate - use this to forecast */
IF OBJECT_ID('tempdb.dbo.#temp_assoc_forecast', 'U') IS NOT NULL DROP TABLE #temp_assoc_forecast;
select distinct a.name, b.task_type_count, count(distinct work_type2) as work_type_count, 
                sum(weighted_hours_to_go) as weighted_hours_to_go, round(sum(weighted_hours_to_go)/40,1) as weeks_until_free,
				case when round(sum(weighted_hours_to_go)/40,1)<0 then 'over allocated' else 'ok to schedule' end as assoc_status
into #temp_assoc_forecast
from 
(
select a.*, hours_per_assoc_x_work_types, assoc_total_hours_work_type, 
       hours_per_assoc_x_work_types - assoc_total_hours_work_type as hours_to_go_per_assoc_per_work_type,
       round((hours_per_assoc_x_work_types - assoc_total_hours_work_type)*prop_of_total_hours_last_month,1) as weighted_hours_to_go
from (select distinct a.name, /*a.task_type_truncated,*/ a.work_type2, sum(a.hours) as assoc_hrs_x_work_types_last_month/*assoc_hrs_x_task_work_types_last_month*/,
                      round(sum(a.hours) / b.total_hours,2) as prop_of_total_hours_last_month
      from (select * 
	        from #temp_task_type 
			where task_date between lagged_time_entry and last_time_entry) a
	  inner join
	       (select distinct name, sum(hours) as total_hours
		    from #temp_task_type
			where task_date between lagged_time_entry and last_time_entry
			group by name) b
	  on a.name=b.name
      group by a.name, /*a.task_type_truncated,*/ a.work_type2, b.total_hours) a
inner join
      /*(select distinct task_type_truncated, work_type2, hours_per_assoc_x_task_work_types
	   from #aw_capacityplanning_task_x_worktype_summary_trunc) b*/ 
	   (select distinct work_type2, hours_per_assoc_x_work_types
	    from #aw_capacityplanning_worktype_summary_trunc) b
on /*a.task_type_truncated=b.task_type_truncated and*/
   a.work_type2=b.work_type2
inner join
      (select distinct name, /*task_type_truncated,*/ work_type2, sum(hours) as assoc_total_hours_work_type
	   from #temp_task_type
	   group by name, /*task_type_truncated,*/ work_type2) c  /* no time restriction */
on a.name=c.name and
   /*a.task_type_truncated=c.task_type_truncated and*/
   a.work_type2=c.work_type2
--order by a.name, assoc_total_hours_work_x_task_type desc
) a
inner join
     (select distinct name, count(distinct task_type_truncated) as task_type_count
	  from #temp_task_type
	  group by name) b
on a.name=b.Name
group by a.name, b.task_type_count
order by a.name; 
	create index cp_assocs_idxN on #temp_assoc_forecast(name); 
		select * from #temp_assoc_forecast;
/* end */


/*****************************************************************************************************************************/
/*************************************   CORE TABLES FOR SUMMARY ANALYSES   **************************************************/
/*****************************************************************************************************************************/
DECLARE @task varchar(255)  /* what to do w/ managers - some work at detail level, others pm, others management */
SET @task='%oracle%'  /* remove "task_type_truncated" from screens 2+ */  /* is "weeks_until_free" reflective of their entire workload */

/* Jenn questions
   1) how to account for differential timing of colleague engagement for non-project reasons -- need leave date
   2) how to account for net-new associates
*/


/* Key work types for a given task */
IF OBJECT_ID('tempdb.dbo.#temp_cp0', 'U') IS NOT NULL DROP TABLE #temp_cp0
select distinct task_type_truncated, work_type2, distinct_associates as distinct_associates_aggregate, 
                task_x_worktype_hours as task_x_worktype_hours_aggregate, hours_per_assoc_x_task_work_types, task_worktype_rank
into #temp_cp0
from #aw_capacityplanning_task_x_worktype_summary_trunc 
where lower(task_type_truncated) like (@task) 
order by task_x_worktype_hours desc
		select * from #temp_cp0 order by task_x_worktype_hours_aggregate desc


/* Most qualified people, based on historical time entries */
IF OBJECT_ID('tempdb.dbo.#temp_cp', 'U') IS NOT NULL DROP TABLE #temp_cp
select distinct a.work_type2, a.distinct_associates_x_worktype, 
                a.worktype_hours as worktype_hours_aggregate, b.Name, b.worktype_rank as worktype_rank_within_assoc
into #temp_cp
from (select *
      from #aw_capacityplanning_worktype_summary_trunc
      where lower(work_type2) in (select distinct work_type2 from #temp_cp0)) a
inner join
     (select w.* 
	  from #aw_capacityplanning_name_work_summary w
	  inner join
	       #aw_capacityplanning_name_task_summary t
	  on w.name=t.name
	  where worktype_rank<11 /*and lower(task_type_truncated) like (@task) and tasktype_rank<11*/) b /* find associates who have the required skills as 1 of their top 10 skills, in terms of historical time reporting */
on a.work_type2=b.work_type2
order by a.worktype_hours desc, b.worktype_rank asc;
		select * from #temp_cp order by worktype_hours_aggregate desc, worktype_rank_within_assoc asc


/* Given the identified Task, who is available? */
IF OBJECT_ID('tempdb.dbo.#temp_cp2', 'U') IS NOT NULL DROP TABLE #temp_cp2
select distinct a.work_type2, a.distinct_associates_x_worktype, a.worktype_hours_aggregate, a.name, a.worktype_rank_within_assoc, 
                task_type_count as assoc_current_task_type_count, work_type_count as assoc_current_work_type_count, weeks_until_free, c.assoc_status
into #temp_cp2
from (select distinct work_type2, distinct_associates_x_worktype, worktype_hours_aggregate, name, worktype_rank_within_assoc
	  from #temp_cp) a
inner join
     (select distinct work_type2, distinct_associates_x_worktype, worktype_hours_aggregate, min(worktype_rank_within_assoc) as best_worktype_rank
	  from #temp_cp
	  group by work_type2, distinct_associates_x_worktype, worktype_hours_aggregate) b
on a.work_type2=b.work_type2 and
   a.distinct_associates_x_worktype=b.distinct_associates_x_worktype and
   a.worktype_hours_aggregate=b.worktype_hours_aggregate and
   a.worktype_rank_within_assoc=b.best_worktype_rank
inner join
   #temp_assoc_forecast c
on a.name=c.name
order by worktype_hours_aggregate desc, assoc_status asc
		select * from #temp_cp2 order by worktype_hours_aggregate desc, assoc_status asc


/* Find the associate most likely to be able to support the project */
select a.*
from #temp_cp2 a
inner join
     (select distinct work_type2, max(weeks_until_free) as weeks_until_free_max
	  from #temp_cp2
	  group by work_type2) b
on a.work_type2=b.work_type2 and
   a.weeks_until_free=b.weeks_until_free_max
where a.work_type2 not in ('Meetings')
order by worktype_hours_aggregate desc;