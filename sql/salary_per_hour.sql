-- Truncate destination table
truncate table analytics.salary_per_hour 
;


-- Read, transform, and load data to destination table 
insert into analytics.salary_per_hour (
	year ,
	month ,
	branch_id ,
	salary_per_hour
)
with cleanse_timesheets_t as (
select
	timesheet_id , 
	employee_id ,
	"date" ,
	extract(year from date) as absence_year ,
	extract(month from date) as absence_month,
	nullif(replace(checkin,'"', ''),'')::time as checkin ,
	nullif(replace(checkout,'"', ''), '')::time as checkout 
from
	staging.timesheets
)
, total_hour_t as (
select 
	employee_id,
	absence_year ,
	absence_month ,
	sum(extract(epoch from(checkout - checkin)) / 3600) as total_hour 
from 
	cleanse_timesheets_t 
where 
	concat(employee_id, absence_year, absence_month) not in (
		select 
			concat(employee_id, absence_year, absence_month) 
		from 
			cleanse_timesheets_t 
		where checkin > checkout or checkout is null or checkin is null 
		) -- remove employee_id in specific year and month if there is vague data; checkin > checkout or null value in either checkin or checkout
group by 
	1,
	2,
	3
)
, cleanse_employees_t as (
select 
	employe_id ,
	branch_id , 
	extract(year from nullif(join_date,'')::date) as join_year ,
	extract(month from nullif(join_date,'')::date) as join_month,		
	extract(year from nullif(resign_date,'')::date) as resign_year ,
	extract(month from nullif(resign_date,'')::date) as resign_month,	
	salary 
from 
	staging.employees
where 
	employe_id not in (select employe_id from staging.employees group by 1 having count(employe_id) > 1) -- duplicate employee_id (218078), the cleansing method can be various (sum/average/min/max/remove the duplicate), but in this case I'll remove the duplicate
)
select 
	a.absence_year as year,
	a.absence_month as month ,
	b.branch_id ,
	sum(b.salary)  / sum(a.total_hour) as salary_per_hour 
from 
	total_hour_t a
inner join 
	cleanse_employees_t b
on 
	a.employee_id = b.employe_id 
where 
	concat(a.absence_year, a.absence_month) != concat(b.resign_year, b.resign_month) -- exclude the working hours of the employees in their last month of work
	and concat(a.absence_year, a.absence_month) != concat(b.join_year, b.join_month) -- exclude the working hours of the employees in their first month of work
group by 
	1,
	2,
	3
;

