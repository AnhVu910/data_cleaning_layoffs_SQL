
Create table layoffs_backup as select * from layoffs;

select * from layoffs_backup;

select *,
row_number()
over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions)
as row_num
from layoffs_backup;

-- 1.create CTE check duplicate
with duplicate_cte as(
select *,
row_number()
over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions)
as row_num
from layoffs_backup
)

select * from duplicate_cte where row_num > 1;

-- 1.1 check duplicate
select * from layoffs_backup where company= 'Hibob';

-- 1.1.1 create table layoffs_backup2 add num_row to delete duplicate
CREATE TABLE IF NOT EXISTS public.layoffs_backup2
(
    company character varying(255) COLLATE pg_catalog."default",
    location character varying(255) COLLATE pg_catalog."default",
    industry character varying(255) COLLATE pg_catalog."default",
    total_laid_off integer,
    percentage_laid_off numeric(5,2),
    date date,
    stage character varying(255) COLLATE pg_catalog."default",
    country character varying(255) COLLATE pg_catalog."default",
    funds_raised_millions numeric(10,1),
	row_num integer
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.layoffs_backup2
    OWNER to postgres;

select * from layoffs_backup2;

-- 1.1.2 insert data from layoffs_backup
insert into layoffs_backup2
select *,
row_number()
over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions)
as row_num
from layoffs_backup;

select * from layoffs_backup2 where row_num > 1;

--1.2 delete duplicate
delete from layoffs_backup2 where row_num > 1;


--2. standardizing data 
--2.1 remove empty space, Correct spelling and syntax errors
select company , trim(company) as trim_company
from layoffs_backup2;

update layoffs_backup2
set company= trim(company);

select distinct industry
from layoffs_backup2
order by 1;

select * from layoffs_backup2
where industry like 'Crypto%';

update layoffs_backup2
set industry ='Crypto'
where industry like 'Crypto%';

select distinct country 
from layoffs_backup2
where country like 'United States%';

update layoffs_backup2
set country = trim(trailing '.' from country)
where country like 'United States%';

select date
from layoffs_backup2;

--check value null
select * from layoffs_backup2
where industry is null or industry ='';

select * from layoffs_backup2
where company ='Airbnb';

-- Filter missing data use self-join
select t1.industry,t2.industry
from layoffs_backup2 t1
Join layoffs_backup2 t2
On t1.company=t2.company
where t1.industry is null 
and t2.industry is not null;


-- change empty values to null
update layoffs_backup2
set industry = null
where industry ='';

-- replace null values with existing values using subquery
update layoffs_backup2 t1
set industry=(
	select t2.industry
	from layoffs_backup2 t2
	where t1.company=t2.company
	and t2.industry is not null 
	limit 1
)
where t1.industry is null;

-- After checking to see if there is still a null value,
--we see that there is still Bally's Interactive, whose industry contains a null value

select * from layoffs_backup2
where company like 'Bally%';

-- 3. delete some unnecessary data
select * from layoffs_backup2
where total_laid_off is null
and percentage_laid_off is null;

delete 
from layoffs_backup2
where total_laid_off is null
and percentage_laid_off is null;

select * from layoffs_backup2;

alter table layoffs_backup2
drop column row_num;