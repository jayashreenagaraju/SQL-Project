-- Data Cleaning

select * from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null values or Blank Values
-- 4. Remove any columns

-- Making a copy of the raw data. Real time Good practice to work on the dataset.
create table layoffs_staging
like layoffs;

select * from layoffs_staging;

-- Now inserting values to the layoffs-staging
Insert layoffs_staging 
select *
from layoffs;

-- using this table for entire cleaning
select * from layoffs_staging;

-- 1. Removing duplicates

select * ,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,
stage,country,funds_raised_millions) as row_num   -- because date is a keyword so for date using backtick
from layoffs_staging;

with duplicate_cte as
(select * ,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,
stage,country,funds_raised_millions) as row_num   
from layoffs_staging)
select * 
from duplicate_cte
where row_num > 1;   -- checking the duplicates so it has row_num greater than 1.

-- Finding out duplicates by each single company 
select * from layoffs_staging
where company = 'Casper';

with duplicate_cte as
(select * ,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,
stage,country,funds_raised_millions) as row_num   
from layoffs_staging)
Delete
from duplicate_cte
where row_num > 1;

-- The above method can be used in Microsoft SQL Server. but we cannot do it in MySql
-- So  above method cannot remove duplicates. So  we should add one more column to the layoffs_staging and add row_num column
-- for that select table layoffs_staging  right click then copy to clipboard then select create statement and press ctrl+v

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

Insert into layoffs_staging2
select * ,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,
stage,country,funds_raised_millions) as row_num   
from layoffs_staging;

select * 
from layoffs_staging2
where row_num > 1;

Delete 
from layoffs_staging2
where row_num > 1;

select * 
from layoffs_staging2;

-- Standardizing the data

select company,(Trim(company)) 
from layoffs_staging2;

update layoffs_staging2
set company = Trim(company);

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
select * 
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%';

select distinct location
from layoffs_staging2
order by 1;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1; 

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- Let's also fix the date columns:
select `date`,str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

-- we can use str to date to update this field
update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

-- now we can convert the data type properly
alter table layoffs_staging2
modify column `date` date;

-- Null values or Blank Values

select * from layoffs_staging2
where industry is null
or industry = '';

update layoffs_staging2
set industry = null
where industry = '';

select * 
from layoffs_staging2
where company ='Airbnb';

select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
where t1.industry is null
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select * 
from layoffs_staging2;

-- Removing columns

select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

select * 
from layoffs_staging2;

-- Exploratory Data Analysis

Select * from layoffs_staging2;

select max(total_laid_off),max(percentage_laid_off)
from layoffs_staging2;

-- companies had 1 which is basically 100 percent of they company laid off
select *
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

-- Companies with the most Total Layoffs
select company, sum(total_laid_off) 
from layoffs_staging2
group by company
order by sum(total_laid_off) desc;

Select min(`date`),max(`date`) 
from layoffs_staging2;

-- Industry with the most Total Layoffs
select industry, sum(total_laid_off) 
from layoffs_staging2
group by industry
order by sum(total_laid_off) desc;

 -- country with the most Total Layoffs
select country, sum(total_laid_off) 
from layoffs_staging2
group by country
order by sum(total_laid_off) desc;

select year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by year(`date`)
order by 1 desc;

select stage, sum(total_laid_off) 
from layoffs_staging2
group by stage
order by 2 desc;

-- Rolling Total of Layoffs Per Month
Select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7)  is  not null
group by `month`
order by 1;

-- now using it in a CTE so we can query off of it
with rolling_total as
(Select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7)  is  not null
group by `month`
order by 1)
select `month`, total_off,
sum(total_off)over(order by `month`) as rolling_total
from rolling_total;

-- Companies with the most Layoffs per year.
select company, year(`date`),sum(total_laid_off) 
from layoffs_staging2
group by company,year(`date`)
order by 3 desc;

with company_year(company,years,total_laid_off) as
(
select company, year(`date`),sum(total_laid_off) 
from layoffs_staging2
group by company,year(`date`)
),
company_year_rank as (
select *,
dense_rank()over(partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null
)
select * from company_year_rank
where ranking <=5;

