select *
from layoffs_staging2;

select max(total_laid_off),max(percentage_laid_off)
from layoffs_staging2;
-- entire company went down

select*
from layoffs_staging2
where percentage_laid_off=1
order by funds_raised_millions desc; 


select company,sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select min(`date`), max(`date`)
from layoffs_staging2;

-- what industry and country got hit the most during COVID
select industry,sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

select country,sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

insert layoffs_staging
select *
from layoffs;

select year( `date`),sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 1 desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 1 desc;

-- progression of layoffs,rolling total by month

select substring(`date`,1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;

with rolling_total as
(
select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)

select `month`,total_off,
 sum(total_off) over (order by `month`) as rolling_total
from rolling_total;


select company,year(`date`),sum(total_laid_off)
from layoffs_staging2
group by company,year(`date`)
order by company asc
;

-- which years employees were laid off the most 


with company_year (company,years,total_laid_off)as
(

select company,year(`date`),sum(total_laid_off)
from layoffs_staging2
group by company,year(`date`)
)
,company_year_rank as

(select*,
 DENSE_RANK() OVER(partition by YEARS ORDER BY TOTAL_LAID_OFF DESC) as ranking
FROM company_year
where years is not null
)

select*
from company_year_rank
where ranking <=5
;
