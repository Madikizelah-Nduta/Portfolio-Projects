select*
from layoffs;



-- copy dataset,work on staging dataset
create table layoffs_staging
like layoffs;

select * 
from layoffs_staging;

insert layoffs_staging
select * 
from layoffs;

 -- remove duplicates
-- identify duplicates

select * ,
row_number() over(
partition by company,location,industry,total_laid_off,'date',
stage,country,funds_raised_millions  ) as row_num
from layoffs_staging;

-- create CTE
with duplicate_cte as
(
select * ,
row_number() over(partition by company,location,industry,total_laid_off,'date',
stage,country,funds_raised_millions  ) as row_num
from layoffs_staging
)

select*
from duplicate_cte
where row_num >1; 

-- double check duplicates

select*
from layoffs_staging
where company= 'casper';

-- 
select * ,
row_number() over(partition by company,location,industry,total_laid_off,'date',
stage,country,funds_raised_millions  ) as row_num
from layoffs_staging;

with duplicate_cte as
(
select * ,
row_number() over(partition by company,location,industry,total_laid_off,'date',stage,country,funds_raised_millions  ) as row_num
from layoffs_staging
)
delete
from duplicate_cte
where row_num >1; 

-- create another staging database,delete rows there
-- create statement
CREATE TABLE `layoffs_staging2layoffs_staging2` (
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

-- insert staging 1 dataset into the new one

insert into layoffs_staging2
select * ,
row_number() over(partition by company,location,industry,total_laid_off,'date',
stage,country,funds_raised_millions  ) as row_num
from layoffs_staging;

select*
from layoffs_staging2
where row_num>1;


-- delete dupliates
delete
from layoffs_staging2
where row_num >1;

-- standardise the data

-- trim the space at the beginning of columns

select distinct company,(trim(company))
from layoffs_staging2;

select distinct country,trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set company= trim(company);

-- update industries by category
select distinct industry
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry  like 'crypto%';

update layoffs_staging2
set industry='crypto'
where industry like 'crypto%';


-- look at the `date`
select distinct date
from layoffs_staging2
;

-- standardise the countries

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country= trim(trailing '.' from country)
where country like 'united states%';

-- change date format
select `date`,
str_to_date(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

update layoffs_staging2
set date=str_to_date(`date`,'%m/%d/%Y');

-- change data type of the date
alter table layoffs_staging2
modify column `date` date;

-- null and blanks


select*
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select*
from layoffs_staging2
where industry is null
or industry = '';

select *
 FROM layoffs_staging2
 where company='airbnb';
 
 
 SELECT T1.INDUSTRY ,T2.INDUSTRY 
FROM layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.COMPANY=T2.company
WHERE (T1.INDUSTRY IS NULL OR T1.industry= '')
AND T2.industry IS NOT NULL;

 
update layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.COMPANY=T2.company
SET T1.INDUSTRY=T2.INDUSTRY
WHERE T1.INDUSTRY IS NULL 
AND T2.industry IS NOT NULL;

 UPDATE layoffs_staging2
 SET INDUSTRY=NULL
 WHERE INDUSTRY= '';
 
 -- remove unnecessary rows and columns

select*
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;


