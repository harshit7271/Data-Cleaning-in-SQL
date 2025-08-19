-- DATA CLEANING --- PROJECT

SELECT *
FROM layoffs;


-- 1. REMOVE DUPLICATES
-- 2. Standardize the data 
-- 3. Null or blank values
-- 4. Remove Any Coloumns 

# to get a new table from existing one and all we hv to do is inserting data

CREATE TABLE layoffs_staging
LIKE layoffs ;     
      
-- means from here we create new table



SELECT *
FROM layoffs_staging;
 #Now we insert data
 INSERT layoffs_staging
 SELECT *     
 FROM layoffs;
 
 SELECT *,
 ROW_NUMBER() OVER( PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') AS row_num
 FROM layoffs_staging;
 
 WITH duplicate_cte AS
 (
 select*,
 ROW_NUMBER() OVER( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country,
  funds_raised_millions) as row_num
 FROM layoffs_staging
 )
 
 select *
 FROM duplicate_cte
 where rom_num > 1;
 
 
SELECT *     
FROM layoffs_staging
where company = '&Open' 
 ;
 
 
 WITH duplicate_cte AS
 (
 select*,
 ROW_NUMBER() OVER( PARTITION BY company, location, industry, total_laid_off, 
 percentage_laid_off, 'date', stage, country,
 funds_raised_millions) as row_num
 FROM layoffs_staging
 )
 
 DELETE
 FROM duplicate_cte
 where rom_num > 1;
 
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `total_laid_off` text,
  `date` text,
  `percentage_laid_off` text,
  `industry` text,
  `source` text,
  `stage` text,
  `funds_raised` text,
  `country` text,
  `date_added` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT*
FROM layoffs_staging2
where row_num >1 ;

insert into layoffs_staging2
select*,
 ROW_NUMBER() OVER( PARTITION BY company, location, total_laid_off,
 'date', percentage_laid_off, industry, 'source', stage, funds_raised,
 country, date_added, row_num ) as row_num1
 FROM layoffs_staging
 ;
 
 
delete
FROM layoffs_staging2
where row_num >1 ;

SELECT*
FROM layoffs_staging2;
 
 
 -- STANDARDIZING DATA
 
 
 
 
 
 


 
 
 
 

 
 
 
 
 
 
 
 
 
 



 






