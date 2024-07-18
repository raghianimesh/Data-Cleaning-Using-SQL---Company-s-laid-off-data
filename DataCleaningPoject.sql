-- ------------------------ Data Cleaning ---------------------------

USE world_layoffs;  # The created database name is world_layoffs and it is using for this project 
SELECT * FROM layoffs;  # Visulaizing data from the table

####  Data cleaning process involves :
-- 1) Remove duplicates
-- 2) Standardize the Data
-- 3) Null values pr blank values check
-- 4) Remove any unnecessary columns

# To create another table with same data which helps us to use the original raw data if any error occured in 
# data manipulation table layoffs

DROP TABLE layoff_staging; # if its present table will deleted
CREATE TABLE layoff_staging
LIKE layoffs;              # Creating a table like the original table for data cleaning process
 
SELECT * FROM layoff_staging;  # inserting data into it
INSERT  layoff_staging
SELECT * FROM layoffs;

-- Remove Duplicates ------
SELECT * FROM layoff_staging;
CREATE TABLE layoff_staging2 LIKE layoff_staging;
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,
funds_raised_millions)
as row_num
FROM layoff_staging
)
SELECT company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,
funds_raised_millions,row_num
INTO layoff_staging2
FROM duplicate_cte
WHERE row_num>1;

-- To remove duplicates, create another table where we only add row_num = 1 rows 

SHOW CREATE TABLE layoffs;
-- copy the statement of table from output window and add the new column row_num into it and
-- give a new table name..

CREATE TABLE `layoff_staging2` (`company` text,  `location` text,  `industry` text,
  `total_laid_off` int DEFAULT NULL,`percentage_laid_off` text, `date` text,`stage` text,
    `country` text,
 `funds_raised_millions` int DEFAULT NULL,
 `row_num` int) 
 ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoff_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,
funds_raised_millions)
as row_num
FROM layoff_staging;


DELETE FROM layoff_staging2 WHERE row_num>1;  # deleting duplicate rows from the table

SELECT * FROM layoff_staging2 WHERE row_num>1; 

-- 2) Standardize Data ---------------------------------------------------------------

-- Remove the whitespace from company column ----

SELECT company, TRIM(company) FROM layoff_staging2;
UPDATE layoff_staging2 SET company = trim(company);
SELECT company FROM layoff_staging2;

-- Checking industry column ----

SELECT DISTINCT(industry) FROM layoff_staging2 ORDER BY industry;  ## Null values are seeing also names need to change
SELECT * FROM layoff_staging2 WHERE industry  LIKE 'Crypto%' ;
UPDATE layoff_staging SET industry = 'Crypto' WHERE industry LIKE 'Crypto%';

SELECT DISTINCT(industry) FROM layoff_staging2;

-- Checking Location column --------------------------------

SELECT DISTINCT(location) FROM layoff_staging2;

-- Checking country column --------------------------------

SELECT DISTINCT(country) FROM layoff_staging2 ORDER BY 1;
UPDATE layoff_staging2 SET country = 'United States' WHERE country LIKE 'United States%';

SELECT DISTINCT(country) FROM layoff_staging2 ORDER BY country DESC;

-- Date Column --------------------------------------------------

SELECT `date` FROM layoff_staging2;
UPDATE layoff_staging2 SET `date` = str_to_date(`date`,'%m/%d/%Y');
SELECT `date` FROM layoff_staging2;
ALTER TABLE layoff_staging2 MODIFY COLUMN`date` DATE;

-- -Null Check ---------------------------------------------------

SELECT  * FROM layoff_staging2 WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;
delete  FROM layoff_staging2 WHERE total_laid_off is null and percentage_laid_off IS NULL;
SELECT   * FROM layoff_staging2 WHERE industry IS NULL;

SELECT   * FROM layoff_staging2 WHERE industry = 'Airbnb';

update layoff_staging2 set industry = 'Travel'
 WHERE company = 'Airbnb';
 
SELECT   * FROM layoff_staging2 WHERE industry = 'Airbnb';
 
SELECT   distinct(industry) FROM layoff_staging2 ;
SELECT  * FROM layoff_staging2 WHERE industry IS NULL OR industry = '';
 
SELECT  * FROM layoff_staging2 WHERE industry = 'Carvana';
UPDATE layoff_staging2 SET industry = 'Transportation' WHERE company = 'Carvana';
 
SELECT  * FROM layoff_staging2 WHERE company = 'Juul';
UPDATE layoff_staging2 SET industry = 'Consumer' WHERE company = 'Juul';
SELECT  * FROM layoff_staging2 WHERE company = 'Juul';
 
ALTER TABLE layoff_staging2
 DROP COLUMN row_num;
 
 
 SELECT * FROM layoff_staging2;
 
 
 
 