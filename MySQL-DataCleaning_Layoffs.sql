-- SQL Project - Data Cleaning
	-- Dataset: https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- View the data
SELECT * 
FROM layoffs;

-- First create a staging table, that we will work in and clean the data, but keep the  original raw data in case something happens
CREATE TABLE layoffs.layoffs_copy
LIKE layoffs.layoffs;

-- Insert all the data in the copy table
INSERT layoffs_copy 
SELECT * FROM layoffs.layoffs;

-- View the new table
SELECT * 
FROM layoffs_copy;

-- Usual steps for data cleaning
	-- 1. Check for and remove duplicates
	-- 2. Standardize data and fix errors
	-- 3. Look at null values
	-- 4. Remove any columns and rows that are not necessary

-- ------------------------------------------------------------------------------------------------------------------------------------------

# 1. Remove Duplicates

-- First let's check for duplicates
SELECT *
FROM layoffs.layoffs_copy
;

-- Use ROW_NUMBER and PARTITION BY all the columns to look for duplicate rows
SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, 
						location, 
                        industry, 
                        total_laid_off,
                        percentage_laid_off,
                        stage,
                        country,
                        funds_raised_millions,
                        `date`
						) AS row_num
	FROM 
		layoffs.layoffs_copy;

-- To filter the duplicate rows, we need to look for row_num > 1
-- We can use a CTE to do that, and the query above
WITH duplicates_cte AS(
SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, 
						location, 
                        industry, 
                        total_laid_off,
                        percentage_laid_off,
                        `date`,
                        stage,
                        country,
                        funds_raised_millions
						) AS row_num
	FROM 
		layoffs.layoffs_copy
)
SELECT *
FROM duplicates_cte
WHERE row_num >1;

-- We can check if the method worked by running the following query 
-- Use a company name that appeared in the output of the previous query
SELECT *
FROM layoffs.layoffs_copy
WHERE company = 'Yahoo'
;

-- It looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- The previous query with row_num > 1 are our real duplicates 
-- These are the ones we want to delete

-- Since RON_NUMBER creates a new column we need to create a new table with an added column that later we can delete
-- This will allow us filter the duplicate rows we need to delete that we found using ROW_NUMBER

ALTER TABLE layoffs.layoffs_copy
ADD COLUMN row_num INT;

CREATE TABLE `layoffs`.`layoffs_copy2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT DEFAULT NULL,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int DEFAULT NULL,
`row_num` INT
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_copy2
SELECT  `company`,
		`location`,
		`industry`,
		`total_laid_off`,
		`percentage_laid_off`,
		`date`,
		`stage`,
		`country`,
		`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, 
						location, 
                        industry, 
                        total_laid_off,
                        percentage_laid_off,
                        `date`,
                        stage,
                        country,
                        funds_raised_millions
						) AS row_num
	FROM 
		layoffs_copy;

-- Now that we have this new table populated we can delete the rows were row_num is greater than 2, which means it's a duplicate

DELETE FROM layoffs.layoffs_copy2
WHERE row_num >= 2;

-- We can check if the method worked by running the following query 
SELECT * 
FROM layoffs.layoffs_copy2;

-- ------------------------------------------------------------------------------------------------------------------------------------------
# 2. Standardize Data

-- From now on the table we'll be working with is the second copy, where we deleted the duplicates
SELECT * 
FROM layoffs.layoffs_copy2;

-- Let's trim the company column to make it more neat
UPDATE layoffs_copy2
SET company = TRIM(company);

-- -------------------------------------
-- If we look at industry it looks like some are similar, we need to standardize that so it doesn't affect our analysis
SELECT DISTINCT industry
FROM layoffs.layoffs_copy2
ORDER BY industry;

-- 	The problem is with Cripto, let's look at those
SELECT DISTINCT *
FROM layoffs.layoffs_copy2
WHERE industry LIKE 'Crypto%';

-- Now lets update all to Crypto
UPDATE layoffs_copy2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- -------------------------------------
-- Let's check country
SELECT DISTINCT country
FROM layoffs.layoffs_copy2
ORDER BY country;

-- There are some "United States" and some "United States." with a period at the end so we need to standardize that
-- We can use TRIM in case there are some more like that, what's TRAILING (in this case ".") FROM the column name
UPDATE layoffs_copy2
SET country = TRIM(TRAILING '.' FROM country);

-- Now if we run this query it should be fixed
SELECT DISTINCT country
FROM layoffs.layoffs_copy2
ORDER BY country;

-- -------------------------------------- 
-- Let's also fix the date columns, it's type text right now

-- We can use STR_TO_DATE to update this field justin case not everyone used the same one
-- We use date formats for this '%m/%d/%Y' this is for example 2023-12-31
UPDATE layoffs_copy2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Now that its standardized we can convert the data type of the column
ALTER TABLE layoffs_copy2
MODIFY COLUMN `date` DATE;

-- ------------------------------------------------------------------------------------------------------------------------------------------
# 3. Look at Null Values

-- The industry column has null values as we noticed when looking at the distinct of it to standardize
SELECT *
FROM layoffs.layoffs_copy2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- It looks like some we know but they just aren't populated.
-- We can write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- That way it makes it easier in case there were thousands we wouldn't have to manually check them all
-- Here's an example
SELECT *
FROM layoffs.layoffs_copy2
WHERE company LIKE 'airbnb%';

-- But in this one there's no other instance of that company
SELECT *
FROM layoffs.layoffs_copy2
WHERE company LIKE 'Bally%';

-- We should set the blanks to nulls since those are typically easier to work with
UPDATE layoffs.layoffs_copy2
SET industry = NULL
WHERE industry = '';

-- Now we have to populate those nulls if possible
-- We join the table with itself to check if there's another row of that company that the industry is not null 
-- Our data shows info for various dates so it's possible in one date the column is not null
-- We check for company and location to make sure it's the same one
-- We SET the column we want to UPDATE using the instance WHERE is null in one AND not null in the other
-- t1 should be the blank one because t2 is what we are joining to it
UPDATE layoffs_copy2 t1
JOIN layoffs_copy2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- If we check it looks like Bally's was the only one without an other row to populate the null value
SELECT *
FROM layoffs.layoffs_copy2
WHERE industry IS NULL 
ORDER BY industry;

-- ------------------------------------------------------------------------------------------------------------------------------------------
# 4. remove any columns and rows we need to

-- The laid off we can't populate so we have to see if we can still use the data with null in that column
-- The laid off percentage we could calculate it if we had the total of employees but it's not the case so it has to stay null as well
SELECT *
FROM layoffs.layoffs_copy2
WHERE total_laid_off IS NULL
OR percentage_laid_off IS NULL;

-- When the rows don't have either of those columns populated so we don't need them for the analysis therefore we have to delete them
SELECT *
FROM layoffs.layoffs_copy2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- We can delete this useless data that we can't really use like this
DELETE FROM layoffs.layoffs_copy2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- We can also delete the row_num column because we don't need it anymore
ALTER TABLE layoffs_copy2
DROP COLUMN row_num;

-- Now we have the data ready for our analysis
SELECT * 
FROM layoffs.layoffs_copy2;


