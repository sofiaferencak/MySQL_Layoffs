-- MySQL - Exploratory Data Analysis
	-- In this analysis it just looking around, explore the data and find trends or patterns or anything interesting

-- This is the data for this analysis
SELECT * 
FROM layoffs.layoffs_copy2;

-- Maximum amounts of layoffs
SELECT MAX(total_laid_off)
FROM layoffs.layoffs_copy2;

-- Percentage of layoffs to see how big these were
	-- 1 means 100% of the company, basically the company doesn't exist anymore
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoffs.layoffs_copy2
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 100% of the company laid off
SELECT *
FROM layoffs.layoffs_copy2
WHERE  percentage_laid_off = 1;

-- Of those companies which one had the most amount of layoffs
SELECT *
FROM layoffs.layoffs_copy2
WHERE  percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- We can order by funcs_raised_millions to see how big some of these companies were
SELECT *
FROM layoffs.layoffs_copy2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
	-- BritishVolt looks like they raised 2.4 billions

-- Companies with the biggest Layoff in just one day
SELECT company, total_laid_off
FROM layoffs.layoffs_copy2
ORDER BY 2 DESC
LIMIT 5;

-- Range of dates to see in what amount of time the total of layoffs was
SELECT MAX(`date`),  MIN(`date`)
FROM layoffs.layoffs_copy2;
-- The total we calculate with this dataset is in the past 3 years

-- Top 10 companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs.layoffs_copy2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- Top 10 locations with the most Total Layoffs
SELECT location, SUM(total_laid_off)
FROM layoffs.layoffs_copy2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- Industries with the most Total Layoffs
SELECT industry, SUM(total_laid_off)
FROM layoffs.layoffs_copy2
GROUP BY industry
ORDER BY 2 DESC;

-- Countries with the most Total Layoffs
SELECT country, SUM(total_laid_off)
FROM layoffs.layoffs_copy2
GROUP BY country
ORDER BY 2 DESC;

-- Stage that the company that had the most Total Layoffs was in
SELECT stage, SUM(total_laid_off)
FROM layoffs.layoffs_copy2
GROUP BY stage
ORDER BY 2 DESC;
	-- IPO means initial public offering

-- Companies that had 100% layoffs at what stage they were and how many layoffs they had total
SELECT stage, SUM(total_laid_off)
FROM layoffs.layoffs_copy2
WHERE  percentage_laid_off = 1
GROUP BY stage
ORDER BY 2 DESC;
	-- Looks like most were small companies

-- Year with the most Total Layoffs
	-- YEAR function takes only the year of the date column
SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs.layoffs_copy2
GROUP BY YEAR(date)
ORDER BY 2 DESC;

-- Total layoffs by a company in each year
SELECT company, YEAR(date), SUM(total_laid_off)
FROM layoffs.layoffs_copy2
GROUP BY company, YEAR(date)
ORDER BY 1 ASC;

-- Top 5 companies with the most Layoffs each year
	-- First CTE to look for total layoffs
	-- Second CTE to filter the ranking
WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_copy2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 5
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- Top 3 companies with the most Layoffs each month
WITH Company_Month AS 
(
  SELECT company, SUBSTRING(date,1,7) AS months, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_copy2
  GROUP BY company, SUBSTRING(date,1,7)
)
, Company_Month_Rank AS (
  SELECT company, months, total_laid_off, DENSE_RANK() OVER (PARTITION BY months ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Month
)
SELECT company, months, total_laid_off, ranking
FROM Company_Month_Rank
WHERE ranking <= 3
AND months IS NOT NULL
ORDER BY months ASC, total_laid_off DESC;

-- Top 3 countries with the most Layoffs each month
WITH Country_Month AS 
(
  SELECT country, SUBSTRING(date,1,7) AS months, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_copy2
  GROUP BY country, SUBSTRING(date,1,7)
)
, Country_Month_Rank AS (
  SELECT country, months, total_laid_off, DENSE_RANK() OVER (PARTITION BY months ORDER BY total_laid_off DESC) AS ranking
  FROM Country_Month
)
SELECT country, months, total_laid_off, ranking
FROM Country_Month_Rank
WHERE ranking <= 3
AND months IS NOT NULL
ORDER BY months ASC, total_laid_off DESC;

-- Top 5 industries with the most Layoffs each year
WITH Industry_Year AS 
(
  SELECT industry, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_copy2
  GROUP BY industry, YEAR(date)
)
, Industry_Year_Rank AS (
  SELECT industry, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Industry_Year
)
SELECT industry, years, total_laid_off, ranking
FROM Industry_Year_Rank
WHERE ranking <= 5
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- Total of Layoffs per month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off)
FROM layoffs_copy2
WHERE `date` IS NOT NULL
GROUP BY dates
ORDER BY dates ASC;

-- Using a CTE it can query off of it, and calculate the rolling Total of Layoffs per month
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_copy2
WHERE `date` IS NOT NULL
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, total_laid_off, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;

-- Using the same CTE and adding one more column it calculates the rolling Total of Layoffs per month for each country
WITH DATE_CTE AS 
(
SELECT country, SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_copy2
WHERE `date` IS NOT NULL
GROUP BY dates, country
ORDER BY dates ASC
)
SELECT dates, country, total_laid_off, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;
