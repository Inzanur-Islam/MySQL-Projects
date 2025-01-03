
SELECT *
FROM layoffs;


-- Now we create a staging table where we work on because in a real world scenario
-- data might be coming in from multiple sources. So to avoid any complications
-- we create a staging table where we work. 

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

-- The next part of the code is just for learning purpose

INSERT INTO layoffs_staging
VALUES ('Atlassian','Sydney','Other','500','0.05','3/6/2023','Post-IPO','Australia','210');

DELETE FROM layoffs_staging
WHERE location = 'Sydney';

DROP TABLE layoffs_staging;

-- Now we populate the new table with the data from the original source of raw data. 

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- We will clean the data in multiple parts.

-- 1. Removing duplicates. 
-- Now that we now if we use ROW_NUMBER combined with PARTITION BY, the entire dataset will be 
-- partitioned based on the specified columns, followed by numbering them. 
-- Now how ROW NUMBER works is that, it will number the rows 1, 2, 3, and so on for the similar ones,
-- And once a new type is encountered, it starts from 1 again. So any row with ROw Number greater thatn 1 is 
-- basically a duplicate of something. 

SELECT *,
row_number() OVER 
	(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country) as row_num
FROM layoffs_staging;

-- Now that we have the Row number, we need to find the oneec that is greater than 1. 
-- We can either do it via a Sub Query like below

SELECT *
FROM
	(	SELECT *,
		row_number() OVER 
			(
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country) as row_num
		FROM layoffs_staging
	) duplicates
WHERE row_num > 1;

-- Or the same thing can be done via CTE ( Common table expression)

WITH duplicate_CTE AS
(
	SELECT *,
		row_number() OVER 
			(
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country) as row_num
		FROM layoffs_staging
)
SELECT *
FROM duplicate_CTE
WHERE row_num > 1;

-- Now that we have the duplicate items, we may want to delete it like the below.
-- But it will not work because MySQL does not allow deletion on CTE Table. 

WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE;

-- So in order to delete the duplicate rows, we can create a new table (staging table 2) and then copy the items 
-- from staging table and then delete them.
-- Right click and copy in the left schemas panel and paste here. 

CREATE TABLE `layoffs_staging2` 
(
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT		-- One extra row for the row number column
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

-- Now we populate the new staging2 table. 

INSERT layoffs_staging2
SELECT *,
row_number() OVER 
	(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country) as row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- 2. Standardizing data: Finding issues/flaws in a dataset and fixing it

SELECT company, TRIM(company)
FROM layoffs_staging2;

-- First thing we can do is remove the any unnecessary spaces that might have been put by mistake.

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- If we look at the output, there are few things which are kind of repeatative. 
-- Like Crypto, Crypto Currency and CryptoCurrency. If we plot any graphor something, 
-- they would be counted as 3 different industries. 

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- SO we can update all the results to fall under one industry "Crypto" 

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE  industry LIKE 'Crypto%';

-- Next we check the locations column. We manually check the columns to find the issues/flaws

SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1;

-- We notice that there are 2 columnns with United States but one with a '.' at the end. 
-- Once way to do it is as below. Just like the previous one.
UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

-- Another way to do is to use a TRIM function with an add on.

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- The date column in the dataset is a text column. 
-- If in the future we want to plot a time based visualization,
-- it is important that we update this to a date field. 

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- So now we update the date column to DATE field

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

-- The items in the date column has been changed to date type. 
-- Howwever, the column is still in the text type. This can be confirmed by selecting it in the left panel. 

-- So we modify the table now. 
-- AND WE DO THIS TO THE STAGING TABLES AND NEVER TO THE ACTUAL TABLE.

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; 

-- 3. Look at the null values. 

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
