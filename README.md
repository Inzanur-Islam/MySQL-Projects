# Layoffs Data Cleaning

## Overview
This project demonstrates data cleaning techniques on a dataset of company layoffs. It includes:
- Removal of duplicates using `ROW_NUMBER` and `PARTITION BY`
- Standardization of text data with functions like `TRIM`
- Conversion of date fields from text to date format
- Handling null values

## Steps
1. **Create a Staging Table**: Work on staging data to avoid complications in production systems.
2. **Remove Duplicates**: Use `ROW_NUMBER` to identify and delete duplicate entries.
3. **Standardize Data**: Trim spaces, unify inconsistent text, and format date columns.
4. **Handle Null Values**: Replace or address missing data fields.
