# Data Management Backbone
Data management backbone project for UPC Data Science Master ADSDB

This projects consists on implementing a data pipeline from some source to a basic preprocessed state
The pipeline is divided in four zones
 1. Landing zone: Download the dataset and store the diferent versions
 2. Formatted zone: Standardize all dataset files to DB tables
 3. Trusted zone: Join all versions of the same dataset and apply basic data cleaning and transformations
 4. Exploitation zone: Join, reshape and restructure the different tables around the relevant entities of the datasets
