# DE-ML-Projects
![PW Staking](https://github.com/Hbowe/DE-ML-Projects/assets/122368075/3563cee1-ec01-48c5-81ec-9f24c14dfce2)

Data pipeline scraping the PlanetWatch blockchain (built on Algorand) for user participation in a staking incentive program running 02/23-02/24

![Blank diagram](https://github.com/Hbowe/DE-ML-Projects/assets/122368075/2a35c5e8-4339-4270-afab-fdf1742a446a)

Raw JSON data is scraped via the Algod API. JSON files come segmented into pre-set length pages with a token value at the bottom of each page pointing to the next page of results. 

Files are stored in the temp directory for the duration of scraping loop before being moved into raw individual directories. 

Files are cleaned and stored as a list of dataframes to be concatonated into the existing cleaned and consolidate file created and maintaed from prior runs. 

Cleaned files are uploaded to SQL server via a SQL-Alchemy connection and dashboarded via Apache Superset.
