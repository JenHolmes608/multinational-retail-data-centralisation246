# multinational-retail-data-centralisation246

Scenario: You work for a multinational company that sells various goods across the globe.

Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.

In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.

Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.

You will then query the database to get up-to-date metrics for the business.

The 'data_utils.py' file contains the 'DatabaseConnector' class which connects to the given database, ready to extract the data. It also connects to a local database so the cleaned data can be exported there.

The 'data_extraction.py' file contains the 'DatabasExtractor' class which extracts data from the database, pdf files and json files ready for the data to be cleaned.

The 'data_cleaning.py' file contains the 'DatabaseCleaning' class which cleans the data depending on its requirements. 

The 'mrdc_project.ipynb' file imports the 3 previous files to extract, clean and transfer the data to a local database. The database is now ready to be queried to metrics fro the business.

To implement running the project, git clone the repo and run the file in the terminal. Python is required:

python milestone_5.py

To run the project, credentials for where to extract data to and where to upload it once cleaned must be given in a yaml file. The project also needs to be connected to AWS and a bucket must be defined. You will also need the endpoint url or the json url. When extracting data from a pdf file, the link must be given. 