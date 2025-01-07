# Prototype
In this prototype, we'll work with real-world data using Python to obtain it<br>
directly from a webpage using Web Scraping techniques, transforming the data<br>
to meet the given requirements, and saving it as a local file as a table in a database.<br>
We will also run basic queries on that database using Python.<br>
<br>
Basic concepts of the <strong>ETL (extract-transform-load)</strong> process will be applied, extracting<br>
the required information using <strong>Web Scraping</strong> and <strong>APIs</strong>, developing a functional<br>
<strong>ETL Pipeline</strong> for the stages of data acquisition and processing of ingested data in multiple formats<br>
from a piblic domain.<br>
<br>
Finally, we will create modules, run unit tests, <strong>Package Applications</strong>, and perform a <strong>Static Code<br>
Analysis</strong> also using Python.

<h2>Scenario</h2>
We are tasked with creating an automated script that can extract the list of all countries in order of their GDPs in billion USDs (rounded to 2 decimal places), as logged by the International Monetary Fund (IMF). Since IMF releases this evaluation twice a year, this code will be used by the organization to extract the information as it is updated.

The required data seems to be available on the following <a href="https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29" target="_blank">URL</a>

<p>
The required information needs to be made accessible as a <strong>CSV</strong> file 
<strong>Countries_by_GDP.csv</strong> as well as a table <strong>Countries_by_GDP</strong> 
in a database file <strong>World_Economies.db</strong> with attributes 
<strong>Country</strong> and <strong>GDP_USD_billion</strong>.
</p>

<p>
To demonstrate the success of this code, we'll run a query on the database table to display only the entries with more than a 100 billion USD economy. Also, we'll log in a file with the entire process of execution named 
<strong>etl_project_log.txt</strong>.
</p>

<p>
We'll create a Python code <strong>etl_project_gdp.py</strong> that performs all the required tasks.
</p>

<h2>Objectives</h2>
<p>
  Write a data extraction function to retrieve the relevant information from the required URL.
  
  Transform the available GDP information into 'Billion USD' from 'Million USD'.
  
  Load the transformed information to the required CSV file and as a database file.
  
  Run the required query on the database.
  
  Log the progress of the code with appropriate timestamps.
</p>
