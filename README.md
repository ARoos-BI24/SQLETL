## ETL

These files were first locally and some were worked as part of a private repo with a test folder, and moving them to their own repo lost commit info.

Documentation was limited to coding comments as asked, in retrospect it is really hard to remember what challenges I had.

<details>
<summary>Project_1 requirements</summary>
<p>

# Real-World Banking ETL

### Background

You are an analytics engineer at a bank. Each month, a third-party agency provides a file containing updated vehicle information – including model names, energy classes (eg., electric, gas, diesel), and other specifications.

Currently, a business analyst manually runs queries to calculate estimated profits per vehicle and to generate new auto loan offers. These calculations are based on:

·         The energy class of the car (e.g., electric, gasoline, diesel)

·         The floating interest rate, which is updated monthly

·         The bank’s margin that is added on top of the base rate

---

### Requirements

You are required to build a SQL-based ETL job in SSMS (SQL Server Management Studio) that automatically:


·         Ingests the monthly vehicle data into a staging table.

·         Validates and transforms the data (e.g., corrects data types and formats).

·         Loads the enriched data into a final target table. The business analyst should be able to simply run “SELECT * FROM LoanProfitEstimates_yyyymm;” to get the final results each month.

---

### Critera

·         Dynamically select the correct CarInformation_yyyymm table based on the current month.

·         If the base interest rate is missing for the current month, fall back to the most recent available month.

·         Generate the output as a new results table named LoanProfitEstimates_yyyymm, with no errors

·         Include clear and well-structured code, with explanatory comments and efficient performance.

·         Implement logging to capture errors with descriptive messages, such as:

o   "Base interest rate for the current month is missing. Value from the previous month was used."

o   "CarInformation_yyyymm table for the current month not found."

·         Send a confirmation email after execution, stating whether the job/jobs completed successfully and including details such as the number of rows processed and the name of the generated output table.

---

</p>
</details>

<details>
<summary>Project_2 requirements</summary>
<p>

# End-to-End ETL

### Background

You are a consultant from a company bidding on a project with a bank.

The objective is to redesign an end-to-end data pipeline focused on the loan portfolio and to develop an interactive dashboard in Power BI or Excel via SSAS.

The business stakeholder has requested a report on the development of the loan portfolio, although the specific metrics and reporting requirements have not yet been finalized.

Due to data confidentiality, the bank has provided only three masked tables with sample data for this proof of concept (POC), which have been extracted from their existing analytical layer.

---

### Data

The sample data is provide as a excel file and it contains three tables, you can consider them as three raw data sources that gradually increase over time:

·         Loan_Account

·         Loan_Account_Balance

·         Loan_Account_Transaction

--- 

### Task

•       Transform the raw data (Raw_Loan_Account, Raw_Loan_Balance, and Raw_Loan_Transaction) into a clean and consistent data layer.

•       Build an analytical layer using fact and dimension tables.

•       Export the data to SSAS or Power BI, and build a data model and dashboard that can be shown to stakeholders as a starting point for reporting.

---

### Criteria


·         Create an end-to-end pipeline from raw data -> cleansed layer -> analytical layer that can be successfully run at least at the first time.

·         Create a dashboard that shows the following information dynamically:

o   Loan Account

o   Loan Transaction

o   Transaction Amount

o   Loan Balance

·         Include clear and well-structured code/ orchestration, with explanatory comments and efficient performance.

·         Each job can be run multiple times without changing the output if the source data hasn't changed.

·         Making the table in the analytical layer that contains the information of Loan Account even contains the historical data.

</p>
</details>
