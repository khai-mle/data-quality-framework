# EDA-DataQuality-Framework

## Overview

This EDA-DataQuality-Framework is a Python-based solution designed for comprehensive data quality assessment. It provides tools for data validation, cleaning, profiling, and fuzzy matching. Originally built to interface directly with SQL Server databases, it has been adapted to also process local CSV files, making it flexible for various development and analysis scenarios.

The framework is driven by configurations defined in Python scripts, primarily `table_mapping.py`, allowing users to specify how different datasets (whether from CSVs or database tables) should be processed, what data quality rules to apply, and which columns to target.

## Key Features

*   **CSV File Processing:** Directly analyze local CSV files.
*   **Data Validation:** Perform various checks, including:
    *   Null value detection (`find_nulls`)
    *   Exact duplicate detection (`find_duplicates`)
    *   Address validation and cleaning (`address_cleaning`)
    *   Phone number validation and formatting (`phone_cleaning`)
    *   Email syntax and validity checks (`Email` class)
    *   URL validation (`Website` class)
    *   Proper name checks (`UserName` class)
    *   Numeric outlier detection (Z-score and IQR methods - `Outliers` class)
*   **Configurable Rules:** Define data sources, primary keys, reference fields, and specific columns for validation in `table_mapping.py`.
*   **Custom Dictionaries:** Utilize `dictionaries.py` for lookups and defining sets of valid values.
*   **Reporting:** Generates various reports in the `output_files/` directory (primarily CSV format in the current mode), including:
    *   Exact duplicates
    *   Nulls in required fields
    *   Detailed validation results per data type (address, phone, email, etc.)
    *   Outlier reports
    *   Frequency counts for specified columns
    *   A summary validation CSV per input file, including a `row_is_valid` flag.
*   **Database Capabilities (Currently Commented Out):**
    *   Direct connection to MS SQL Server.
    *   Execution of SQL queries to fetch data.
    *   Writing profiling and validation results back to database tables.
*   **Modularity:** Organized into specific Python scripts for different functionalities (connection handling, cleaning, validation).

## Setup

1.  **Clone the Repository:**
    ```bash
    git clone <your_repository_url>
    cd eda-dataquality-framework
    ```

2.  **Python Version:**
    This framework was developed with Python 3.12 (as noted in `SETUP_NOTES.txt`). Ensure you have a compatible Python version.

3.  **Create and Activate a Virtual Environment (Recommended):
    ```bash
    python -m venv venv
    # On Windows
    .\venv\Scripts\activate
    # On macOS/Linux
    source venv/bin/activate
    ```

4.  **Install Dependencies:**
    A `requirements.txt` file is provided. Install the necessary packages:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

The primary configuration for processing specific datasets (CSV files or database tables) is done in **`table_mapping.py`**.

*   You need to add an entry for each CSV file (or database table) you intend to process.
*   The key for the entry should be the basename of the CSV file (without the `.csv` extension) or the database table name.
*   Within each entry, you define:
    *   `db_primary_key`: List of column(s) forming the primary key.
    *   `reference_fields`: Other fields to include in base reports.
    *   Fields for specific validations: `address` (with nested structure for address parts), `phone`, `email`, `url`, `name`, `z_num_outliers`, `iq_num_outliers`, `duplicates`, `isNull`.
    *   Ensure the column names used in `table_mapping.py` exactly match the headers in your CSV files (or column names in database tables).

Refer to `dictionaries.py` if you need to customize lists of valid values or other lookup data used during validation.

## Usage (CSV File Processing Mode)

The framework is currently set up to process local CSV files using the `data_validation_reporting.py` script.

1.  **Place your CSV files:** For simplicity, you can place the CSV files you want to analyze in the root directory of the project.

2.  **Run the Script:**
    Execute `data_validation_reporting.py` from the command line, followed by the paths to your CSV files:
    ```bash
    python data_validation_reporting.py your_file1.csv path/to/your_file2.csv ...
    ```
    For example:
    ```bash
    python data_validation_reporting.py Logistics_DQPilot_CONTACT.csv Logistics_DQPilotACCOUNT.csv
    ```

3.  **Output:**
    The script will generate various CSV reports in the `output_files/` directory. These include reports on duplicates, nulls, detailed validation checks, and a summary CSV for each input file with validation flags.

### Important Note on Input Data Files:

*   Large or sensitive data CSV files (like the example `Logistics_DQPilot_CONTACT.csv` and `Logistics_DQPilotACCOUNT.csv`) are **not tracked by Git** and are listed in the `.gitignore` file. You should keep your actual data files local and not commit them to the repository.
*   If you need to share sample data for demonstration or testing within the repository, create a `sample_data/` directory, place small, anonymized versions of your CSV files there, and commit this directory.

## Future Database Mode

This framework was originally designed to work directly with MS SQL Server databases.

*   The database-related logic in scripts like `connection_handler.py`, `data_validation_reporting.py`, and others is currently commented out to enable CSV processing.
*   To revert to database mode:
    *   You would need to uncomment the relevant sections in these scripts.
    *   Ensure the SQL Server instances are accessible and configured as per `SETUP_NOTES.txt`.
    *   The framework uses SQL files (located in `sql_files_*/` directories) to define data sources.
*   `SETUP_NOTES.txt` contains details on the original database setup, required ODBC drivers, and server information.

## Key Files & Directories

*   `data_validation_reporting.py`: Main script for running validation reports (adapted for CSV).
*   `connection_handler.py`: Manages database connections (DB part commented out).
*   `data_cleaning.py`: Contains various data cleaning and transformation functions.
*   `data_validation.py`: Defines validation classes and logic.
*   `table_mapping.py`: **Crucial configuration file** for defining dataset processing rules.
*   `dictionaries.py`: Stores custom lists and dictionaries for validation.
*   `file_path.py`: Utility for managing file paths.
*   `requirements.txt`: Lists Python dependencies.
*   `.gitignore`: Specifies intentionally untracked files by Git.
*   `output_files/`: Default directory for generated CSV reports.
*   `html_output_files/`: (For future use with HTML reports, e.g., from `ydata-profiling`).
*   `sql_files_*/`: Directories containing SQL queries for database mode.
*   `sql_mapping/`: Contains SQL for fetching table/column metadata in DB mode.
*   `unitTests.py`, `unitTests_address_cleaning.py`: Unit tests for the framework.
*   `SETUP_NOTES.txt`: Original setup instructions and notes, useful for DB context.
*   `archive/`: Contains archived files. Review its contents to determine if it's needed for your current repository.
*   `excel_files/`: Contains Excel-related mapping or files. Review if these are templates or old data.

## Contributing

(Placeholder for future contribution guidelines, if any. For now, ensure code is well-commented, and consider adding more unit tests for new functionality.)

## Getting started

To make it easy for you to get started with GitLab, here's a list of recommended next steps.

Already a pro? Just edit this README.md and make it your own. Want to make it easy? [Use the template at the bottom](#editing-this-readme)!

## Add your files

- [ ] [Create](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#create-a-file) or [upload](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#upload-a-file) files
- [ ] [Add files using the command line](https://docs.gitlab.com/ee/gitlab-basics/add-file.html#add-a-file-using-the-command-line) or push an existing Git repository with the following command:

```
cd existing_repo
git remote add origin https://gitrepo.tierpoint.com/EDA-DataQuality-Framework/eda-dataquality-framework.git
git branch -M main
git push -uf origin main
```

## Integrate with your tools

- [ ] [Set up project integrations](https://gitrepo.tierpoint.com/EDA-DataQuality-Framework/eda-dataquality-framework/-/settings/integrations)

## Collaborate with your team

- [ ] [Invite team members and collaborators](https://docs.gitlab.com/ee/user/project/members/)
- [ ] [Create a new merge request](https://docs.gitlab.com/ee/user/project/merge_requests/creating_merge_requests.html)
- [ ] [Automatically close issues from merge requests](https://docs.gitlab.com/ee/user/project/issues/managing_issues.html#closing-issues-automatically)
- [ ] [Enable merge request approvals](https://docs.gitlab.com/ee/user/project/merge_requests/approvals/)
- [ ] [Set auto-merge](https://docs.gitlab.com/ee/user/project/merge_requests/merge_when_pipeline_succeeds.html)

## Test and Deploy

Use the built-in continuous integration in GitLab.

- [ ] [Get started with GitLab CI/CD](https://docs.gitlab.com/ee/ci/quick_start/index.html)
- [ ] [Analyze your code for known vulnerabilities with Static Application Security Testing (SAST)](https://docs.gitlab.com/ee/user/application_security/sast/)
- [ ] [Deploy to Kubernetes, Amazon EC2, or Amazon ECS using Auto Deploy](https://docs.gitlab.com/ee/topics/autodevops/requirements.html)
- [ ] [Use pull-based deployments for improved Kubernetes management](https://docs.gitlab.com/ee/user/clusters/agent/)
- [ ] [Set up protected environments](https://docs.gitlab.com/ee/ci/environments/protected_environments.html)

***

# Editing this README

When you're ready to make this README your own, just edit this file and use the handy template below (or feel free to structure it however you want - this is just a starting point!). Thanks to [makeareadme.com](https://www.makeareadme.com/) for this template.

## Suggestions for a good README

Every project is different, so consider which of these sections apply to yours. The sections used in the template are suggestions for most open source projects. Also keep in mind that while a README can be too long and detailed, too long is better than too short. If you think your README is too long, consider utilizing another form of documentation rather than cutting out information.

## Name
Choose a self-explaining name for your project.

## Description
Let people know what your project can do specifically. Provide context and add a link to any reference visitors might be unfamiliar with. A list of Features or a Background subsection can also be added here. If there are alternatives to your project, this is a good place to list differentiating factors.

## Badges
On some READMEs, you may see small images that convey metadata, such as whether or not all the tests are passing for the project. You can use Shields to add some to your README. Many services also have instructions for adding a badge.

## Visuals
Depending on what you are making, it can be a good idea to include screenshots or even a video (you'll frequently see GIFs rather than actual videos). Tools like ttygif can help, but check out Asciinema for a more sophisticated method.

## Installation
Within a particular ecosystem, there may be a common way of installing things, such as using Yarn, NuGet, or Homebrew. However, consider the possibility that whoever is reading your README is a novice and would like more guidance. Listing specific steps helps remove ambiguity and gets people to using your project as quickly as possible. If it only runs in a specific context like a particular programming language version or operating system or has dependencies that have to be installed manually, also add a Requirements subsection.

## Usage
Use examples liberally, and show the expected output if you can. It's helpful to have inline the smallest example of usage that you can demonstrate, while providing links to more sophisticated examples if they are too long to reasonably include in the README.

## Support
Tell people where they can go to for help. It can be any combination of an issue tracker, a chat room, an email address, etc.

## Roadmap
If you have ideas for releases in the future, it is a good idea to list them in the README.

## Contributing
State if you are open to contributions and what your requirements are for accepting them.

For people who want to make changes to your project, it's helpful to have some documentation on how to get started. Perhaps there is a script that they should run or some environment variables that they need to set. Make these steps explicit. These instructions could also be useful to your future self.

You can also document commands to lint the code or run tests. These steps help to ensure high code quality and reduce the likelihood that the changes inadvertently break something. Having instructions for running tests is especially helpful if it requires external setup, such as starting a Selenium server for testing in a browser.

## Authors and acknowledgment
Show your appreciation to those who have contributed to the project.

## License
For open source projects, say how it is licensed.

## Project status
If you have run out of energy or time for your project, put a note at the top of the README saying that development has slowed down or stopped completely. Someone may choose to fork your project or volunteer to step in as a maintainer or owner, allowing your project to keep going. You can also make an explicit request for maintainers.
