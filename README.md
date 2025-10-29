### Leukerbad Dynamic Pricing ETL



#### Overview



This project retrieves live price information for dynamically priced tickets (ski tickets and winter cards) from the \*\*Pricenow\*\* API, cleans and standardizes the data, and updates the \*\*LeukerbaDB\*\* database hosted on \*\*Supabase\*\*.



The script is built in Python and can be run manually for testing or automatically on a schedule via \*\*GitHub Actions\*\*.



---



#### Data Flow



1\. \*\*Extract\*\* – Connects to the Pricenow API using private access keys.

2\. \*\*Transform\*\* – Cleans and normalizes raw data (for example, adjusting field names, handling missing values, and standardizing status labels).

3\. \*\*Load\*\* – Pushes the processed data into the LeukerbaDB (Supabase) database, updating existing records as needed.



---



#### Project Structure



```

.

├─ scripts/

│  └─ pricenow_etl.py          # Main ETL script

├─ .github/

│  └─ workflows/

│     └─ run_pricenow.yml          # GitHub Actions workflow

├─ .env.example           # Example environment variables

├─ .gitignore             # Ignored files and folders

├─ requirements.txt       # Python dependencies

└─ README.md

```



---



#### Running the ETL Locally



1\. \*\*Install dependencies\*\*



&nbsp;  ```bash

&nbsp;  python -m venv .venv

&nbsp;  source .venv/bin/activate      # Windows: .venv\\Scripts\\activate

&nbsp;  pip install -r requirements.txt

&nbsp;  ```



2\. \*\*Set up environment variables\*\*

&nbsp;  Copy the example file and fill in your actual credentials:



&nbsp;  ```bash

&nbsp;  cp .env.example .env

&nbsp;  ```



&nbsp;  Then edit `.env`:



&nbsp;  ```

&nbsp;  API\_BASE\_URL=https://api.infosnow.ch

&nbsp;  API\_PUBLIC\_KEY=your\_public\_key

&nbsp;  API\_PRIVATE\_KEY=your\_private\_key

&nbsp;  DATABASE\_URL=postgresql+psycopg://user:password@db.supabase.co:5432/leukerbaddb

&nbsp;  ```



3\. \*\*Run the script\*\*



&nbsp;  ```bash

&nbsp;  python scripts/run\_etl.py

&nbsp;  ```



---



#### Automated Updates with GitHub Actions



The workflow file `.github/workflows/run_pricenow.yml` runs the ETL automatically twice a day and can also be triggered manually from the GitHub Actions tab.



Secrets are stored securely under \*\*Settings → Environments → prod → Secrets\*\* and injected at runtime.

Required secrets/env variables:

\* `PRICENOW\_CLIENT\_ID`
\* `PRICENOW\_CLIENT\_SECRET`
\* `AUDIENCE`
\* `GRANT\_TYPE`
\* `AUTH\_VERSION\_HEADER` 
\* `PRICING\_API\_VERSION`
\* `SUPABASE\_URL`
\* `SUPABASE\_KEY`

---



#### Security



\* No credentials are committed to the repository.

\* The local `.env` file is excluded via `.gitignore`.

\* GitHub Actions secrets are encrypted and masked in logs.

\* Database credentials are limited to only the permissions necessary for inserts and updates.



---



#### Technology Stack



| Component      | Technology                                                  |

| -------------- | ----------------------------------------------------------- |

| Language       | Python 3.11+                                                |

| Data Source    | Pricenow APIs                                               |

| Database       | Supabase (PostgreSQL)                                       |

| Scheduler / CI | GitHub Actions                                              |

| Dependencies   | requests, pandas, supabase, python-dotenv, etc.             |



---



#### Maintained by



\*\*Anka Güldenpfennig\*\*

Leukerbad, Switzerland

\[anka.gueldenpfennig@leukerbad.ch](mailto:anka.gueldenpfennig@leukerbad.ch)

