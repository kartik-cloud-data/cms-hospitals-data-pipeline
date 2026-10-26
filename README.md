# CMS Hospitals Data Pipeline

A Python data-engineering project that discovers datasets in the CMS Provider Data Catalog's **Hospitals** theme, downloads CSV data concurrently, converts column names to `snake_case`, and tracks successful downloads so unchanged datasets can be skipped.

## Features

- Discovers Hospitals datasets through the CMS Provider Data Catalog API.
- Retrieves each dataset's metadata and CSV download URL.
- Downloads and processes multiple datasets concurrently.
- Converts mixed-case headers, spaces, and punctuation into `snake_case`.
- Saves processed CSVs under `data/`.
- Tracks modification timestamps and successful downloads in `metadata.json`.
- Logs pipeline activity and errors to `logs/pipeline.log`.
- Runs on standard Windows or Linux Python; no AWS or Databricks dependency.

## Requirements

- Python 3.10 or newer
- Internet connection
- Packages listed in `requirements.txt`

## Run on Windows

Open Command Prompt in this project folder:

```powershell
py -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
python main.py
```

## Run on Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python main.py
```

## Output

- `data/`: downloaded CSVs with normalized headers.
- `metadata.json`: successful download state and last-run summary.
- `logs/pipeline.log`: timestamped logs.

Example header transformation:

| Original header | Normalized header |
|---|---|
| `Patients' rating of the facility linear mean score` | `patients_rating_of_the_facility_linear_mean_score` |
| `Hospital Name` | `hospital_name` |
| `ZIP Code` | `zip_code` |

The pipeline records metadata only after a CSV has been successfully downloaded and processed. If the source does not provide a modification timestamp, the script downloads that dataset again rather than risk incorrectly treating it as unchanged.

## Schedule it daily

### Windows Task Scheduler

1. Open **Task Scheduler** and select **Create Basic Task**.
2. Name it `CMS Hospitals Data Pipeline` and choose **Daily**.
3. For the action, choose **Start a program**.
4. Set **Program/script** to the full path of your Python executable.
5. Set **Add arguments** to the full path of `main.py`.
6. Set **Start in** to the project folder.
7. Save the task and test it using **Run**.

Use the Python executable from the environment where `requests` was installed.

### Linux cron

Run `crontab -e` and add a line like this, replacing the example path with your actual project path:

```cron
0 2 * * * /absolute/path/to/project/.venv/bin/python /absolute/path/to/project/main.py
```

## API reference

- [CMS Provider Data Catalog](https://data.cms.gov/provider-data/)
- [Provider Data API documentation](https://data.cms.gov/provider-data/docs)
- [Hospitals datasets](https://data.cms.gov/provider-data/datasets?theme%5B0%5D=Hospitals)

## Limitations and design notes

- The script relies on the CMS catalog's metadata and distribution download URLs; source API responses can evolve.
- Some datasets may not expose a direct CSV URL in the metadata. Such datasets are logged as skipped and should be reviewed.
- Run metadata is local to this project. If you delete `metadata.json`, the next run will download datasets again.
- The pipeline does not commit downloaded healthcare data to GitHub. Large generated CSVs and local metadata should remain local.

## License

For learning and demonstration. Follow CMS data-use terms and any applicable requirements when redistributing data.
