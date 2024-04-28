# TEFAS Data Scraper

This Python script is designed to scrape fund data from the TEFAS website and store it in a MySQL database. The script uses a configuration file for database and logging settings.

## Requirements

- Python 3.6 or newer
- MySQL Server
- A set of Python packages listed in `requirements.txt`

## Setup

### Install Dependencies

Before running the script, you need to install the required Python packages. Run the following command:

```bash
pip install -r requirements.txt
```

### Configuration

Modify the `config.ini` file in the root directory to match your database and logging settings:

```ini
[database]
user =
password =
host =
database =

[logging]
level = INFO
; file = app.log
```

### Database

Ensure that your MySQL server is running and accessible using the credentials provided in `config.ini`. The script will create necessary tables automatically.

## Usage

Run the script using the following command:

```bash
python tefas-crawl-daily-data.py
```
