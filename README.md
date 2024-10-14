# downloader

Simple utility to download files under network interruptions using async framework

## Dependencies
- Python 3.10+

## Usage

### Command line
```shell
python downloader <url> <output_file>
```

### REST API server

Install requirements:
```shell
pip install -r requirements.txt
```
Start the server:
```shell
fastapi dev downloader/api.py
```
Call the endpoint:
```shell
curl -H  "Content-Type: application/json" -d '{"files_to_download": ["my_file", "my_other_file"], "url": "https://my.url.path/"}' http://localhost:8000/download_files
```
