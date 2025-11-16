## Setup Instructions
1. Download docker cli
2. Download and install docker compose plugin
```bash
# Download Compose V2 plugin\
mkdir -p ~/.docker/cli-plugins\
curl -SL https://github.com/docker/compose/releases/download/v2.21.0/docker-compose-linux-x86_64 -o ~/.docker/cli-plugins/docker-compose\
\
# Make it executable\
chmod +x ~/.docker/cli-plugins/docker-compose\
\
# Verify\
docker compose version
```

3. Clone the repository
4. Navigate to the repo folder and run the following command to build and start the backend server
```bash
docker compose up --build
```
5. The backend server should now be running at `http://localhost:8000` , along with other associated services like the NATS server and the backend workers.

## Testing the API

The server exposes a simple API for document ingestion and TF-IDF based search. You can use tools like `curl` or Postman to interact with the API.

### Ingest Documents
To ingest documents, send a POST request to the `/ingest` endpoint with the document file
```bash
curl -X POST "http://localhost:8000/ingest" -F "file=@path_to_your_document.txt"
```
### Search Documents
To search for terms in the ingested documents, send a GET request to the `/search` endpoint with the query parameter
```bash
curl -X GET "http://localhost:8000/search?query=your_search_term"
```