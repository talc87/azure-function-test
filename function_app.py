import azure.functions as func
import logging
from .blueprint import CreateDWHTable

@app.route('/api/hello', methods=['POST'])
def hello(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Azure Function API received an HTTP requests @CreateDWHTable, triggering function processed a request.')

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "Invalid JSON request body.",
            status_code=400
        )

    
    logging.info('JSON request body was validated')
    
    DWHConnection = CreateDWHTable(req_body['ExtractionConfig'],req_body['MySQLConnectionString'])
    cDWHTables = DWHConnection.CreateTablesObjects()
    
    
    
    
    return func.HttpResponse(cDWHTables)
