import azure.functions as func
import logging

app = func.FunctionApp(auth_level=func.AuthLevel.ANONYMOUS)

@app.function_name(name="HttpTrigger1")
@app.route(route="hello", methods=["POST"])
def test_function(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
    except ValueError:
        pass
    else:
        name = req_body.get('name')

        if name:
            return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    
    return func.HttpResponse(
        "This HTTP triggered function executed successfully. Pass a name in the request body for a personalized response.",
        status_code=200
    )
