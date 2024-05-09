import logging 
import os, sys
import json
from datetime import datetime

import azure.functions as func 
from translate import translate_text, \
    translate_docs, \
    translate_docs_bactch_status, \
    upload_blob_stream, \
    download_output_blob_stream, \
    list_blob_files, \
    delete_blob_file, \
    delete_blob

#from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential, get_bearer_token_provider

### setup
os.environ["LOGGING_LEVEL"] = "WARNING"
APPLICATION_TEXT = "application/text"
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

#TODO download ->download-output
     

#NOTE : change 1
@app.route(route="translate-text",methods=["POST"]) 
def translate_text_post(req: func.HttpRequest) -> func.HttpResponse:  
    #this_func = sys._getframe().f_code.co_name
    #logging.info(f'Processing {this_func}') 
    
    try:
        param_list = ['model_category', 'from_lang', 'to_lang']
        params = {}
        req_data = req.get_json()
        req_params = req.params
        for p in param_list:
            params[p] = req_params.get(p)
        text = translate_text(text=req_data, **params)
        result = {"text": text}
        
    except Exception as e:
        logging.error('Error: %s', e)
        result = {"Error": f"Error in function translate_text_post."}
    return func.HttpResponse( 
                json.dumps({ "result":result}).encode('utf8'),
                status_code=200, mimetype="application/json")

# NOTE: change 2                
@app.route(route="upload-blob",methods=["POST"]) 
def upload_input_blob_post(req: func.HttpRequest) -> func.HttpResponse:  
    #this_func = sys._getframe().f_code.co_name
    #logging.info(f'Processing {this_func}') 
    try:
        param_list = ['user_id', 'file_name']
        params = {}
        file_data = req.get_body()
        req_params = req.params
        for p in param_list:
            params[p] = req_params.get(p)
        result =  upload_blob_stream(**params, file_data=file_data)
        blob_name = result["blob_name"]
        try:
            files = list_blob_files(blob_name=blob_name, container_type="input")
            result.update(files)#append the files dictionary
        except Exception as e:
            result.update({"Error": e.args})
    except Exception as e:
        logging.error('Error: %s', e)
        result = {"Error": f"Error in function upload_input_blob_post."}
    return func.HttpResponse( 
                json.dumps({ "result":result}).encode('utf8'),
                status_code=200, mimetype="application/json")


@app.route(route="download-output-blob",methods=["POST"]) 
def download_output_blob_post(req: func.HttpRequest) -> func.HttpResponse:  
    #this_func = sys._getframe().f_code.co_name
    #logging.info(f'Processing {this_func}') 
    try:
        param_list = ["blob_name", "file_name"]
        params = {}
        req_params = req.params
        for p in param_list:
            params[p] = req_params.get(p)
        result =  download_output_blob_stream(**params)
        resp_params = { "stream_bytes": result["stream_bytes"]}
        resp_body =  result["stream_data"]
    except Exception as e:
        logging.error('Error: %s', e)
        result = {"Error": f"Error in function download_output_blob_post."}
        resp_body = json.dumps({ "result":result}).encode('utf8')
        resp_params = { "stream_bytes": 0}
    return func.HttpResponse( 
                resp_body,
                headers=resp_params,
                status_code=200, mimetype="application/json-data-stream")


@app.route(route="list-blob-files",methods=["POST"]) 
def list_blob_files_post(req: func.HttpRequest) -> func.HttpResponse:  
    #this_func = sys._getframe().f_code.co_name
    #logging.info(f'Processing {this_func}') 
    try:
        param_list = ['container_type','blob_name']
        params = {}
        req_params = req.params
        for p in param_list:
            params[p] = req_params.get(p)
        files = list_blob_files(**params)
        result = files
    except Exception as e:
        logging.error('Error: %s', e)
        result = {"Error": f"Error in function list_blob_files_post."}
    return func.HttpResponse( 
                json.dumps({ "result":result}).encode('utf8'),
                status_code=200, mimetype="application/json")


@app.route(route="delete-blob-file",methods=["POST"]) 
def delete_blob_file_post(req: func.HttpRequest) -> func.HttpResponse:  
    #this_func = sys._getframe().f_code.co_name
    #logging.info(f'Processing {this_func}') 
    try:
        param_list = ['container_type','blob_name', 'file_name']
        params = {}
        req_params = req.params
        for p in param_list:
            params[p] = req_params.get(p)
        result = delete_blob_file(**params)
    except Exception as e:
        logging.error('Error: %s', e)
        result = {"Error": f"Error in function delete_blob_file_post."}
    return func.HttpResponse( 
                json.dumps({ "result":result}).encode('utf8'),
                status_code=200, mimetype="application/json")


@app.route(route="delete-blob",methods=["POST"]) 
def delete_blob_post(req: func.HttpRequest) -> func.HttpResponse:  
    #this_func = sys._getframe().f_code.co_name
    #logging.info(f'Processing {this_func}') 
    try:
        param_list = ['container_type','blob_name']
        params = {}
        req_params = req.params
        for p in param_list:
            params[p] = req_params.get(p)
        result = delete_blob(**params)
    except Exception as e:
        logging.error('Error: %s', e)
        result = {"Error": f"Error in function delete_blob_post."}
    return func.HttpResponse( 
                json.dumps({ "result":result}).encode('utf8'),
                status_code=200, mimetype="application/json")

#NOTE : change 3
@app.route(route="translate-docs",methods=["POST"]) 
def translate_docs_post(req: func.HttpRequest) -> func.HttpResponse:  
    #this_func = sys._getframe().f_code.co_name
    #logging.info(f'Processing {this_func}') 
    
    try:
        param_list = ['blob_name', 'model_category', 'from_lang', 'to_lang','glossary_csvs']
        params = {}
        req_data = req.get_json()
        for p in param_list:
            params[p] = req_data.get(p)
        result =  translate_docs(**params)
    except Exception as e:
        logging.error('Error: %s', e)
        result = {"Error": f"Error in function translate_docs_post."}
    return func.HttpResponse( 
                json.dumps({ "result":result}).encode('utf8'),
                status_code=200, mimetype="application/json")

                
@app.route(route="get-batch-status", methods=["GET","POST"]) 
def get_batch_status(req: func.HttpRequest) -> func.HttpResponse:  
    #this_func = sys._getframe().f_code.co_name
    #logging.info(f'Processing {this_func}') 
    params = {}
    try:
        try:
            batch_id = req.route.get('batch_id')
        except:
            batch_id = req.params.get('batch_id')
        params["batch_id"] = batch_id
        batch_status =  translate_docs_bactch_status(**params)
        status = batch_status["status"]
        result = {"status": status, "status_details": batch_status}
    except Exception as e:
        logging.error('Error: %s', e)
        result = {"Error": f"Error in function get_batch_status."}
    return func.HttpResponse(
                json.dumps({ "result":result}).encode('utf8'),
                status_code=200, mimetype="application/json")

        
@app.route(route="health-check", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS) 
def health_check(req: func.HttpRequest) -> func.HttpResponse: 
    #this_func = sys._getframe().f_code.co_name
    #logging.info(f'Processing {this_func}') 
    return func.HttpResponse( 
            "Health check passed.", 
            status_code=200, mimetype="APPLICATION_TEXT")


@app.route(route="translator-check", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS) 
def translator_check(req: func.HttpRequest) -> func.HttpResponse: 
    #this_func = sys._getframe().f_code.co_name
    #logging.info(f'Processing {this_func}') 
    
    input_text = "Hello, welcome to custom translator:)"
    result = translate_text(
        text=input_text,
        model_category="general", 
        from_lang="en", 
        to_lang="fr"
    )
    resp_result = f"Model: 'general'\nInput (En): {input_text}\nOuput (Fr): {result}"
    return func.HttpResponse( 
            resp_result.encode('utf8'), 
            status_code=200, mimetype="APPLICATION_TEXT")
