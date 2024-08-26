import ast
import json
import os
import re
import urllib
from queue import Queue
from dotenv import load_dotenv
import google.generativeai as genai
import PIL.Image
import requests
import socketio
from boxes import DataConverter, OCRExtractor
from functional_update import (add_serial_numbers_to_list, empty_folder,
                               format_data, format_data_receipt, merge_data,
                               modify_item_details, process_data,
                               remove_commas_and_periods, remove_leading_space)

load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GENAI_MODEL = "models/gemini-1.5-flash-latest"
IMAGE_URL_PREFIX = "https://devdtdemo-image.aspiresoftware.in/"
INPUT_RECEIVE_URL = "https://devdtdemo-api.aspiresoftware.in/kafka/receive-input"
OUTPUT_SEND_URL = "https://devdtdemo-api.aspiresoftware.in/kafka/send-output"
RESULT_PDF_PATH = "result/pdf/"
RESULT_IMAGE_PATH = "result/image/"
RESULT_DB_PATH = "result/DB/"
LLM_OUTPUT_FILE = "result/LLM/output.txt"

# Initialize Google Generative AI
genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel(GENAI_MODEL)

# Initialize threading and socket
lock = threading.Lock()
socket_queue = Queue()
sio = socketio.Client(logger=True)
sio.connect("https://devdtdemo-ws.aspiresoftware.in")

def process_single_message():
    global socket_queue
    with lock:
        if not socket_queue.empty():
            message = socket_queue.get()
            result = invoice_details(message)
            print(result)
            try:
                process_id = str(result["process_id"])
                flow_id = str(result["flow_id"])
                doc_type = result["type"]
                result1 = result.get("result", "null")
                stat = "Completed" if "error" not in result else "Failed"
                processed_message = {
                    "process_id": process_id,
                    "status": stat,
                    "type": doc_type,
                    "flow_id": flow_id,
                    "result": result1,
                }
                requests.post(OUTPUT_SEND_URL, json=processed_message)
                print("done")
            except Exception as e:
                print(e)

def invoice_details(message):
    try:
        data = {}
        flow_id = ""
        process_id = ""
        doc_type = ""
        if message == "Message Pushed into Input queue":
            input_request = requests.get(INPUT_RECEIVE_URL)
            input_request_data = input_request.json()
            data = input_request_data[0]
            process_id = str(data.get("process_id"))
            flow_id = data.get("flow_id", "")
            doc_type = data.get("type", "")
            status = "InProgress"
            initial_message = {
                "process_id": process_id,
                "flow_id": flow_id,
                "type": doc_type,
                "status": status,
            }
            requests.post(OUTPUT_SEND_URL, json=initial_message)
        
        image_url = data.get("image_url", "")
        image_url = image_url.replace("http://192.168.2.25:9014/", IMAGE_URL_PREFIX)
        file_name = os.path.basename(image_url)
        file_extension = os.path.splitext(file_name)[1].lower()
        file_path = os.path.join(RESULT_PDF_PATH if file_extension == ".pdf" else RESULT_IMAGE_PATH, file_name)

        try:
            request = urllib.request.Request(image_url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(request) as response:
                file_data = response.read()
                with open(file_path, "wb") as file:
                    file.write(file_data)
                image_db_path = os.path.join(RESULT_DB_PATH, file_name)
                with open(image_db_path, "wb") as image_file:
                    image_file.write(file_data)
        except Exception as e:
            print(f"Error: {e}")

        img = PIL.Image.open(file_path)
        response = model.generate_content(
            [generate_instruction(doc_type), img]
        )
        response.resolve()

        result1 = response.text
        result1 = re.search(r"{.*}", result1, re.DOTALL).group(0)
        extracted_json = json.loads(result1)
        
        with open(LLM_OUTPUT_FILE, "w") as file:
            json.dump(extracted_json, file)
        print(f"Data saved to {LLM_OUTPUT_FILE}")

        process_extracted_data()
        
    except Exception as e:
        print(f"Error processing invoice details: {e}")

def generate_instruction(doc_type):
    if doc_type == "receipt":
        return """
            Instruction:
            **Extract the following from the Receipt data and format it in the provided JSON structure**
            ** all value in json data should be in String format("") and if don't find any value then put there empty string "" don't put null or none**
            JSON format :
                      {
                        "InvoiceDetails": {"ReceiptNumber": "", "ReceiptDateTime": ""},
                        "SellerDetails": {"SellerName": "", "SellerAddress": "", "SellerTaxNumber": "", "SellerContactNumber": "", "SellerEmail": ""},
                        "BuyerDetails": {"ClientName": ""},
                        "ItemDetails": [{"Description": "", "Qty": "", "NetPrice": "", "Total_for_each_item": ""}] #list all items inside this respectively,
                        "BankDetails": {"IBAN":"","AccountNumber":"","IFSC":"","PaymentMethod":""},
                        "InvoiceSummary": {"TotalTaxableAmount": "", "TotalTaxesAmount": "","SGST": "", "CGST": "", "IGST": "", "FinalTotal": ""}
                      }
          this is Receipt image"""
    elif doc_type == "invoice":
        return """
            Instruction:
            **Extract the following from the Invoice data and format it in the provided JSON structure**
            ** all value in json data should be in String format("") and if don't find any value then put there empty string "" don't put null or none**
            ** GSTIN or UIN numbers are related to tax numbers**
            JSON format :
                      {
                        "InvoiceDetails": {"InvoiceNumber": "", "InvoiceDate": "", "InvoiceOrderDate": "", "InvoicePurchaseOrderNumber": "", "CurrencyCode": ""},
                        "SellerDetails": {"SellerName": "", "SellerAddress": "", "SellerTaxNumber": "", "SellerContactNumber": "", "SellerEmail": "", "SellerBusinessNumber": "", "SellerGSTNumber": "", "SellerVat": "", "SellerWebsite": ""},
                        "BuyerDetails": {"ClientName": "", "ClientAddress": "", "ClientTaxNumber": "", "ClientContactNumber": "", "ClientEmail": "", "ClientNumber": "", "ClientCompanyName": "", "ClientBillingAddress": "", "ClientDeliveryAddress": ""},
                        "ItemDetails": [{"Description": "","HSN/SAC":"", "Qty": "", "UnitOfMeasure": "", "NetPrice": "", "TaxPercentage": "", "SGSTPercent": "", "SGSTAmount": "", "CGSTPercent": "", "CGSTAmount": "", "IGSTPercent": "", "IGSTAmount": "", "Total_for_each_item": ""}] #list all items inside this respectively,
                        "BankDetails": {"IBAN":"","AccountNumber":"","IFSC":"","PaymentMethod":""},
                        "InvoiceSummary": {"TotalTaxableAmount": "", "TotalTaxesAmount": "","SGSTAmount": "", "CGSTAmount": "", "IGSTAmount": "", "VATAmount": "", "FinalTotal": "", "PaymentDateDue": ""}
                      }
          this is Invoice image"""
    return ""

def process_extracted_data():
    with open(LLM_OUTPUT_FILE, "r") as file:
        input_json = json.load(file)
    remove_commas_and_periods(input_json)
    remove_leading_space(input_json)
    print(json.dumps(input_json, indent=2))
    data = format_data(input_json)
    modify_item_details(data)
    data = add_serial_numbers_to_list(data)
    print(json.dumps(data, indent=2))
    # You can add more processing steps here if needed