import google.generativeai as genai
import socketio
from queue import Queue
import requests
import urllib
import os
import PIL.Image
import json
import re
from boxes import OCRExtractor
from boxes import DataConverter
import ast
from functional_update import remove_commas_and_periods
from functional_update import remove_leading_space
from functional_update import process_data
from functional_update import format_data_recipt
from functional_update import format_data
from functional_update import add_serial_numbers_to_list
from functional_update import modify_item_details
from functional_update import merge_data
from functional_update import empty_folder

GOOGLE_API_KEY=os.getenv("GOOGLE_API_KEY")
genai.configure(api_key=GOOGLE_API_KEY)

model = genai.GenerativeModel('models/gemini-1.5-flash-latest')

import threading

# Create a lock
lock = threading.Lock()

socket_queue = Queue()
sio = socketio.Client(logger=True)
sio.connect("https://devdtdemo-ws.aspiresoftware.in")

def process_single_message():
    global socket_queue
    with lock:
        if not socket_queue.empty():
            message = socket_queue.get()
            output_send = 'https://devdtdemo-api.aspiresoftware.in/kafka/send-output'
            result = invoice_details(message)
            print(result)
            print(type(result))
            try :
              process_id = str(result["process_id"])
              flow_id = str(result["flow_id"])
              doc_type = result["type"]
              print(process_id)
              try :
                result1 = result["result"]
              except Exception as e :
                print(e)
              # print(result1)
              if 'error' in result :
                process_id = result["process_id"]
                print(f"failed {process_id}")
                stat = 'Failed'
                processed_message = {
                  "process_id": process_id,
                  "status": stat,# Update the status to "completed"
                  "type": doc_type,
                  "flow_id": flow_id,
                  # "error_message": '',
                  "result": 'null'  # Include the processed result
                }
              else :
                stat = 'Completed'

                # Message indicating processing completion
                processed_message = {
                    "process_id": process_id,
                    "status": stat,
                    "type": doc_type,
                    "flow_id": flow_id,# Update the status to "completed"
                    "result": result1  # Include the processed result
                }
              requests.post(output_send, json = processed_message)
              print("done")
            except Exception as e :
              print(e)

def invoice_details(message):
  try:
      data = {}
      flow_id = ""
      process_id = ""
      doc_type = ""
      socket_message = message
      if socket_message == 'Message Pushed into Input queue':
        input_recive = 'https://devdtdemo-api.aspiresoftware.in/kafka/receive-input'
        output_send = 'https://devdtdemo-api.aspiresoftware.in/kafka/send-output'
        input_request = requests.get(input_recive)
        input_request_data = input_request.json()
        data = input_request_data[0]
        print(data)
        process_id = str(data.get("process_id"))
        flow_id = data.get("flow_id")
        print(flow_id)
        print(type(flow_id))
        doc_type = data.get("type")
        status = "InProgress"
        initial_message = {
            "process_id": process_id,
            "flow_id": flow_id,
            "type": doc_type,
            "status": status
        }
        requests.post(output_send, json=initial_message)
      file_name = ""
      image_url = data.get("image_url")
      print(process_id)
      print(image_url)
      new_ip = "https://devdtdemo-image.aspiresoftware.in/"

      image_url = image_url.replace("http://192.168.2.25:9014/", new_ip)
      print(image_url)

      try:
          # Create a request with User-Agent header
          request = urllib.request.Request(image_url, headers={'User-Agent': 'Mozilla/5.0'})

          # Download the file from the URL
          with urllib.request.urlopen(request) as response:
              file_data = response.read()

              # Get the file name and extension
              file_name = os.path.basename(image_url)
              _, file_extension = os.path.splitext(file_name)

              # Save the file based on its extension
              if file_extension.lower() == '.pdf':
                  file_path = os.path.join("result/pdf", file_name)
              else:
                  file_path = os.path.join("result/image", file_name)

              with open(file_path, "wb") as file:
                  file.write(file_data)

              image_db = os.path.join("result/DB", file_name)
              with open(image_db, "wb") as image_file:
                  image_file.write(file_data)

              root_name, file_extension = os.path.splitext(file_name)
              print(root_name)

      except Exception as e:
          print(f"Error: {e}")
      image_path = 'result/image/'+file_name
      print(image_path)
      img = PIL.Image.open(image_path)
      if doc_type == "receipt":
        print(doc_type)
        response = model.generate_content(['''
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
                      this is Receipt image''', img])
        response.resolve()
      elif doc_type == 'invoice' :
        print(doc_type)
        response = model.generate_content(['''
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

                                           
                      this is Invoice image''', img])
        response.resolve()
      result1 = response.text
      result1 = result1.replace("`","")
      result1 = result1.replace("json","")
      print(result1)
      json_data = re.search(r'{.*}', result1, re.DOTALL).group(0)
      try:
            extracted_json = json.loads(json_data)
            print("Parsed JSON:")
            print(json.dumps(extracted_json, indent=2))
      except json.JSONDecodeError as e:
          print("Error decoding JSON:", e)

      print("Data from llm extracted")
      extracted_json = json.dumps(extracted_json)
      file_path = "result/LLM/output.txt"
      # Open the file in write mode
      with open(file_path, "w") as file:
          # Write the extracted_data to the file
          file.write(extracted_json)
      print(f"Data saved to {file_path}")

      ocr_extractor = OCRExtractor('db_resnet50', 'crnn_vgg16_bn', pretrained=True, detect_language=True)
      ocr_extractor.extract('result', show_prediction=False)

      # Convert to sparrow format
      data_converter = DataConverter()
      data_converter.convert_to_sparrow_format('result/ocrs',
                                                'result/output')

      with open(f'result/output/{root_name}.json', 'r') as file:
          da = json.load(file)
      json_string = json.dumps(da)
      first_brace_index = json_string.find('[')
      last_brace_index = json_string.rfind(']')
      # Extract the data between the first "{" and last "}"
      extracted_data1 = json_string[first_brace_index:last_brace_index+1]
      file_path = "result/LLM/output1.txt"
      # Open the file in write mode
      with open(file_path, "w") as file:
      # Write the extracted_data to the file
          file.write(extracted_data1)
      print(f"Data saved to {file_path}")

    #---------------------------------

      with open('result/LLM/output1.txt', 'r') as file:
          input_jso_1 = file.read()
      input_json_1 = ast.literal_eval(input_jso_1)
      #remove commas and periods within the values
      remove_commas_and_periods(input_json_1)
      print(type(input_json_1))

      with open('result/LLM/output.txt', 'r') as file:
          input_json_2 = json.load(file)
      #remove commas and periods within the values
      remove_commas_and_periods(input_json_2)
      remove_leading_space(input_json_2)
      print(json.dumps(input_json_2, indent = 2))

      output_json = process_data(input_json_1, input_json_2)
      print(json.dumps(output_json, indent=2))
      result1 = output_json
      result_to_save = json.dumps(output_json)
      file_path = "result/LLM/final_output.txt"
      #open the file in write mode
      with open(file_path, "w") as file:
        file.write(result_to_save)
      print(f"Data saved to {file_path}")

      labels_to_update = ['Description', 'Qty', 'UnitOfMeasure', 'NetPrice', 'Tax', 'Total_for_each_item']

      #replace value LLM  to Doctr
      with open('result/LLM/final_output.txt', 'r') as file:
        data_1_replace = file.read()
      data_1_replace = ast.literal_eval(data_1_replace)
      if doc_type == "receipt":
        data_1_replace = format_data_recipt(data_1_replace)
      elif doc_type == 'invoice':
        data_1_replace = format_data(data_1_replace)
      print("formate done")
      modify_item_details(data_1_replace)
      print(json.dumps(data_1_replace, indent=2))
      with open('result/LLM/output.txt', 'r') as file:
        data_2_replace = json.load(file)
      data_2_replace = add_serial_numbers_to_list(data_2_replace, labels_to_update)
      print(json.dumps(data_2_replace, indent=2))
      data_1_replace = merge_data(data_1_replace, data_2_replace)
      # print(json.dumps(data_1_replace, indent=2))
      result = data_1_replace
      print(json.dumps(result, indent=2))

    # Example usage
      empty_folder("result/image")
      empty_folder("result/ocrs")
      # empty_folder("result/output")
      empty_folder("result/pdf")

      final = {"result": result, "process_id": process_id, "flow_id": flow_id, "type": doc_type }
      print('done')

  except SystemExit as system_exit:
      # Handle the SystemExit exception
      error_message = f"SystemExit exception occurred: {system_exit}"
      print(error_message)
      final = {"error" : str(error_message), "process_id": process_id, "flow_id": flow_id, "type": doc_type}


  except Exception as e:
      final = {"error" : str(e), "process_id": process_id, "flow_id": flow_id, "type": doc_type}
      print(final)

  return final


@sio.on('SendInInput')
def on_message(message):
    print(f"Received message: {message}")
    socket_queue.put(message)
    # Start a new thread to process the message
    threading.Thread(target=process_single_message).start()

sio.wait()