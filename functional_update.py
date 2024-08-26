
from collections import defaultdict
from paddleocr import PaddleOCR
import os
import shutil
import difflib


def extract_text_from_image(image_path):
    ocr = PaddleOCR(use_gpu=True)  # Enable GPU support
    results = ocr.ocr(image_path)
    ocr_text = []
    for resl in results[0]:
        line_text = resl[1][0]  # Access the text from the tuple
        ocr_text.append(line_text)
    return '\n'.join(ocr_text)

def remove_commas_and_periods(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, str):
                obj[key] = value.replace(',', '').replace('.', '').replace('$', '')
            elif isinstance(value, (list, dict)):
                remove_commas_and_periods(value)
    elif isinstance(obj, list):
        for item in obj:
            remove_commas_and_periods(item)

def merge_data(data1, data2):
    modified_data1 = data1.copy()

    def replace_values(section1, section2):
        for entry1 in section1:
            for key, value in section2.items():
                if entry1["label"] == key:
                    entry1["value"] = value

    def replace_item_values(item1, item2):
      for i, item_details in enumerate(item1):
          for j, detail in enumerate(item_details):
              for key, value in item2[i].items():
                  if detail["label"] == key:
                      detail["value"] = value


    # header = ["InvoiceDetails", "SellerDetails", "BuyerDetails", "ItemDetails", "BankDetails", "InvoiceSummary"]


    a = 0
    if a < len(data1) and "InvoiceDetails" in data1[a] and "InvoiceDetails" in data2:
         replace_values(modified_data1[a]["InvoiceDetails"], data2["InvoiceDetails"])
         a += 1
    if a < len(data1) and "SellerDetails" in data1[a] and "SellerDetails" in data2:
        replace_values(modified_data1[a]["SellerDetails"], data2["SellerDetails"])
        a += 1
    if a < len(data1) and "BuyerDetails" in data1[a] and "BuyerDetails" in data2:
        replace_values(modified_data1[a]["BuyerDetails"], data2["BuyerDetails"])
        a += 1
    if a < len(data1) and "ItemDetails" in data1[a] and "ItemDetails" in data2:
        replace_item_values(modified_data1[a]["ItemDetails"], data2["ItemDetails"])
        a += 1
    if a < len(data1) and "BankDetails" in data1[a] and "BankDetails" in data2:
        replace_values(modified_data1[a]["BankDetails"], data2["BankDetails"])
        a += 1
    if a < len(data1) and "InvoiceSummary" in data1[a] and "InvoiceSummary" in data2:
        replace_values(modified_data1[a]["InvoiceSummary"], data2["InvoiceSummary"])

    return modified_data1


def format_data(data):
    formatted_data = defaultdict(list)

    for item in data:
        label = item['label']
        if label in ['InvoiceNumber', 'InvoiceDate', 'InvoiceOrderDate', 'InvoicePurchaseOrderNumber', 'CurrencyCode']:
            formatted_data['InvoiceDetails'].append(item)
        elif label in ['SellerName', 'SellerAddress', 'SellerTaxNumber', 'SellerContactNumber', 'SellerEmail', 'SellerBusinessNumber', 'SellerGSTNumber', 'SellerVat', 'SellerWebsite']:
            formatted_data['SellerDetails'].append(item)
        elif label in ['ClientName', 'ClientAddress', 'ClientTaxNumber', 'ClientContactNumber', 'ClientEmail', 'ClientNumber', 'ClientCompanyName', 'ClientBillingAddress', 'ClientDeliveryAddress']:
            formatted_data['BuyerDetails'].append(item)
        elif label == "Description":
            formatted_data['ItemDetails'].append([item])
        elif label in ['Qty','HSN/SAC','UnitOfMeasure', 'NetPrice', 'TaxPercentage', 'SGSTPercent', 'SGSTAmount', 'CGSTPercent', 'CGSTAmount', 'IGSTPercent', 'IGSTAmount', 'Total_for_each_item']:
            formatted_data['ItemDetails'][-1].append(item)
        elif label in ['IBAN', 'AccountNumber', 'IFSC', 'PaymentMethod']:
            formatted_data['BankDetails'].append(item)
        elif label in ['TotalTaxableAmount', 'TotalTaxesAmount', 'SGSTAmount', 'CGSTAmount', 'IGSTAmount', 'VATAmount', 'FinalTotal', 'PaymentDateDue']:
            formatted_data['InvoiceSummary'].append(item)

    formatted_data = dict(formatted_data)
    final_format = [{key: value} for key, value in formatted_data.items()]

    return final_format
    
def format_data_recipt(data):
    formatted_data = defaultdict(list)

    for item in data:
        label = item['label']
        if label in ['ReceiptNumber', 'ReceiptDateTime']:
            formatted_data['InvoiceDetails'].append(item)
        elif label in ['Seller', 'SellerAddress', 'SellerTaxNumber', 'SellerContactNumber', 'SellerEmail']:
            formatted_data['SellerDetails'].append(item)
        elif label in ['Buyer']:
            formatted_data['BuyerDetails'].append(item)
        elif label == "Description":
            formatted_data['ItemDetails'].append([item])
        elif label in ['Qty', 'NetPrice', 'Total_for_each_item']:
            formatted_data['ItemDetails'][-1].append(item)
        elif label in ['PaymentDetails', 'AccountNumber', 'IFSC', 'PaymentMethod']:
            formatted_data['BankDetails'].append(item)
        elif label in ['TotalTaxableAmount', 'TotalTaxesAmount', 'SGST' 'CGST', 'IGST', 'FinalTotal']:
            formatted_data['InvoiceSummary'].append(item)

    formatted_data = dict(formatted_data)
    final_format = [{key: value} for key, value in formatted_data.items()]

    return final_format

#update description and all sr. no.:
def add_serial_numbers_to_list(data, keys):
    for i, item in enumerate(data['ItemDetails'], start=1):
        for key in list(item.keys()):
            if key in keys:
                new_key = f"{key}_{i}"
                item[new_key] = item.pop(key)
    return data

#update description and all in boundingbox output
def modify_item_details(final_format):
    for section in final_format:
        if "ItemDetails" in section:
            for id, items in enumerate(section["ItemDetails"], start=1):
                for item in items:
                    for key in item:
                        if key == "label":
                            item[key] = f"{item[key]}_{id}"
                            
                            
def remove_leading_space(data):
    if isinstance(data, dict):
        for key, value in data.items():
            data[key] = remove_leading_space(value)
    elif isinstance(data, list):
        for i in range(len(data)):
            data[i] = remove_leading_space(data[i])
    elif isinstance(data, str) and data.startswith(" "):
        data = data.lstrip()

    return data


def similarity(a, b):
    return difflib.SequenceMatcher(None, a, b).ratio()

def process_data(input_json_1, input_json_2):
    output_json = []
    
    section_names = list(input_json_2.keys())

    for i in range(len(section_names)):
        if section_names[i] != 'ItemDetails':
            for key1, value in input_json_2[section_names[i]].items():
                value = value.replace("\n", " ")
                spval = value.split(' ')
                if len(spval) > 0 and spval != ['']:
                    new_lst = []
                    indices_to_remove = []
                    similarity_threshold = 1.0  # Start with 100% match requirement
                    while similarity_threshold >= 0.8:  # Adjust the lower limit as needed
                        for item in range(len(spval)):
                            for key in range(len(input_json_1)):
                                if similarity(spval[item], input_json_1[key]['value']) >= similarity_threshold:
                                    i = item
                                    j = key
                                    output = []
                                    for k in range(len(spval) - item):
                                        if similarity(spval[i], input_json_1[j]['value']) >= similarity_threshold:
                                            output.append({
                                                "value": input_json_1[j]['value'],
                                                "label": "",
                                                "rect": input_json_1[j]["rect"]
                                            })
                                            i = i + 1
                                            j = j + 1
                                    if len(output) > len(new_lst):
                                        new_lst = output
                                        indices_to_remove.append(key)  # Store index for removal
                        if len(new_lst) > 0:
                            break  # If a match is found, break out of the loop
                        similarity_threshold -= 0.01  # Gradually reduce similarity threshold

                    if len(new_lst) > 0:
                        combined_rect = {
                            "x1": min(item["rect"]["x1"] for item in new_lst),
                            "y1": min(item["rect"]["y1"] for item in new_lst),
                            "x2": max(item["rect"]["x2"] for item in new_lst),
                            "y2": max(item["rect"]["y2"] for item in new_lst),
                        }
                        combined_value = " ".join(item["value"] for item in new_lst)
                        output1 = {
                            "rect": combined_rect,
                            "value": combined_value,
                            "label": key1
                        }
                        output_json.append(output1)

                    # Remove used objects from input_json_1
                    for idx in sorted(indices_to_remove, reverse=True):
                        del input_json_1[idx]

        else:
            for item2 in input_json_2[section_names[i]]:
                for key1, value in item2.items():
                    value = value.replace("\n", " ")
                    spval = value.split(' ')
                    if spval:
                        new_lst = []
                        indices_to_remove = []
                        similarity_threshold = 1.0  # Start with 100% match requirement
                        while similarity_threshold >= 0.8:  # Adjust the lower limit as needed
                            for item in range(len(spval)):
                                for key in range(len(input_json_1)):
                                    if similarity(spval[item], input_json_1[key]['value']) >= similarity_threshold:
                                        i = item
                                        j = key
                                        output = []
                                        for k in range(len(spval) - item):
                                            if similarity(spval[i], input_json_1[j]['value']) >= similarity_threshold:
                                                output.append({
                                                    "value": input_json_1[j]['value'],
                                                    "label": "",
                                                    "rect": input_json_1[j]["rect"]
                                                })
                                                i = i + 1
                                                j = j + 1
                                        if len(output) > len(new_lst):
                                            new_lst = output
                                            indices_to_remove.append(key)  # Store index for removal
                            if len(new_lst) > 0:
                                break  # If a match is found, break out of the loop
                            similarity_threshold -= 0.01  # Gradually reduce similarity threshold

                        if len(new_lst) > 0:
                            combined_rect = {
                                "x1": min(item["rect"]["x1"] for item in new_lst),
                                "y1": min(item["rect"]["y1"] for item in new_lst),
                                "x2": max(item["rect"]["x2"] for item in new_lst),
                                "y2": max(item["rect"]["y2"] for item in new_lst),
                            }
                            combined_value = " ".join(item["value"] for item in new_lst)
                            output1 = {
                                "rect": combined_rect,
                                "value": combined_value,
                                "label": key1
                            }
                            output_json.append(output1)

                        # Remove used objects from input_json_1
                        for idx in sorted(indices_to_remove, reverse=True):
                            del input_json_1[idx]

    return output_json
    
def empty_folder(folder_path):
  for filename in os.listdir(folder_path):
      file_path = os.path.join(folder_path, filename)
      try:
          if os.path.isfile(file_path):
              os.unlink(file_path)
          elif os.path.isdir(file_path):
              shutil.rmtree(file_path)
      except Exception as e:
          print(f"Failed to delete {file_path}. Reason: {e}")