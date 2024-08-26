import difflib
import os
import shutil
from collections import defaultdict

def remove_commas_and_periods(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, str):
                obj[key] = value.replace(",", "").replace(".", "").replace("$", "")
            elif isinstance(value, (list, dict)):
                remove_commas_and_periods(value)
    elif isinstance(obj, list):
        for item in obj:
            remove_commas_and_periods(item)

def merge_data(data1, data2):
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

    modified_data1 = data1.copy()
    sections = [
        ("InvoiceDetails", replace_values),
        ("SellerDetails", replace_values),
        ("BuyerDetails", replace_values),
        ("ItemDetails", replace_item_values),
        ("BankDetails", replace_values),
        ("InvoiceSummary", replace_values),
    ]

    for a, (section, func) in enumerate(sections):
        if a < len(data1) and section in data1[a] and section in data2:
            func(modified_data1[a][section], data2[section])

    return modified_data1


def format_data(data):
    formatted_data = defaultdict(list)
    section_map = {
        "InvoiceDetails": [
            "InvoiceNumber",
            "InvoiceDate",
            "InvoiceOrderDate",
            "InvoicePurchaseOrderNumber",
            "CurrencyCode",
        ],
        "SellerDetails": [
            "SellerName",
            "SellerAddress",
            "SellerTaxNumber",
            "SellerContactNumber",
            "SellerEmail",
            "SellerBusinessNumber",
            "SellerGSTNumber",
            "SellerVat",
            "SellerWebsite",
        ],
        "BuyerDetails": [
            "ClientName",
            "ClientAddress",
            "ClientTaxNumber",
            "ClientContactNumber",
            "ClientEmail",
            "ClientNumber",
            "ClientCompanyName",
            "ClientBillingAddress",
            "ClientDeliveryAddress",
        ],
        "ItemDetails": [
            "Description",
            "Qty",
            "HSN/SAC",
            "UnitOfMeasure",
            "NetPrice",
            "TaxPercentage",
            "SGSTPercent",
            "SGSTAmount",
            "CGSTPercent",
            "CGSTAmount",
            "IGSTPercent",
            "IGSTAmount",
            "Total_for_each_item",
        ],
        "BankDetails": ["IBAN", "AccountNumber", "IFSC", "PaymentMethod"],
        "InvoiceSummary": [
            "TotalTaxableAmount",
            "TotalTaxesAmount",
            "SGSTAmount",
            "CGSTAmount",
            "IGSTAmount",
            "VATAmount",
            "FinalTotal",
            "PaymentDateDue",
        ],
    }

    for item in data:
        label = item["label"]
        for section, labels in section_map.items():
            if label in labels:
                if section == "ItemDetails" and label == "Description":
                    formatted_data[section].append([item])
                elif section == "ItemDetails":
                    formatted_data[section][-1].append(item)
                else:
                    formatted_data[section].append(item)

    return [{key: value} for key, value in dict(formatted_data).items()]


def format_data_receipt(data):
    formatted_data = defaultdict(list)
    section_map = {
        "InvoiceDetails": ["ReceiptNumber", "ReceiptDateTime"],
        "SellerDetails": [
            "Seller",
            "SellerAddress",
            "SellerTaxNumber",
            "SellerContactNumber",
            "SellerEmail",
        ],
        "BuyerDetails": ["Buyer"],
        "ItemDetails": ["Description", "Qty", "NetPrice", "Total_for_each_item"],
        "BankDetails": ["PaymentDetails", "AccountNumber", "IFSC", "PaymentMethod"],
        "InvoiceSummary": [
            "TotalTaxableAmount",
            "TotalTaxesAmount",
            "SGST",
            "CGST",
            "IGST",
            "FinalTotal",
        ],
    }
    for item in data:
        label = item["label"]
        for section, labels in section_map.items():
            if label in labels:
                if section == "ItemDetails" and label == "Description":
                    formatted_data[section].append([item])
                elif section == "ItemDetails":
                    formatted_data[section][-1].append(item)
                else:
                    formatted_data[section].append(item)
    return [{key: value} for key, value in dict(formatted_data).items()]


def add_serial_numbers_to_list(data, keys):
    for i, item in enumerate(data["ItemDetails"], start=1):
        for key in list(item.keys()):
            if key in keys:
                new_key = f"{key}_{i}"
                item[new_key] = item.pop(key)
    return data


def modify_item_details(final_format):
    for section in final_format:
        if "ItemDetails" in section:
            for id, items in enumerate(section["ItemDetails"], start=1):
                for item in items:
                    if "label" in item:
                        item["label"] = f"{item['label']}_{id}"


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
    for section_name in section_names:
        if section_name != "ItemDetails":
            for key1, value in input_json_2[section_name].items():
                value = value.replace("\n", " ")
                spval = value.split(" ")
                if spval:
                    new_lst = []
                    indices_to_remove = []
                    similarity_threshold = 1.0
                    while similarity_threshold >= 0.8:
                        for item in range(len(spval)):
                            for key in range(len(input_json_1)):
                                if (
                                    similarity(spval[item], input_json_1[key]["value"])
                                    >= similarity_threshold
                                ):
                                    i, j = item, key
                                    output = []
                                    for k in range(len(spval) - item):
                                        if (
                                            similarity(
                                                spval[i], input_json_1[j]["value"]
                                            )
                                            >= similarity_threshold
                                        ):
                                            output.append(
                                                {
                                                    "value": input_json_1[j]["value"],
                                                    "label": "",
                                                    "rect": input_json_1[j]["rect"],
                                                }
                                            )
                                            i += 1
                                            j += 1
                                    if len(output) > len(new_lst):
                                        new_lst = output
                                        indices_to_remove.append(key)
                        if new_lst:
                            break
                        similarity_threshold -= 0.01
                    if new_lst:
                        combined_rect = {
                            "x1": min(item["rect"]["x1"] for item in new_lst),
                            "y1": min(item["rect"]["y1"] for item in new_lst),
                            "x2": max(item["rect"]["x2"] for item in new_lst),
                            "y2": max(item["rect"]["y2"] for item in new_lst),
                        }
                        combined_value = " ".join(item["value"] for item in new_lst)
                        output_json.append(
                            {
                                "rect": combined_rect,
                                "value": combined_value,
                                "label": key1,
                            }
                        )
                    for idx in sorted(indices_to_remove, reverse=True):
                        del input_json_1[idx]
        else:
            for item2 in input_json_2[section_name]:
                for key1, value in item2.items():
                    value = value.replace("\n", " ")
                    spval = value.split(" ")
                    if spval:
                        new_lst = []
                        indices_to_remove = []
                        similarity_threshold = 1.0
                        while similarity_threshold >= 0.8:
                            for item in range(len(spval)):
                                for key in range(len(input_json_1)):
                                    if (
                                        similarity(
                                            spval[item], input_json_1[key]["value"]
                                        )
                                        >= similarity_threshold
                                    ):
                                        i, j = item, key
                                        output = []
                                        for k in range(len(spval) - item):
                                            if (
                                                similarity(
                                                    spval[i], input_json_1[j]["value"]
                                                )
                                                >= similarity_threshold
                                            ):
                                                output.append(
                                                    {
                                                        "value": input_json_1[j][
                                                            "value"
                                                        ],
                                                        "label": "",
                                                        "rect": input_json_1[j]["rect"],
                                                    }
                                                )
                                                i += 1
                                                j += 1
                                        if len(output) > len(new_lst):
                                            new_lst = output
                                            indices_to_remove.append(key)
                            if new_lst:
                                break
                            similarity_threshold -= 0.01
                        if new_lst:
                            combined_rect = {
                                "x1": min(item["rect"]["x1"] for item in new_lst),
                                "y1": min(item["rect"]["y1"] for item in new_lst),
                                "x2": max(item["rect"]["x2"] for item in new_lst),
                                "y2": max(item["rect"]["y2"] for item in new_lst),
                            }
                            combined_value = " ".join(item["value"] for item in new_lst)
                            output_json.append(
                                {
                                    "rect": combined_rect,
                                    "value": combined_value,
                                    "label": key1,
                                }
                            )
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
