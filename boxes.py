import os
import shutil
from tqdm import tqdm
from pdf2image import convert_from_path
from doctr.io import DocumentFile
from doctr.models import ocr_predictor
import json
import math


class PDFConverter:
    def convert_to_jpg(self, pdf_path, jpg_path, dpi=300):
        # loop through all the pdf files in the folder ordered by name

        for pdf_file in tqdm(
            sorted(
                (f for f in os.listdir(pdf_path) if not f.startswith(".")),
                key=str.lower,
            )
        ):
            # convert the pdf file to jpg
            pages = convert_from_path(pdf_path + "/" + pdf_file, dpi)
            # save pdf as jpg image or images
            if len(pages) == 0:
                print(f"No pages read from pdf file: {pdf_file}")
            elif len(pages) == 1:
                pages[0].save(
                    jpg_path + "/" + pdf_file.replace(".pdf", "") + ".jpg", "JPEG"
                )
            else:
                # multi-page docs
                for i, page in enumerate(pages):
                    fname = pdf_file.replace(".pdf", "") + f"_pg{i+1}.jpg"
                    page.save(jpg_path + "/" + fname, "JPEG")


class OCRExtractor:
    def __init__(self, det_arch, reco_arch, pretrained, detect_language):
        self.model = ocr_predictor(
            det_arch, reco_arch, pretrained=pretrained, detect_language=detect_language
        )

    def extract(self, file_path, show_prediction=False):
        data_path = file_path + "/image/"
        ocr_path = file_path + "/ocrs/"

        for data_file in tqdm(
            sorted(
                (f for f in os.listdir(data_path) if not f.startswith(".")),
                key=str.lower,
            )
        ):
            doc = DocumentFile.from_images(data_path + data_file)
            predictions = self.model(doc)

            result = predictions.export()
            # write the result to a json file
            with open(
                ocr_path
                + data_file.replace(".jpg", "").replace(".png", "").replace(".jpeg", "")
                + ".json",
                "w",
            ) as f:
                json.dump(result, f, indent=4)

            if show_prediction:
                predictions.show(doc)
                break


class DataConverter:
    def convert_to_sparrow_format(self, data_path, output_path):
        file_id = 0
        for ocr_file in sorted(
            (f for f in os.listdir(data_path) if not f.startswith(".")), key=str.lower
        ):
            output_file = output_path + "/" + ocr_file
            # convert the ocr file to sparrow format
            with open(data_path + "/" + ocr_file, "r") as f:
                ocr_data = json.load(f)
                page = ocr_data["pages"][0]
                dimensions = page["dimensions"]

                annotations_json = {
                    "meta": {
                        "version": "v0.1",
                        "split": "-",
                        "image_id": file_id,
                        "image_size": {"width": dimensions[1], "height": dimensions[0]},
                    },
                    "words": [],
                }

                for block in page["blocks"]:
                    for line in block["lines"]:
                        for word in line["words"]:

                            len_x = dimensions[1]
                            len_y = dimensions[0]
                            (x1, y1) = word["geometry"][0]
                            (x2, y2) = word["geometry"][1]
                            x1 = math.floor(x1 * len_x)
                            y1 = math.floor(y1 * len_y)
                            x2 = math.ceil(x2 * len_x)
                            y2 = math.ceil(y2 * len_y)

                            word_data = {
                                "value": word["value"],
                                "label": "",
                                "rect": {"x1": x1, "y1": y1, "x2": x2, "y2": y2},
                            }
                            annotations_json["words"].append(word_data)

                with open(output_file, "w") as f:
                    json.dump(annotations_json, f, indent=2)

            file_id += 1
