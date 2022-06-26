from google.cloud import language_v1, storage
import pandas as pd


def check_address(request):
    client = language_v1.LanguageServiceClient()

    # Available types: PLAIN_TEXT, HTML
    type_ = language_v1.Document.Type.PLAIN_TEXT

    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language = "pt"

    df = pd.read_csv("addresses.csv", encoding="latin-1")
    direccion_list = []

    # iterate over rows in addresses.csv
    for i in range(0, len(df)):
        text_content = df.loc[i, "address"].encode("latin-1")
        document = {"content": text_content, "type_": type_, "language": language}

        response = client.analyze_entities(request={"document": document})

        direccion = {}
        for entity in response.entities:
            if language_v1.Entity.Type(entity.type_).name == "ADDRESS":
                for l in entity.metadata.items():
                    direccion[l[0]] = l[1]

        direccion_list.append(direccion)

    # direction_list format:
    # [
    # {street_name: Nova York, street_number: 499, sublocality: Brooklyn},
    # {street_name: Brasil, street_number: 500}
    # ]

    final = pd.DataFrame(direccion_list)
    final["address"] = df["address"]

    final.to_csv("/tmp/ml_directions.csv", index=False, sep=";", encoding="latin-1")

    client = storage.Client()
    bucket = client.get_bucket("addresses-decoupling")

    blob = bucket.blob("ml_directions.csv")
    blob.upload_from_filename("/tmp/ml_directions.csv")

    return "Done"
