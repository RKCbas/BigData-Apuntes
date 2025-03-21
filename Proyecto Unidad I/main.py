from ElasticSearchProvider import ElasticSearchProvider
import json, time, pandas as pd

def main():
    try:
        # Create an instance of the ElasticSearchProvider class
        # and establish a connection with the ElasticSearch server
        es_handler = ElasticSearchProvider(index="COVID-19")
        print("es_handler: ", es_handler, "\n")

        RESPONSE_LITERAL = "response: "
        
        with ElasticSearchProvider(index="covid-19") as es:

            response = es.delete_index()
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body if hasattr(response, 'body') else response, indent=4)}\n")

            mapping_data = pd.read_json("./Proyecto Unidad I/Covid-19_mapping.json")
            mapping = mapping_data.to_dict()

            response = es.create_index(mapping=mapping)
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body if hasattr(response, 'body') else response, indent=4)}\n")

            response = es.get_mapping()
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body if hasattr(response, 'body') else response, indent=4)}\n")

            # Load a JSON file into the index
            json_file_path = "./Proyecto Unidad I/Datos.json"

            print("Load Covid-19 JSON File Response:")
            response = es.load_json_file(json_file_path, batch_size=10000)
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body if hasattr(response, 'body') else response, indent=4)}\n")

            time.sleep(2)

            # Search for documents in the index
            print("Search Covid-19 Documents Response:")
            response = es.get_all_documents()
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body if hasattr(response, 'body') else response, indent=4)}\n")


    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
