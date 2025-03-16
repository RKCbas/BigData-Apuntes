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
            mapping_data = pd.read_json("./Proyecto Unidad I/Covid-19_mapping.json")
            mapping = mapping_data.to_dict()

            response = es.create_index(mapping=mapping)
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body if hasattr(response, 'body') else response, indent=4)}\n")

            response = es.get_mapping()
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body if hasattr(response, 'body') else response, indent=4)}\n")

            # Load the json in an array with lines=false and ident=4
            json_file_path = "./Proyecto Unidad I/Datos.json"
            df = pd.read_json(json_file_path)
            print("Datos leídos")
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Datos actualizados")

            # Load a JSON file into the index
            print("Load Covid-19 JSON File Response:")
            response = es.load_json_file(json_file_path)
            print(f"{RESPONSE_LITERAL} {json.dumps(response, indent=4)}\n")

            time.sleep(2)

            # Show all indices
            response = es.show_all_indices()
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body if hasattr(response, 'body') else response, indent=4)}\n")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
