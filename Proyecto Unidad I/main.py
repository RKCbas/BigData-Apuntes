from ElasticSearchProvider import ElasticSearchProvider
import json, time, pandas as pd

def main():
    try:
        # Create an instance of the ElasticSearchProvider class
        # and establish a connection with the ElasticSearch server
        es_handler = ElasticSearchProvider(index="covid-19")
        print("es_handler: ", es_handler, "\n")

        RESPONSE_LITERAL = "response: "
        load_data = False
        
        with ElasticSearchProvider(index="covid-19") as es:

            if load_data:
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

            size = 32
            response = es.search_document(query={
                "size": 0,
                "aggs": {
                    "entidades": {
                        "terms": {
                            "field": "ENTIDAD_UM.NOMBRE",
                            "size": size
                        },
                        "aggs": {
                            "nombre_entidad": {
                                "top_hits": {
                                    "size": 1,
                                    "_source": {
                                        "includes": ["ENTIDAD_UM.ID"]
                                    }
                                }
                            }
                        }
                    }
                }
            })

            # Process the response to extract data for the DataFrame
            if hasattr(response, 'body'):
                response_data = response.body
            else:
                response_data = response
            
            # Extract buckets from the aggregation
            buckets = response_data.get("aggregations", {}).get("entidades", {}).get("buckets", [])
            data = []
            for bucket in buckets:
                entidad_nombre = bucket.get("key")
                entidad_id = bucket.get("nombre_entidad", {}).get("hits", {}).get("hits", [{}])[0].get("_source", {}).get("ENTIDAD_UM.ID")
                data.append({"Entidad Nombre": entidad_nombre, "Entidad ID": entidad_id})

            # Create a pandas DataFrame
            df = pd.DataFrame(data)
            print("DataFrame created from search_document response:")
            print(df)

            print(f"{RESPONSE_LITERAL} {json.dumps(response_data, indent=4)}\n")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
