from ElasticSearchProvider import ElasticSearchProvider
import json, time

def main():
    try:
        # Create an instance of the ElasticSearchProvider class
        # and establish a connection with the ElasticSearch server
        es_handler = ElasticSearchProvider()
        print("es_handler: ", es_handler, "\n")

        RESPONSE_LITERAL = "response: "

        with ElasticSearchProvider(index="pokemon") as es:

            # Load Pokemon JSON file into the index
            print("Load Pokemon JSON File Response:")
            response = es.load_json_file("./Unidad I/pokemon.json")
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body if hasattr(response, 'body') else response, indent=4)}\n")


    except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
