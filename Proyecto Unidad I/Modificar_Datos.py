import pandas as pd
import os

def main():
    
    json_file_path = "./Proyecto Unidad I/Datos.json"
    csv_file_path = "./Proyecto Unidad I/covid-19.csv"

    if not os.path.exists(json_file_path):
        # Load
        df = pd.read_csv(csv_file_path)

        # Convert to Json and save it as a file
        df.to_json("./Proyecto Unidad I/Datos.json", orient="records", lines=True)
    else:
        # Load the Json file
        df = pd.read_json("./Proyecto Unidad I/Datos.json", orient="records", lines=True)

        # Check for duplicate IDs
        if df['id'].duplicated().any():
            print("There are duplicate IDs in the JSON file.")
        else:
            print("All IDs in the JSON file are unique.")

        # Check for duplicate ID_REGISTRO
        if df['ID_REGISTRO'].duplicated().any():
            print("There are duplicate ID_REGISTRO in the JSON file.")
        else:
            print("All ID_REGISTRO in the JSON file are unique.")

        print(df.head())

if __name__ == "__main__":
    main()