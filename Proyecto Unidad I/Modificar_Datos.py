import pandas as pd

def main():
    # Load
    df = pd.read_csv("./Proyecto Unidad I/covid-19.csv")

    # Convert to Json and save it as a file
    df.to_json("./Proyecto Unidad I/Datos.json", orient="records", lines=True)

    print(f"data frame: \n {df}")

if __name__ == "__main__":
    main()