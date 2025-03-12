import pandas as pd

def main():
    # Load
    df = pd.read_csv("./Proyecto Unidad I/mexico_covid19_263mil.csv")

    # Convert to Json
    df.to_json("Datos.json", orient="records", lines=True)

    print(f"data frame: \n {df}")

if __name__ == "__main__":
    main()