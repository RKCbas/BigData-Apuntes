import pandas as pd
import os

ENTIDADES_DICT = {
    1: "AGUASCALIENTES",
    2: "BAJA CALIFORNIA",
    3: "BAJA CALIFORNIA SUR",
    4: "CAMPECHE",
    5: "COAHUILA DE ZARAGOZA",
    6: "COLIMA",
    7: "CHIAPAS",
    8: "CHIHUAHUA",
    9: "CIUDAD DE MÉXICO",
    10: "DURANGO",
    11: "GUANAJUATO",
    12: "GUERRERO",
    13: "HIDALGO",
    14: "JALISCO",
    15: "MÉXICO",
    16: "MICHOACÁN DE OCAMPO",
    17: "MORELOS",
    18: "NAYARIT",
    19: "NUEVO LEÓN",
    20: "OAXACA",
    21: "PUEBLA",
    22: "QUERÉTARO",
    23: "QUINTANA ROO",
    24: "SAN LUIS POTOSÍ",
    25: "SINALOA",
    26: "SONORA",
    27: "TABASCO",
    28: "TAMAULIPAS",
    29: "TLAXCALA",
    30: "VERACRUZ DE IGNACIO DE LA LLAVE",
    31: "YUCATÁN",
    32: "ZACATECAS",
    36: "ESTADOS UNIDOS MEXICANOS",
    97: "NO APLICA",
    98: "SE IGNORA",
    99: "NO ESPECIFICADO"
}

def obtener_nombre_entidad(entidad_id) -> str:
    """
    Devuelve el nombre de la entidad correspondiente al ID proporcionado.
    Si el ID no está en el diccionario, devuelve el ID original.
    """
    try:
        return ENTIDADES_DICT.get(entidad_id, entidad_id)
    except Exception as e:
        print(f"Error al obtener el nombre de la entidad para ID {entidad_id}: {e}")
        return entidad_id

def main():
    
    json_file_path = "./Proyecto Unidad I/Datos.json"
    csv_file_path = "./Proyecto Unidad I/COVID19MEXICO2020.csv"
    excel_catalog_path = "./Proyecto Unidad I/Catálogos.xlsx"

    try:

        # Check if the JSON file already exists
        if not os.path.exists(json_file_path):

            # Cargar los catálogos de entidades y municipios desde el archivo Excel
            entidades_df = pd.read_excel(excel_catalog_path, sheet_name="Catálogo de ENTIDADES")
            municipios_df = pd.read_excel(excel_catalog_path, sheet_name="Catálogo MUNICIPIOS")

            # Renombrar columnas para facilitar el merge
            entidades_df.rename(columns={"CLAVE_ENTIDAD": "ENTIDAD_RES", "ENTIDAD_FEDERATIVA": "NOMBRE_ENTIDAD"}, inplace=True)
            municipios_df.rename(columns={"CLAVE_ENTIDAD": "ENTIDAD_RES", "CLAVE_MUNICIPIO": "MUNICIPIO_RES", "MUNICIPIO": "NOMBRE_MUNICIPIO"}, inplace=True)

            # delete ABREVIATURA column from entidades_df
            entidades_df.drop(columns=["ABREVIATURA"], inplace=True)

            # Definir el mapeo de tipos de datos para las columnas
            dtype_mapping = {
                "PAIS_NACIONALIDAD": "str",  # Columna 38
                "PAIS_ORIGEN": "str",       # Columna 39
            }

            # Load the CSV file into a DataFrame
            df = pd.read_csv(csv_file_path, dtype=dtype_mapping)

            # Hacer merge para agregar los nombres de entidad y municipio
            df = df.merge(entidades_df, on="ENTIDAD_RES", how="left")
            df = df.merge(municipios_df, on=["ENTIDAD_RES", "MUNICIPIO_RES"], how="left")

            # Crear el campo RES como un diccionario
            df["RES"] = df.apply(
                lambda row: {
                    "IS_ENTIDAD": row["ENTIDAD_RES"],
                    "ENTIDAD": row["NOMBRE_ENTIDAD"],
                    "ID_MUNICIPIO": row["MUNICIPIO_RES"],
                    "MUNICIPIO": row["NOMBRE_MUNICIPIO"]
                },
                axis=1
            )

            # delete the columns "ENTIDAD_RES", "MUNICIPIO_RES", "NOMBRE_ENTIDAD", "NOMBRE_MUNICIPIO"
            df.drop(columns=["ENTIDAD_RES", "MUNICIPIO_RES", "NOMBRE_ENTIDAD", "NOMBRE_MUNICIPIO"], inplace=True)
            
            # Leave only the first 1000000 rows
            df = df.head(1000000)
            
            # Save the DataFrame to a JSON file
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Archivo JSON creado")
            
            # Load the JSON file into a DataFrame
            df = pd.read_json(json_file_path, orient="records", lines=False)

            # Change "ENTIDAD_UM", "ENTIDAD_NAC" to a dictionary of the id and the name of the entity
            if "ENTIDAD_UM" in df.columns:
                df["ENTIDAD_UM"] = df["ENTIDAD_UM"].apply(
                    lambda entidad_id: {
                        "ID": entidad_id,
                        "NOMBRE": obtener_nombre_entidad(int(entidad_id)) if pd.notnull(entidad_id) else None
                    }
                )

            if "ENTIDAD_NAC" in df.columns:
                df["ENTIDAD_NAC"] = df["ENTIDAD_NAC"].apply(
                    lambda entidad_id: {
                        "ID": entidad_id,
                        "NOMBRE": obtener_nombre_entidad(int(entidad_id)) if pd.notnull(entidad_id) else None
                    }
                )

            # Guardar el DataFrame modificado en el archivo JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Archivo JSON actualizado con los cambios en 'ENTIDAD_UM' y 'ENTIDAD_NAC'")
        
        # Load the JSON file into a DataFrame
        df = pd.read_json(json_file_path, orient="records", lines=False)

        # Check and convert 'PAIS_NACIONALIDAD' column values
        if "97" in df["PAIS_ORIGEN"].values:
            df["PAIS_ORIGEN"] = 'No aplica'
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'PAIS_ORIGEN' actualizados")

        # List of date columns
        date_columns = [
            'FECHA_ACTUALIZACION',
            'FECHA_INGRESO',
            'FECHA_SINTOMAS',
            'FECHA_DEF'
        ]

        # Check and convert date format for each date column
        for date_column in date_columns:
            if date_column in df.columns:
                # Replace invalid dates with empty strings
                if '9999-99-99' in df[date_column].values:
                    df[date_column] = df[date_column].replace('9999-99-99', '')

                if not df[date_column].str.match(r'\d{2}-\d{2}-\d{4}').all():
                    # Convert the date format
                    df[date_column] = pd.to_datetime(df[date_column], errors='coerce').dt.strftime('%m-%d-%Y')

                    # Save the modified DataFrame back to JSON
                    df.to_json(json_file_path, orient="records", lines=False, indent=4)
                    print(f"Formato de fecha actualizado para la columna {date_column}")
        
        
        
        # Check and convert 'RESULTADO_LAB' column values
        if df["RESULTADO_LAB"].isin([1, 2, 3, 4, 97]).any():
            df["RESULTADO_LAB"] = df["RESULTADO_LAB"].replace({
                1: 'Positivo', 
                2: 'Negativo',
                3: 'Pendiente',
                4: 'Resultado no aplicable',
                97: 'No se realizó la prueba'
                })

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'RESULTADO_LAB' actualizados")
        
        # # Check and convert 'RESULTADO_PCR' column values
        # if df["RESULTADO_PCR"].isin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 32, 33, 34, 35, 36, 37, 41, 997, 998, 999]).any():
        #     df["RESULTADO_PCR"] = df["RESULTADO_PCR"].replace({
        #         1: 'INFLUENZA AH1N1 PMD',
        #         2: 'INFLUENZA A H1',
        #         3: 'INFLUENZA A H3',
        #         4: 'INFLUENZA B',
        #         5: 'NEGATIVO',
        #         6: 'MUESTRA NO ADECUADA',
        #         7: 'ADENOVIRUS',
        #         8: 'INFLUENZA 1',
        #         9: 'INFLUENZA 2',
        #         10: 'INFLUENZA 3',
        #         11: 'VIRUS SINCICIAL RESPIRATORIO',
        #         13: 'INFLUENZA A NO SUBTIPIFICADA',
        #         14: 'INFLUENZA A H5',
        #         15: 'MUESTRA RECHAZADA',
        #         17: 'MUESTRA SIN CÉLULAS',
        #         20: 'VIRUS SINCICIAL RESPIRATORIO A',
        #         21: 'VIRUS SINCICIAL RESPIRATORIO B',
        #         22: 'CORONA 229E',
        #         23: 'CORONA OC43',
        #         24: 'CORONA SARS',
        #         25: 'CORONA NL63',
        #         26: 'CORONA HKU1',
        #         27: 'MUESTRA QUE NO AMPLIFICO',
        #         28: 'ENTEROV//RHINOVIRUS',
        #         29: 'METAPNEUMOVIRUS',
        #         30: 'MUESTRA SIN AISLAMIENTO',
        #         32: 'INFLUENZA 4',
        #         33: 'MUESTRA SIN CÉLULAS',
        #         34: 'SARS-CoV-2',
        #         35: 'MERS-CoV',
        #         36: 'SARS-CoV',
        #         37: 'BOCAVIRUS',
        #         41: 'MUESTRA NO RECIBIDA',
        #         997: 'NO APLICA (CASO SIN MUESTRA)',
        #         998: 'SIN COINFECCIÓN',
        #         999: 'PENDIENTE'
        #     })

        #     # Save the modified DataFrame back to JSON
        #     df.to_json(json_file_path, orient="records", lines=False, indent=4)
        #     print("Valores de 'RESULTADO_PCR' actualizados")

        # Check and convert 'RESULTADO_ANTIGENO' column values
        if df["RESULTADO_ANTIGENO"].isin([1, 2, 97]).any():
            df["RESULTADO_ANTIGENO"] = df["RESULTADO_ANTIGENO"].replace({
                1: 'Positivo', 
                2: 'Negativo',
                97: 'No se realizó la prueba',
                })

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'RESULTADO_ANTIGENO' actualizados")

        # Check and convert 'CLASIFICACION_FINAL' column values
        if df["CLASIFICACION_FINAL"].isin([1, 2, 3, 4, 5, 6, 7]).any():
            df["CLASIFICACION_FINAL"] = df["CLASIFICACION_FINAL"].replace({
                1: 'CASO DE COVID-19 CONFIRMADO POR ASOCIACIÓN CLÍNICA EPIDEMIOLÓGICA',
                2: 'CASO DE COVID-19 CONFIRMADO POR COMITÉ DE DICTAMINACIÓN',
                3: 'CASO DE SARS-COV-2 CONFIRMADO',
                4: 'INVÁLIDO POR LABORATORIO',
                5: 'NO REALIZADO POR LABORATORIO',
                6: 'CASO SOSPECHOSO',
                7: 'NEGATIVO A SARS-COV-2',
                })

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'CLASIFICACION_FINAL' actualizados")
            
        # Check and convert 'SEXO' column values
        if df["SEXO"].isin([1, 2, 99]).any():
            df["SEXO"] = df["SEXO"].replace({2: 'Hombre', 1: 'Mujer', 99: 'No Especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'SEXO' actualizados")
        
        # Check and convert 'TIPO_PACIENTE' column values
        if df["TIPO_PACIENTE"].isin([1, 2, 99]).any():
            df["TIPO_PACIENTE"] = df["TIPO_PACIENTE"].replace({1: 'Ambulatorio', 2: 'Hospitalizado', 99: 'No Especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'TIPO_PACIENTE' actualizados")
        
        # Check and convert 'INTUBADO' column values
        if df["INTUBADO"].isin([1, 2, 97, 98, 99]).any():
            df["INTUBADO"] = df["INTUBADO"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'INTUBADO' actualizados")
        
        # Check and convert 'NEUMONIA' column values
        if df["NEUMONIA"].isin([1, 2, 97, 98, 99]).any():
            df["NEUMONIA"] = df["NEUMONIA"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'NEUMONIA' actualizados")

        # Check and convert 'EMBARAZO' column values
        if df["EMBARAZO"].isin([1, 2, 97, 98, 99]).any():
            df["EMBARAZO"] = df["EMBARAZO"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'EMBARAZO' actualizados")

        # Check and convert 'HABLA_LENGUA_INDIG' column values
        if df["HABLA_LENGUA_INDIG"].isin([1, 2, 97, 98, 99]).any():
            df["HABLA_LENGUA_INDIG"] = df["HABLA_LENGUA_INDIG"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'HABLA_LENGUA_INDIG' actualizados")

        # Check and convert 'INDIGENA' column values
        if df["INDIGENA"].isin([1, 2, 97, 98, 99]).any():
            df["INDIGENA"] = df["INDIGENA"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'INDIGENA' actualizados")

        # Check and convert 'DIABETES' column values
        if df["DIABETES"].isin([1, 2, 97, 98, 99]).any():
            df["DIABETES"] = df["DIABETES"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'DIABETES' actualizados")

        # Check and convert 'EPOC' column values
        if df["EPOC"].isin([1, 2, 97, 98, 99]).any():
            df["EPOC"] = df["EPOC"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'EPOC' actualizados")

        # Check and convert 'ASMA' column values
        if df["ASMA"].isin([1, 2, 97, 98, 99]).any():
            df["ASMA"] = df["ASMA"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'ASMA' actualizados")

        # Check and convert 'INMUSUPR' column values
        if df["INMUSUPR"].isin([1, 2, 97, 98, 99]).any():
            df["INMUSUPR"] = df["INMUSUPR"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'INMUSUPR' actualizados")
        
        # Check and convert 'HIPERTENSION' column values
        if df["HIPERTENSION"].isin([1, 2, 97, 98, 99]).any():
            df["HIPERTENSION"] = df["HIPERTENSION"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'HIPERTENSION' actualizados")
        
        # Check and convert 'OTRA_COM' column values
        if df["OTRA_COM"].isin([1, 2, 97, 98, 99]).any():
            df["OTRA_COM"] = df["OTRA_COM"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'OTRA_COM' actualizados")
        
        # Check and convert 'CARDIOVASCULAR' column values
        if df["CARDIOVASCULAR"].isin([1, 2, 97, 98, 99]).any():
            df["CARDIOVASCULAR"] = df["CARDIOVASCULAR"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'CARDIOVASCULAR' actualizados")
        
        # Check and convert 'OBESIDAD' column values
        if df["OBESIDAD"].isin([1, 2, 97, 98, 99]).any():
            df["OBESIDAD"] = df["OBESIDAD"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'OBESIDAD' actualizados")
        
        # Check and convert 'RENAL_CRONICA' column values
        if df["RENAL_CRONICA"].isin([1, 2, 97, 98, 99]).any():
            df["RENAL_CRONICA"] = df["RENAL_CRONICA"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'RENAL_CRONICA' actualizados")
        
        # Check and convert 'TABAQUISMO' column values
        if df["TABAQUISMO"].isin([1, 2, 97, 98, 99]).any():
            df["TABAQUISMO"] = df["TABAQUISMO"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'TABAQUISMO' actualizados")
        
        # Check and convert 'OTRO_CASO' column values
        if df["OTRO_CASO"].isin([1, 2, 97, 98, 99]).any():
            df["OTRO_CASO"] = df["OTRO_CASO"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'OTRO_CASO' actualizados")
        
        # Check and convert 'TOMA_MUESTRA_LAB' column values
        if df["TOMA_MUESTRA_LAB"].isin([1, 2, 97, 98, 99]).any():
            df["TOMA_MUESTRA_LAB"] = df["TOMA_MUESTRA_LAB"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'TOMA_MUESTRA_LAB' actualizados")

        # Check and convert 'TOMA_MUESTRA_ANTIGENO' column values
        if df["TOMA_MUESTRA_ANTIGENO"].isin([1, 2, 97, 98, 99]).any():
            df["TOMA_MUESTRA_ANTIGENO"] = df["TOMA_MUESTRA_ANTIGENO"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'TOMA_MUESTRA_ANTIGENO' actualizados")

        # Check and convert 'MIGRANTE' column values
        if df["MIGRANTE"].isin([1, 2, 97, 98, 99]).any():
            df["MIGRANTE"] = df["MIGRANTE"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'MIGRANTE' actualizados")
        
        # Check and convert 'UCI' column values
        if df["UCI"].isin([1, 2, 97, 98, 99]).any():
            df["UCI"] = df["UCI"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'UCI' actualizados")
        
        # Check and convert 'ORIGEN' column values
        if df["ORIGEN"].isin([1, 2, 99]).any():
            df["ORIGEN"] = df["ORIGEN"].replace({1: 'USMER', 2: 'Fuera de USMER', 99: 'No Especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'ORIGEN' actualizados")
        
        # Check and convert 'SECTOR' column values
        if df["SECTOR"].isin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 99]).any():
            df["SECTOR"] = df["SECTOR"].replace({
                1: 'CRUZ ROJA',
                2: 'DIF',
                3: 'ESTATAL',
                4: 'IMSS',
                5: 'IMSS-BIENESTAR',
                6: 'ISSSTE',
                7: 'MUNICIPAL',
                8: 'PEMEX',
                9: 'PRIVADA',
                10: 'SEDENA',
                11: 'SEMAR',
                12: 'SSA',
                13: 'UNIVERSITARIO',
                14: 'CIJ',
                15: 'IMSS Bienestar OPD',
                99: 'No Especificado'
            })

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'SECTOR' actualizados")
        
        # Check and convert 'Nacionalidad' column values
        if df["NACIONALIDAD"].isin([1, 2, 99]).any():
            df["NACIONALIDAD"] = df["NACIONALIDAD"].replace({1: 'Mexicana', 2: 'Extranjera', 99: 'No Especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=False, indent=4)
            print("Valores de 'NACIONALIDAD' actualizados")
        
            
    except Exception as e:
        print(f"An error occurred: {e}")
        

if __name__ == "__main__":
    main()




