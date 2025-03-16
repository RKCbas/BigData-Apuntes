import pandas as pd
import os, json
from estados import municipalities_per_state

def main():
    
    json_file_path = "./Proyecto Unidad I/Datos.json"
    csv_file_path = "./Proyecto Unidad I/covid-19.csv"

    try:

        # Check if the JSON file already exists
        if not os.path.exists(json_file_path):
            # Load
            df = pd.read_csv(csv_file_path)

            # Convert to Json and save it as a file
            df.to_json(json_file_path, orient="records", lines=True)
        
        # Load the Json file
        df = pd.read_json(json_file_path, orient="records", lines=True)

        # Leave only 1 ID
        if df.columns[0] != 'ID_REGISTRO':
            # Drop the 'id' column
            df = df.drop(columns=['id'])
            
            # Reorder columns to move 'ID_REGISTRO' to the beginning
            columns = ['ID_REGISTRO'] + [col for col in df.columns if col != 'ID_REGISTRO']
            df = df[columns]
            
            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Identificador de registro actualizado")

        if 'PAIS_ORIGEN' in df.columns:
            # Drop the 'PAIS_ORIGEN' column
            df = df.drop(columns=['PAIS_ORIGEN'])
            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Identificador de registro actualizado")

        # List of date columns
        date_columns = [
            'FECHA_ARCHIVO',
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
                    df.to_json(json_file_path, orient="records", lines=True)
                    print(f"Formato de fecha actualizado para la columna {date_column}")
        
        # Dictionary for correcting country names
        corrections = {
                '99': 'Sin Identificar',
                'M\u00e9xico': 'México',
                'Estados Unidos de AmÃ©rica': 'Estados Unidos de América',
                'EspaÃ±a': 'España',
                'RepÃºblica de Honduras': 'República de Honduras',
                'Gran BretaÃ±a (Reino Unido)': 'Gran Bretaña (Reino Unido)',
                'CanadÃ¡': 'Canadá',
                'Principado de MÃ³naco': 'Principado de Mónaco',
                'RepÃºblica de Costa Rica': 'República de Costa Rica',
                'RepÃºblica Oriental del Uruguay': 'República Oriental del Uruguay',
                'RepÃºblica de PanamÃ¡': 'República de Panamá',
                'RepÃºblica de Mauricio': 'República de Mauricio',
                'RepÃºblica de Corea': 'República de Corea',
                'RepÃºblica de Angola': 'República de Angola',
                'RepÃºblica Checa y RepÃºblica Eslovaca': 'República Checa y República Eslovaca',
                'IrÃ¡n': 'Irán',
                'HaitÃ­': 'Haití',
                'Costa de Marfil': 'Costa de Marfil',
                'JapÃ³n': 'Japón',
                'HungrÃ\xada': 'Hungría',
                'PerÃº': 'Perú',
                'Ucrania': 'Ucrania',
                'Polonia': 'Polonia',
                'República Dominicana': 'República Dominicana',
                'Suiza': 'Suiza',
                'India': 'India',
                'Guatemala': 'Guatemala',
                'Principado de Mónaco': 'Principado de Mónaco',
                'Zona Neutral': 'Zona Neutral',
                'Brasil': 'Brasil',
                'Micronesia': 'Micronesia',
                'Commonwealth de Dominica': 'Commonwealth de Dominica',
                'Australia': 'Australia',
                'Nicaragua': 'Nicaragua',
                'Noruega': 'Noruega',
                'Rumania': 'Rumania',
                'Chile': 'Chile',
                'Rusia': 'Rusia',
                'Malasia': 'Malasia',
                'Israel': 'Israel',
                'Trieste': 'Trieste',
                'Holanda': 'Holanda',
                'AscensiÃ³n': 'Ascensión',
                'Egipto': 'Egipto',
                'Suecia': 'Suecia',
                'Zimbabwe': 'Zimbabwe',
                'Grecia': 'Grecia',
                'ArchipiÃ©lago de Svalbard': 'Archipiélago de Svalbard',
                'Argelia': 'Argelia',
                'Belice': 'Belice',
                'Austria': 'Austria',
                'TurquÃ\xada': 'Turquía',
                'PakistÃ¡n': 'Pakistán',
                'CamerÃºn': 'Camerún',
                'Eritrea': 'Eritrea',
                'Islandia': 'Islandia',
                'Paraguay': 'Paraguay',
                'Finlandia': 'Finlandia',
                'Letonia': 'Letonia',
                'Bangladesh': 'Bangladesh',
                'Eslovenia': 'Eslovenia',
                'Filipinas': 'Filipinas'
            }


        if 'PAIS_NACIONALIDAD' in df.columns:
            
            if df['PAIS_NACIONALIDAD'].isin(corrections.keys()).any():
                df['PAIS_NACIONALIDAD'] = df['PAIS_NACIONALIDAD'].replace(corrections)

                # Check if the last 4 characters are 'xico' and replace with 'México'
                df.loc[df['PAIS_NACIONALIDAD'].str.endswith('xico'), 'PAIS_NACIONALIDAD'] = 'México'

                # Save the modified DataFrame back to JSON
                df.to_json(json_file_path, orient="records", lines=True)
                print("Valores de 'PAIS_NACIONALIDAD' corregidos")
        
        state_columns = [
            'ENTIDAD_NAC',
            'ENTIDAD_RES',
            'ENTIDAD_UM',
            'ENTIDAD_REGISTRO'
        ]

        state_codes = {
            1: 'AGUASCALIENTES',
            2: 'BAJA CALIFORNIA',
            3: 'BAJA CALIFORNIA SUR',
            4: 'CAMPECHE',
            5: 'COAHUILA DE ZARAGOZA',
            6: 'COLIMA',
            7: 'CHIAPAS',
            8: 'CHIHUAHUA',
            9: 'CIUDAD DE MEXICO',
            10: 'DURANGO',
            11: 'GUANAJUATO',
            12: 'GUERRERO',
            13: 'HIDALGO',
            14: 'JALISCO',
            15: 'MEXICO',
            16: 'MICHOACAN DE OCAMPO',
            17: 'MORELOS',
            18: 'NAYARIT',
            19: 'NUEVO LEON',
            20: 'OAXACA',
            21: 'PUEBLA',
            22: 'QUERETARO',
            23: 'QUINTANA ROO',
            24: 'SAN LUIS POTOSI',
            25: 'SINALOA',
            26: 'SONORA',
            27: 'TABASCO',
            28: 'TAMAULIPAS',
            29: 'TLAXCALA',
            30: 'VERACRUZ DE IGNACIO DE LA LLAVE',
            31: 'YUCATAN',
            32: 'ZACATECAS',
            97: 'NO APLICA',
            98: 'SE IGNORA',
            99: 'NO ESPECIFICADO'
        }

        error_codes = {
            97: 'NO APLICA',
            98: 'SE IGNORA',
            99: 'NO ESPECIFICADO'
        }

        for state in state_columns:
            if df[state].isin(state_codes.keys()).any():
                df[state] = df[state].replace(state_codes)

                # Save the modified DataFrame back to JSON
                df.to_json(json_file_path, orient="records", lines=True)
                print(f"Formato de estado actualizado para la columna {state}")
        
        # Update 'MUNICIPIO_RES' based on 'ENTIDAD_RES'
        for state, municipalities in municipalities_per_state.items():
            # Convert dictionary keys to float
            municipalities_float = {float(k): v for k, v in municipalities.items()}
            
            if df['ENTIDAD_NAC'].isin([state]).any():
                df.loc[df['ENTIDAD_NAC'] == state, 'MUNICIPIO_RES'] = df['MUNICIPIO_RES'].replace(municipalities_float)

                if df['MUNICIPIO_RES'].isin(error_codes.keys()).any():
                    df['MUNICIPIO_RES'] = df['MUNICIPIO_RES'].replace(error_codes)

                # Save the modified DataFrame back to JSON
                df.to_json(json_file_path, orient="records", lines=True)
                print(f"Valores de 'MUNICIPIO_RES' actualizados para el estado {state}")

        # Check and convert 'RESULTADO' column values
        if df["RESULTADO"].isin([1, 2]).any():
            df["RESULTADO"] = df["RESULTADO"].replace({1: 'Positivo', 2: 'Negativo'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'RESULTADO' actualizados")

        # Check and convert 'SEXO' column values
        if df["SEXO"].isin([1, 2]).any():
            df["SEXO"] = df["SEXO"].replace({2: 'Hombre', 1: 'Mujer'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'SEXO' actualizados")
        
        # Check and convert 'TIPO_PACIENTE' column values
        if df["TIPO_PACIENTE"].isin([1, 2]).any():
            df["TIPO_PACIENTE"] = df["TIPO_PACIENTE"].replace({1: 'Ambulatorio', 2: 'Hospitalizado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'TIPO_PACIENTE' actualizados")
        
        # Check and convert 'INTUBADO' column values
        if df["INTUBADO"].isin([1, 2, 97, 98, 99]).any():
            df["INTUBADO"] = df["INTUBADO"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'INTUBADO' actualizados")
        
        # Check and convert 'NEUMONIA' column values
        if df["NEUMONIA"].isin([1, 2, 97, 98, 99]).any():
            df["NEUMONIA"] = df["NEUMONIA"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'NEUMONIA' actualizados")

        # Check and convert 'EMBARAZO' column values
        if df["EMBARAZO"].isin([1, 2, 97, 98, 99]).any():
            df["EMBARAZO"] = df["EMBARAZO"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'EMBARAZO' actualizados")

        # Check and convert 'HABLA_LENGUA_INDIG' column values
        if df["HABLA_LENGUA_INDIG"].isin([1, 2, 97, 98, 99]).any():
            df["HABLA_LENGUA_INDIG"] = df["HABLA_LENGUA_INDIG"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'HABLA_LENGUA_INDIG' actualizados")

        # Check and convert 'DIABETES' column values
        if df["DIABETES"].isin([1, 2, 97, 98, 99]).any():
            df["DIABETES"] = df["DIABETES"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'DIABETES' actualizados")

        # Check and convert 'EPOC' column values
        if df["EPOC"].isin([1, 2, 97, 98, 99]).any():
            df["EPOC"] = df["EPOC"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'EPOC' actualizados")

        # Check and convert 'ASMA' column values
        if df["ASMA"].isin([1, 2, 97, 98, 99]).any():
            df["ASMA"] = df["ASMA"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'ASMA' actualizados")

        # Check and convert 'INMUSUPR' column values
        if df["INMUSUPR"].isin([1, 2, 97, 98, 99]).any():
            df["INMUSUPR"] = df["INMUSUPR"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'INMUSUPR' actualizados")
        
        # Check and convert 'HIPERTENSION' column values
        if df["HIPERTENSION"].isin([1, 2, 97, 98, 99]).any():
            df["HIPERTENSION"] = df["HIPERTENSION"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'HIPERTENSION' actualizados")
        
        # Check and convert 'OTRA_COM' column values
        if df["OTRA_COM"].isin([1, 2, 97, 98, 99]).any():
            df["OTRA_COM"] = df["OTRA_COM"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'OTRA_COM' actualizados")
        
        # Check and convert 'CARDIOVASCULAR' column values
        if df["CARDIOVASCULAR"].isin([1, 2, 97, 98, 99]).any():
            df["CARDIOVASCULAR"] = df["CARDIOVASCULAR"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'CARDIOVASCULAR' actualizados")
        
        # Check and convert 'OBESIDAD' column values
        if df["OBESIDAD"].isin([1, 2, 97, 98, 99]).any():
            df["OBESIDAD"] = df["OBESIDAD"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'OBESIDAD' actualizados")
        
        # Check and convert 'RENAL_CRONICA' column values
        if df["RENAL_CRONICA"].isin([1, 2, 97, 98, 99]).any():
            df["RENAL_CRONICA"] = df["RENAL_CRONICA"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'RENAL_CRONICA' actualizados")
        
        # Check and convert 'TABAQUISMO' column values
        if df["TABAQUISMO"].isin([1, 2, 97, 98, 99]).any():
            df["TABAQUISMO"] = df["TABAQUISMO"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'TABAQUISMO' actualizados")
        
        # Check and convert 'OTRO_CASO' column values
        if df["OTRO_CASO"].isin([1, 2, 97, 98, 99]).any():
            df["OTRO_CASO"] = df["OTRO_CASO"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'OTRO_CASO' actualizados")
        
        # Check and convert 'MIGRANTE' column values
        if df["MIGRANTE"].isin([1, 2, 97, 98, 99]).any():
            df["MIGRANTE"] = df["MIGRANTE"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'MIGRANTE' actualizados")
        
        # Check and convert 'UCI' column values
        if df["UCI"].isin([1, 2, 97, 98, 99]).any():
            df["UCI"] = df["UCI"].replace({1: 'Sí', 2: 'No', 97: 'No aplica', 98: 'Se ignora', 99: 'No especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'UCI' actualizados")
        
        # Check and conver 'ORIGEN' column values
        if df["ORIGEN"].isin([1, 2, 99]).any():
            df["ORIGEN"] = df["ORIGEN"].replace({1: 'USMER', 2: 'Fuera de USMER', 99: 'No Especificado'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
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
                99: 'No Especificado'
            })

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'SECTOR' actualizados")
        
        # Check and convert 'Nacionalidad' column values
        if df["NACIONALIDAD"].isin([1, 2]).any():
            df["NACIONALIDAD"] = df["NACIONALIDAD"].replace({1: 'Mexicana', 2: 'Extranjera'})

            # Save the modified DataFrame back to JSON
            df.to_json(json_file_path, orient="records", lines=True)
            print("Valores de 'NACIONALIDAD' actualizados")
        
        
            
            
    except Exception as e:
        print(f"An error occurred: {e}")
        

if __name__ == "__main__":
    main()




