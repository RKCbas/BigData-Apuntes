import pandas as pd
import os

def main():
    
    json_file_path = "./Proyecto Unidad I/Datos.json"
    csv_file_path = "./Proyecto Unidad I/covid-19.csv"

    try:

        # Check if the JSON file already exists
        if not os.path.exists(json_file_path):
            # Load
            df = pd.read_csv(csv_file_path)

            # Convert to Json and save it as a file
            df.to_json("./Proyecto Unidad I/Datos.json", orient="records", lines=True)
        
        # Load the Json file
        df = pd.read_json("./Proyecto Unidad I/Datos.json", orient="records", lines=True)

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
        
        if 'PAIS_NACIONALIDAD' in df.columns:
            # Dictionary for correcting country names
            corrections = {
                    '99': 'Sin Identificar',
                    'MÃ©xico': 'México',
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

            if df['PAIS_NACIONALIDAD'].isin(corrections.keys()).any():
                df['PAIS_NACIONALIDAD'] = df['PAIS_NACIONALIDAD'].replace(corrections)

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
            '1': 'AGUASCALIENTES',
            '2': 'BAJA CALIFORNIA',
            '3': 'BAJA CALIFORNIA SUR',
            '4': 'CAMPECHE',
            '5': 'COAHUILA DE ZARAGOZA',
            '6': 'COLIMA',
            '7': 'CHIAPAS',
            '8': 'CHIHUAHUA',
            '9': 'CIUDAD DE MEXICO',
            '10': 'DURANGO',
            '11': 'GUANAJUATO',
            '12': 'GUERRERO',
            '13': 'HIDALGO',
            '14': 'JALISCO',
            '15': 'MEXICO',
            '16': 'MICHOACAN DE OCAMPO',
            '17': 'MORELOS',
            '18': 'NAYARIT',
            '19': 'NUEVO LEON',
            '20': 'OAXACA',
            '21': 'PUEBLA',
            '22': 'QUERETARO',
            '23': 'QUINTANA ROO',
            '24': 'SAN LUIS POTOSI',
            '25': 'SINALOA',
            '26': 'SONORA',
            '27': 'TABASCO',
            '28': 'TAMAULIPAS',
            '29': 'TLAXCALA',
            '30': 'VERACRUZ DE IGNACIO DE LA LLAVE',
            '31': 'YUCATAN',
            '32': 'ZACATECAS',
            '97': 'NO APLICA',
            '98': 'SE IGNORA',
            '99': 'NO ESPECIFICADO'
        }

        for state in state_columns:
            if df[state].isin(corrections.keys()).any():
                df[state] = df[state].replace(state_codes)

                # Save the modified DataFrame back to JSON
                df.to_json(json_file_path, orient="records", lines=True)
                print(f"Formato de estado actualizado para la columna {state}")

        municipalities_per_state = {
            'AGUASCALIENTES': {
                '1.0': 'AGUASCALIENTES',
                '2.0': 'ASIENTOS',
                '3.0': 'CALVILLO',
                '4.0': 'COSIO',
                '5.0': 'JESUS MARIA',
                '6.0': 'PABELLON DE ARTEAGA',
                '7.0': 'RINCON DE ROMOS',
                '8.0': 'SAN JOSE DE GRACIA',
                '9.0': 'TEPEZALA',
                '10.0': 'EL LLANO',
                '11.0': 'SAN FRANCISCO DE LOS ROMO',
                '999.0': 'NO ESPECIFICADO'
            },
            'BAJA CALIFORNIA': {
                '1.0': 'ENSENADA',
                '2.0': 'MEXICALI',
                '3.0': 'TECATE',
                '4.0': 'TIJUANA',
                '5.0': 'PLAYAS DE ROSARITO',
                '999.0': 'NO ESPECIFICADO'
            },
            'BAJA CALIFORNIA SUR': {
                '1.0': 'COMONDU',
                '2.0': 'MULEGE',
                '3.0': 'LA PAZ',
                '4.0': 'LOS CABOS',
                '5.0': 'LORETO',
                '999.0': 'NO ESPECIFICADO'
            },
            'CAMPECHE': {
                '1.0': 'CALKINI',
                '2.0': 'CAMPECHE',
                '3.0': 'CARMEN',
                '4.0': 'CHAMPOTON',
                '5.0': 'HECELCHAKAN',
                '6.0': 'HOPELCHEN',
                '7.0': 'PALIZADA',
                '8.0': 'TENABO',
                '9.0': 'ESCÁRCEGA',
                '10.0': 'CALAKMUL',
                '11.0': 'CANDELARIA',
                '999.0': 'NO ESPECIFICADO'
            },
            'COAHUILA DE ZARAGOZA': {
                '1.0': 'ABASOLO',
                '2.0': 'ACUÑA',
                '3.0': 'ALLENDE',
                '4.0': 'ARTEAGA',
                '5.0': 'CANDELA',
                '6.0': 'CASTAÑOS',
                '7.0': 'CUATRO CIÉNEGAS',
                '8.0': 'ESCOBEDO',
                '9.0': 'FRANCISCO I. MADERO',
                '10.0': 'FRONTERA',
                '11.0': 'GENERAL CEPEDA',
                '12.0': 'GUERRERO',
                '13.0': 'HIDALGO',
                '14.0': 'JIMÉNEZ',
                '15.0': 'JUÁREZ',
                '16.0': 'LAMADRID',
                '17.0': 'MATAMOROS',
                '18.0': 'MONCLOVA',
                '19.0': 'MORELOS',
                '20.0': 'MÚZQUIZ',
                '21.0': 'NADADORES',
                '22.0': 'NAVA',
                '23.0': 'OCAMPO',
                '24.0': 'PARRAS',
                '25.0': 'PIEDRAS NEGRAS',
                '26.0': 'PROGRESO',
                '27.0': 'RAMOS ARIZPE',
                '28.0': 'SABINAS',
                '29.0': 'SACRAMENTO',
                '30.0': 'SALTILLO',
                '31.0': 'SAN BUENAVENTURA',
                '32.0': 'SAN JUAN DE SABINAS',
                '33.0': 'SAN PEDRO',
                '34.0': 'SIERRA MOJADA',
                '35.0': 'TORREÓN',
                '36.0': 'VIESCA',
                '37.0': 'VILLA UNIÓN',
                '38.0': 'ZARAGOZA',
                '999.0': 'NO ESPECIFICADO'
            },
            'COLIMA': {
                '1.0': 'ARMERÍA',
                '2.0': 'COLIMA',
                '3.0': 'COMALA',
                '4.0': 'COQUIMATLÁN',
                '5.0': 'CUAUHTÉMOC',
                '6.0': 'IXTLAHUACÁN',
                '7.0': 'MANZANILLO',
                '8.0': 'MINATITLÁN',
                '9.0': 'TECOMÁN',
                '10.0': 'VILLA DE ÁLVAREZ',
                '999.0': 'NO ESPECIFICADO'
            }
        }

            
    except Exception as e:
        print(f"An error occurred: {e}")
        

if __name__ == "__main__":
    main()




