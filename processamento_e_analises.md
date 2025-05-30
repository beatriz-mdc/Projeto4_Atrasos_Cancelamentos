1. Processar e preparar a base de dados

Os dados foram disponibilizados pela Laboratoria em pasta zipada com 3 planilhas CSV nomeadas “AIRLINE_CODE_DICTIONARY”, “DOT_CODE_DICTIONARY” e “flights_202301”. Esses arquivos contém informações sobre os voôs realizados no período, as companhias aéreas e os órgãos operadores das aeronaves. Ao subir os arquivos no bigquery, os mesmos foram renomeados:

"AIRLINE_CODE_DICTIONARY" - "operador_aeronave";
“DOT_CODE_DICTIONARY” - "companhia_aerea";
"flights_202301" - "tabela_voos".

Identificar e Tratar Valores Nulos

996 na "companhia_aerea" coluna code, 1000 na coluna Description.
9978 na "tabela_voos" coluna DEP_TIME, 9982 na coluna DEP_DELAY, 10197 na coluna TAXI_OUT, 10197 na coluna WHEELS_OFF, 10519 na coluna WHEELS_ON, 10519 na coluna TAXI_IN, 10519 na coluna ARR_TIME, 11640 na coluna ARR_DELAY, 1 na coluna CRS_ELAPSED_TIME, 11640 na coluna ELAPSED_TIME, 11640 na coluna AIR_TIME, 422124 na coluna DELAY_DUE_CARRIER, 422124 na coluna DELAY_DUE_WEATHER, 422124 na coluna DELAY_DUE_NAS, 422124 na coluna DELAY_DUE_SECURITY e 422124 na coluna DELAY_DUE_LATE_AIRCRAFT.





Foram encontrados nulos na tabela "clientes_info". Desses nulos, 7199 estavam na coluna "last_month_salary" e 943 na coluna "number_dependents". Como tratamento, os salários foram subtituídos pela mediana 5400 e os depentes por 0. Abaixo query:
