# Etapas do Projeto

Os dados foram disponibilizados pela Laboratoria em pasta zipada com 3 planilhas CSV nomeadas “AIRLINE_CODE_DICTIONARY”, “DOT_CODE_DICTIONARY” e “flights_202301”. Esses arquivos contém informações sobre os voôs realizados no período de janeiro de 2023, as companhias aéreas e os órgãos operadores das aeronaves. Ao subir os arquivos no bigquery, os mesmos foram renomeados:

- "AIRLINE_CODE_DICTIONARY" - "operador_aeronave";
- “DOT_CODE_DICTIONARY” - "companhia_aerea";
- "flights_202301" - "tabela_voos".

Para iniciar o projeto foram definidas "Perguntas de Negócio" divididas em 3 frentes. Abaixo os questionamentos a serem respondidos:

**Operacionais**
- Quais são as rotas com maior frequência e magnitude (tempo médio) de atrasos e cancelamentos?
- Quais são os principais motivos dos atrasos e cancelamentos registrados, e como esses motivos se distribuem pelas rotas/companhias aéreas mais problemáticas?
- Como os atrasos variam por dia da semana e período do dia, e qual o impacto do “efeito cascata” nesses períodos?
- Quais aeroportos de origem e destino apresentam os maiores tempos médios de taxiamento, indicando possíveis gargalos no solo?

**Financeiras**
- Quais rotas, períodos do dia e principais motivos de atraso estão associados aos atrasos mais longos, indicando potencial impacto financeiro adverso?

**Estratégicas**
- Com base nos padrões identificados (rotas, horários, aeroportos, motivos de atraso/cancelamento), quais são as principais áreas de foco recomendadas para ações de melhoria operacional?
- Existe algum aeroporto de origem ou destino que consistentemente apresenta uma taxa de atraso/cancelamento desproporcionalmente alta (controlando por volume de voos)?
- Quais fatores operacionais exacerbam o “efeito cascata”, e quais estratégias de planejamento de contingência poderiam ser consideradas?

***1. Processar e preparar a base de dados***

***Identificar e Tratar Valores Nulos***

Foram encontrados valores nulos na tabela "companhia_aerea" e na "tabela_voos". Como tratamento, os valores nulos da tabela "companhia_aerea" foram excluídos. Abaixo query:

```sql
DELETE FROM `projeto-4-461417.voos.companhia_aerea`
WHERE Code IS NULL;
```

```sql
DELETE FROM `projeto-4-461417.voos.companhia_aerea`
WHERE Description IS NULL;
```

Na tabela "tabela_voos" os valores nulos foram mantidos, pois foi entendido que são voos cancelados ou desviados.

***Identificar e Tratar Valores Duplicados***

Foram encontrados valores duplicados na tabela "companhia_aerea". Query:

```sql
SELECT 
Code,
COUNT (*) as duplicatas
FROM `projeto-4-461417.voos.companhia_aerea`
GROUP BY Code
HAVING COUNT (*) >1
```

Abaixo as queries utilizadas para excluir as duplicatas:

```sql
DELETE FROM `projeto-4-461417.voos.companhia_aerea`
WHERE Description = 'AXIS AVIATION SWITZERLAND AG: XQQ'
```

```sql
DELETE FROM `projeto-4-461417.voos.companhia_aerea`
WHERE Description = 'WESTERN AIR LTD: WU'
```

```sql
DELETE FROM `projeto-4-461417.voos.companhia_aerea`
WHERE Description = 'SPARFELL MALTA LTD: QFX'
```

***Verificar e Alterar o Tipo de Dados***

Foi corrigido o formato dos campos que registram horários. Abaixo query:

```sql
CREATE OR REPLACE TABLE `projeto-4-461417.voos.tabela_voos` AS
SELECT 
*,
# transformações de horário
CASE
  WHEN crs_dep_time IS NOT NULL THEN 
    PARSE_TIME(
      "%H:%M:%S", 
      FORMAT(
        '%02d:%02d:00',
        CASE WHEN CAST(crs_dep_time / 100 AS INT64) = 24 THEN 0 
             ELSE CAST(crs_dep_time / 100 AS INT64) 
        END,
        MOD(crs_dep_time, 100)
      )
    )
  ELSE NULL
END AS crs_dep_time_parsed,

CASE
  WHEN dep_time IS NOT NULL THEN 
    PARSE_TIME(
      "%H:%M:%S", 
      FORMAT(
        '%02d:%02d:00',
        CASE WHEN CAST(dep_time / 100 AS INT64) = 24 THEN 0 
             ELSE CAST(dep_time / 100 AS INT64) 
        END,
        MOD(dep_time, 100)
      )
    )
  ELSE NULL
END AS dep_time_parsed,

CASE
  WHEN wheels_off IS NOT NULL THEN 
    PARSE_TIME(
      "%H:%M:%S", 
      FORMAT(
        '%02d:%02d:00',
        CASE WHEN CAST(wheels_off / 100 AS INT64) = 24 THEN 0 
             ELSE CAST(wheels_off / 100 AS INT64) 
        END,
        MOD(wheels_off, 100)
      )
    )
  ELSE NULL
END AS wheels_off_parsed,

CASE
  WHEN wheels_on IS NOT NULL THEN 
    PARSE_TIME(
      "%H:%M:%S", 
      FORMAT(
        '%02d:%02d:00',
        CASE WHEN CAST(wheels_on / 100 AS INT64) = 24 THEN 0 
             ELSE CAST(wheels_on / 100 AS INT64) 
        END,
        MOD(wheels_on, 100)
      )
    )
  ELSE NULL
END AS wheels_on_parsed,

CASE
  WHEN crs_arr_time IS NOT NULL THEN 
    PARSE_TIME(
      "%H:%M:%S", 
      FORMAT(
        '%02d:%02d:00',
        CASE WHEN CAST(crs_arr_time / 100 AS INT64) = 24 THEN 0 
             ELSE CAST(crs_arr_time / 100 AS INT64) 
        END,
        MOD(crs_arr_time, 100)
      )
    )
  ELSE NULL
END AS crs_arr_time_parsed,

CASE
  WHEN arr_time IS NOT NULL THEN 
    PARSE_TIME(
      "%H:%M:%S", 
      FORMAT(
        '%02d:%02d:00',
        CASE WHEN CAST(arr_time / 100 AS INT64) = 24 THEN 0 
             ELSE CAST(arr_time / 100 AS INT64) 
        END,
        MOD(arr_time, 100)
      )
    )
  ELSE NULL
END AS arr_time_parsed
FROM `projeto-4-461417.voos.tabela_voos`
```

***Unir Tabelas***

As 3 tabelas foram unificadas. Abaixo queries:

```sql
CREATE OR REPLACE TABLE `projeto-4-461417.voos.tabela_voos` AS
SELECT  
a.*,
b.string_field_1 as motivo_cancel
FROM `projeto-4-461417.voos.tabela_voos` a
JOIN
`projeto-4-461417.voos.code_cancel` b
ON
a.CANCELLATION_CODE = b.string_field_0
```

```sql
CREATE OR REPLACE TABLE `projeto-4-461417.voos.tabela_voos` AS
SELECT  
a.*,
b.Description as companhia
FROM `projeto-4-461417.voos.tabela_voos` a
JOIN
`projeto-4-461417.voos.companhia_aerea` b
ON
a.DOT_CODE = b.Code
```

```sql
CREATE OR REPLACE TABLE `projeto-4-461417.voos.tabela_voos` AS
SELECT  
a.*,
b.string_field_1 as orgao_operador
FROM `projeto-4-461417.voos.tabela_voos` a
JOIN
`projeto-4-461417.voos.operador_aeronave` b
ON
a.AIRLINE_CODE = b.string_field_0
```

***Criar novas variáveis***

Novas variáveis foram criadas e unificadas na tabela final. Abaixo queries:

```sql
CREATE OR REPLACE TABLE `projeto-4-461417.voos.tabela_voos_final` AS
SELECT 
  fl_date AS flight_date,
  # dia da semana
  FORMAT_DATE('%A', fl_date) AS weekday_name,
  airline_code,
  companhia,
  dot_code,
  orgao_operador,
  fl_number AS flight_number,
  # concatena companhia e voo para ficar mais fácil a visualização
  CONCAT(airline_code, ' ', fl_number) AS flight_id,
  # concatena origem e destino para ficar mais fácil a visualização
  CONCAT(origin, '-', dest) AS route_id,
  origin,
  origin_city,
  dest AS destination,
  dest_city AS destination_city,
  crs_dep_time_parsed AS crs_dep_time,
  dep_time_parsed AS dep_time,
  dep_delay,
  taxi_out,
  wheels_off_parsed AS wheels_off,
  wheels_on_parsed AS wheels_on,
  taxi_in,
  # tempo total de taxiamento = partida + chegada
  (taxi_out + taxi_in) AS taxi_time_total,
  crs_arr_time_parsed AS crs_arr_time,
  arr_time_parsed AS arr_time,
  arr_delay,
  cancelled,
  cancellation_code,
  motivo_cancel,
  diverted,
  crs_elapsed_time,
  elapsed_time,
  # o tempo de voo foi maior ou menor que o previsto? ganhamos tempo?
  (crs_elapsed_time - elapsed_time) AS recovered_time,
  air_time,
  distance,
  delay_due_carrier,
  delay_due_weather,
  delay_due_nas,
  delay_due_security,
  delay_due_late_aircraft,
FROM `projeto-4-461417.voos.tabela_voos`
```

Variável para atraso:

```sql
CREATE OR REPLACE TABLE `projeto-4-461417.voos.tabela_voos_final` AS
SELECT
*,
CASE WHEN arr_delay >= 15 THEN 1
ELSE 0
END AS atraso
FROM `projeto-4-461417.voos.tabela_voos_final`
```

Variável para atraso em cascata:

```sql
CREATE OR REPLACE TABLE `projeto-4-461417.voos.tabela_voos_final` AS
SELECT 
*,
CASE 
    WHEN delay_due_late_aircraft > 0 THEN 1 
    ELSE 0 
END AS cascade_delay_flag,
 FROM `projeto-4-461417.voos.tabela_voos_final`
```

Atraso na chegada:

```sql
CREATE OR REPLACE TABLE `projeto-4-461417.voos.tabela_voos_final` AS
SELECT 
*,
CASE
    WHEN arr_delay BETWEEN 15 AND 50 THEN 'Delay 15-50 min'
    WHEN arr_delay BETWEEN 51 AND 120 THEN 'Delay 51-120 min'
    WHEN arr_delay BETWEEN 121 AND 360 THEN 'Delay 121-360 min'
    WHEN arr_delay > 360 THEN 'Delay >360 min'
    ELSE 'On Time or Earlier'
END AS range_arr_delay,
 FROM `projeto-4-461417.voos.tabela_voos_final`
```

Causas de atraso:

```sql
CREATE OR REPLACE TABLE `projeto-4-461417.voos.tabela_voos_final` AS
SELECT
*,
CASE 
    WHEN delay_due_carrier >= GREATEST(
        delay_due_carrier, delay_due_weather, delay_due_nas, delay_due_security, delay_due_late_aircraft) THEN 'Carrier'
    WHEN delay_due_weather >= GREATEST(
        delay_due_carrier, delay_due_weather, delay_due_nas, delay_due_security, delay_due_late_aircraft) THEN 'Weather'
    WHEN delay_due_nas >= GREATEST(
        delay_due_carrier, delay_due_weather, delay_due_nas, delay_due_security, delay_due_late_aircraft) THEN 'NAS'
    WHEN delay_due_security >= GREATEST(
        delay_due_carrier, delay_due_weather, delay_due_nas, delay_due_security, delay_due_late_aircraft) THEN 'Security'
    WHEN delay_due_late_aircraft >= GREATEST(
        delay_due_carrier, delay_due_weather, delay_due_nas, delay_due_security, delay_due_late_aircraft) THEN 'Late Aircraft'
    ELSE 'No Delay Cause'
END AS main_delay_cause,
FROM `projeto-4-461417.voos.tabela_voos_final`
```

Período da partida:

```sql
CREATE OR REPLACE TABLE `projeto-4-461417.voos.tabela_voos_final` AS
SELECT
*,
CASE
    WHEN EXTRACT(HOUR FROM crs_dep_time) BETWEEN 5 AND 11 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM crs_dep_time) BETWEEN 12 AND 17 THEN 'Afternoon'
    WHEN EXTRACT(HOUR FROM crs_dep_time) BETWEEN 18 AND 23 THEN 'Night'
    ELSE 'Red-Eye'
END AS scheduled_dep_period
 FROM `projeto-4-461417.voos.tabela_voos_final`
```

***2. Análise Exploratória***

***Ver Distribuição***

Identificamos que a variável "arr_delay" contabiliza todo o atraso ocorrido no voo. Dessa forma, fizemos a análise de distribuição dessa variável.

```python
import matplotlib.pyplot as plt
import seaborn as sns

# Definir o estilo do gráfico
sns.set(style='whitegrid')

# Filtrar valores extremos para uma visualização mais clara (excluir outliers)
# Aqui limitamos entre -100 e 300 minutos, mas você pode ajustar conforme os dados
arr_delay_filtrado = df['arr_delay'].dropna()
arr_delay_filtrado = arr_delay_filtrado[(arr_delay_filtrado >= -100) & (arr_delay_filtrado <= 300)]

# Criar histograma
plt.figure(figsize=(12,6))
sns.histplot(arr_delay_filtrado, bins=50, kde=True, color='skyblue')

# Personalização do gráfico
plt.title('Histograma de Atrasos na Chegada (arr_delay)', fontsize=16)
plt.xlabel('Atraso na Chegada (minutos)', fontsize=12)
plt.ylabel('Frequência', fontsize=12)
plt.axvline(x=0, color='red', linestyle='--', label='Sem Atraso')
plt.legend()

plt.tight_layout()
plt.show()
```

![image](https://github.com/user-attachments/assets/765682e3-d0b2-4b15-9a65-5cf9b1d1c996)

Podemos ver que a maioria dos atrasos ocorrem com até 50 minutos e que muitos voos também apresentam valores negativos, ou seja, saíram com antecedência.

Na variável "taxi_time_total" foi contabilizado todo o tempo de taxeamento do avião durante um voo. Abaixo a análise de distribuição dessa variável:

```python
import matplotlib.pyplot as plt
import seaborn as sns

# Verificar se a coluna existe (opcional)
assert 'taxi_time_total' in df.columns, "A coluna 'taxi_time_total' não existe no DataFrame."

# Filtrar valores extremos para melhor visualização (ex: até 60 minutos)
taxi_time = df['taxi_time_total'].dropna()
taxi_time = taxi_time[(taxi_time >= 0) & (taxi_time <= 60)]

# Criar histograma
plt.figure(figsize=(12,6))
sns.histplot(taxi_time, bins=40, kde=True, color='mediumseagreen')

# Personalização
plt.title('Histograma do Tempo Total de Taxiamento (taxi_time_total)', fontsize=16)
plt.xlabel('Tempo Total de Taxiamento (minutos)', fontsize=12)
plt.ylabel('Frequência', fontsize=12)
plt.axvline(x=taxi_time.mean(), color='red', linestyle='--', label=f'Média: {taxi_time.mean():.1f} min')
plt.legend()

plt.tight_layout()
plt.show()
```

![image](https://github.com/user-attachments/assets/e89a8c94-3311-4129-a09b-61b51bf6f3f6)

Podemos ver que a maioria dos aviões levam até 30 minutos para fazer o taxeamento completo dentro de um voo, incluindo o taxeamento de decolagem e pouso.

***Calcular quartis, decis ou percentis***

Abaixo a query utilizada para calcular os quartis:

```sql
CREATE OR REPLACE TABLE `projeto-4-461417.voos.tabela_voos_final` AS
SELECT
*,
# cria quartis para atrasos na partida
NTILE(4) OVER (ORDER BY dep_delay) AS dep_delay_quartile,
# cria quartis para tempo de taxiamento na partida
NTILE(4) OVER (ORDER BY taxi_out) AS taxi_out_quartile,
# cria quartis para tempo de taxiamento na chegada
NTILE(4) OVER (ORDER BY taxi_in) AS taxi_in_quartile,
# cria quartis para distancia
NTILE(4) OVER (ORDER BY distance) AS distance_quartile,
# cria quartis para atrasos na chegada
NTILE(4) OVER (ORDER BY arr_delay) AS arr_delay_quartile,
 FROM `projeto-4-461417.voos.tabela_voos_final`
```

***3. Análise***

***Validar Hipótese***

Abaixo validamos as "Perguntas de Negócios" feitas inicialmente.

**Operacionais**
- Quais são as rotas com maior frequência e magnitude (tempo médio) de atrasos e cancelamentos?

![image](https://github.com/user-attachments/assets/6afd212c-8b8d-4986-a668-bd224dfb736a)

![image](https://github.com/user-attachments/assets/5eb384ad-0a56-4c92-934d-48a3dec666ff)

- Quais são os principais motivos dos atrasos e cancelamentos registrados, e como esses motivos se distribuem pelas rotas/companhias aéreas mais problemáticas?

![image](https://github.com/user-attachments/assets/36a106a3-5faf-4802-81b5-fd4f2dabcb48)

![image](https://github.com/user-attachments/assets/35cc59fb-ec37-4267-8d45-5c3aeb6e3ed6)

- Como os atrasos variam por dia da semana e período do dia, e qual o impacto do “efeito cascata” nesses períodos?

![image](https://github.com/user-attachments/assets/43d88ee6-b067-4db5-931f-22b33d973afb)

O principal dia com atraso é quarta-feira e o período da tarde. O efeito cascada sobre isso permece no mesmo dia e período, porém, caindo quase pela metade o atraso inicial.

![image](https://github.com/user-attachments/assets/65fc3b2e-1422-4fb1-b009-b65c64ec114b)

- Quais aeroportos de origem e destino apresentam os maiores tempos médios de taxiamento, indicando possíveis gargalos no solo?

![image](https://github.com/user-attachments/assets/411ae4fc-ee09-422c-9d1b-d45b02bfd80d)

**Financeiras**
- Quais rotas, períodos do dia e principais motivos de atraso estão associados aos atrasos mais longos, indicando potencial impacto financeiro adverso?

![image](https://github.com/user-attachments/assets/6034c4b9-2278-4ef5-919e-e3c6bb66e614)

**Estratégicas**
- Com base nos padrões identificados (rotas, horários, aeroportos, motivos de atraso/cancelamento), quais são as principais áreas de foco recomendadas para ações de melhoria operacional?

As áreas de foco devem ser uma melhor distribuição de dias e horários de voos para retirar o gargalo da quarta-feira e do período da tarde e uma tentativa de diminuir o tempo de taxeamento nos aeroportos de origem e destino para evitar efeito cascata de atraso.

- Existe algum aeroporto de origem ou destino que consistentemente apresenta uma taxa de atraso/cancelamento desproporcionalmente alta (controlando por volume de voos)?

Os aeroportos de Houston e Baton Rouge apresentam as principais taxas de atrasos e cancelamentos mas nada que seja desproporcional em comparação aos demais do top 5 analisado.

![image](https://github.com/user-attachments/assets/d14ed481-3cbc-46d6-911e-9e606ddff2b0)

- Quais fatores operacionais exacerbam o “efeito cascata”, e quais estratégias de planejamento de contingência poderiam ser consideradas?
