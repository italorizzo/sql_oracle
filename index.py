from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException
import platform
import os
from time import sleep
import pandas as pd
import matplotlib.pyplot as plt

diretorio_atual = os.path.dirname(os.path.realpath(__file__))
download_directory = os.path.join(diretorio_atual, "sql_oracle") 
sistema_operacional = platform.system()
meses = {1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun', 7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'}

def encontrar_elemento(by, value, driver, timeout=20):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))

def clicar_elemento(elemento, driver, timeout=20):
    return WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(elemento))

credenciaisOracle ={
    "user": "IRIZZO",
    "password": "X3051Ukv*£9="
}

codigo_sql = [
    ## criação de conta por mes
     """select ms.mes,
       coalesce(count(distinct clogger.cd_cpfcgc), 0) as aberturas_conta
  from (select column_value as mes from table(sys.odcivarchar2list('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'))) ms
  left join veritas_silver.sinacor_tsclogger clogger
    on to_number(ms.mes) = extract(month from clogger.dt_ocorrencia)
   and extract(year from clogger.dt_ocorrencia) = extract(year from sysdate)
   and clogger.ds_ocorrencia = 'INCLUSAO'
 group by ms.mes
 order by to_number(ms.mes);""",

 ## criação de conta hora a hora dia anterior
    """select h.hora,
       coalesce(count(distinct cd_cpfcgc),0) as aberturas_conta
  from (
    select level - 1 as hora
    from dual
    connect by level <= 24
  ) h
  left join veritas_silver.sinacor_tsclogger clogger
    on h.hora = extract(hour from clogger.dt_ocorrencia)
   and to_char(dt_ocorrencia, 'YYYY-MM-DD') = to_char(trunc(sysdate) - 1, 'YYYY-MM-DD')
   and ds_ocorrencia = 'INCLUSAO'
 group by h.hora
 order by h.hora;""",

## criação de conta histórico, 3, 7 e 30 dias uteis
    """select h.hora,
       case when d3.aberturas_conta is null and d1.aberturas_conta is null then 0 else (1-coalesce(d3.aberturas_conta, 0)/coalesce(d1.aberturas_conta, 1)) end as DU3, 
       case when d7.aberturas_conta is null and d1.aberturas_conta is null then 0 else (1-coalesce(d7.aberturas_conta, 0)/coalesce(d1.aberturas_conta, 1)) end as DU7, 
       case when d30.aberturas_conta is null and d1.aberturas_conta is null then 0 else (1-coalesce(d30.aberturas_conta, 0)/coalesce(d1.aberturas_conta, 1)) end as DU30
from (
    select level - 1 as hora
    from DUAL
    connect by level <= 24
) h
left join (
    select extract(hour from dt_ocorrencia) as hora,
        count(distinct cd_cpfcgc) as aberturas_conta
    from veritas_silver.sinacor_tsclogger
    where to_char(dt_ocorrencia, 'YYYY-MM-DD') = (
                                                    select case 
                                                            when calendar_iso_weekday = 7 then to_char(trunc(sysdate) - 3, 'YYYY-MM-DD')
                                                            when calendar_iso_weekday = 6 then to_char(trunc(sysdate) - 2, 'YYYY-MM-DD')
                                                            when is_holiday = 'True' then to_char(trunc(sysdate) - 2, 'YYYY-MM-DD')
                                                        else to_char(trunc(sysdate) - 1, 'YYYY-MM-DD')
                                                        end as dia
                                                    from veritas_silver.shared_calendar
                                                    where calendar_date = to_char(trunc(sysdate) - 1, 'YYYY-MM-DD')
    )
    and ds_ocorrencia = 'INCLUSAO'
    group by extract(hour from dt_ocorrencia)
    order by extract(hour from dt_ocorrencia)
) d1
on h.hora = d1.hora
left join (
    select extract(hour from dt_ocorrencia) as hora,
        count(distinct cd_cpfcgc) as aberturas_conta
    from veritas_silver.sinacor_tsclogger
    where to_char(dt_ocorrencia, 'YYYY-MM-DD') = (
                                                    select case 
                                                            when calendar_iso_weekday = 7 then to_char(trunc(sysdate) - 5, 'YYYY-MM-DD')
                                                            when calendar_iso_weekday = 6 then to_char(trunc(sysdate) - 4, 'YYYY-MM-DD')
                                                            when is_holiday = 'True' then to_char(trunc(sysdate) - 4, 'YYYY-MM-DD')
                                                        else to_char(trunc(sysdate) - 3, 'YYYY-MM-DD')
                                                        end as dia
                                                    from veritas_silver.shared_calendar
                                                    where calendar_date = to_char(trunc(sysdate) - 3, 'YYYY-MM-DD')
    )
    and ds_ocorrencia = 'INCLUSAO'
    group by extract(hour from dt_ocorrencia)
    order by extract(hour from dt_ocorrencia)
) d3
on h.hora = d3.hora
left join (
    select extract(hour from dt_ocorrencia) as hora,
        count(distinct cd_cpfcgc) as aberturas_conta
    from veritas_silver.sinacor_tsclogger
    where to_char(dt_ocorrencia, 'YYYY-MM-DD') = (
                                                    select case 
                                                            when calendar_iso_weekday = 7 then to_char(trunc(sysdate) - 9, 'YYYY-MM-DD')
                                                            when calendar_iso_weekday = 6 then to_char(trunc(sysdate) - 8, 'YYYY-MM-DD')
                                                            when is_holiday = 'True' then to_char(trunc(sysdate) - 8, 'YYYY-MM-DD')
                                                        else to_char(trunc(sysdate) - 7, 'YYYY-MM-DD')
                                                        end as dia
                                                    from veritas_silver.shared_calendar
                                                    where calendar_date = to_char(trunc(sysdate) - 7, 'YYYY-MM-DD')
    )
    and ds_ocorrencia = 'INCLUSAO'
    group by extract(hour from dt_ocorrencia)
    order by extract(hour from dt_ocorrencia)
) d7
on h.hora = d7.hora
left join (
    select extract(hour from dt_ocorrencia) as hora,
        count(distinct cd_cpfcgc) as aberturas_conta
    from veritas_silver.sinacor_tsclogger
    where to_char(dt_ocorrencia, 'YYYY-MM-DD') = (
                                                    select case 
                                                            when calendar_iso_weekday = 7 then to_char(trunc(sysdate) - 32, 'YYYY-MM-DD')
                                                            when calendar_iso_weekday = 6 then to_char(trunc(sysdate) - 31, 'YYYY-MM-DD')
                                                            when is_holiday = 'True' then to_char(trunc(sysdate) - 31, 'YYYY-MM-DD')
                                                        else to_char(trunc(sysdate) - 30, 'YYYY-MM-DD')
                                                        end as dia
                                                    from veritas_silver.shared_calendar
                                                    where calendar_date = to_char(trunc(sysdate) - 30, 'YYYY-MM-DD')
    )
    and ds_ocorrencia = 'INCLUSAO'
    group by extract(hour from dt_ocorrencia)
    order by extract(hour from dt_ocorrencia)
) d30
on h.hora = d30.hora
order by h.hora""",

## onboarding x suitability
    """select mes_criacao as mes,
       round(sum(com_api * case when gap_preenchido = 0 then 1 end)/(sum(sem_api)+sum(com_api))*100, 2) as M0,
       round(sum(com_api * case when gap_preenchido = 1 then 1 end)/(sum(sem_api)+sum(com_api))*100, 2) as M1,
       round(sum(com_api * case when gap_preenchido = 2 then 1 end)/(sum(sem_api)+sum(com_api))*100, 2) as M2,
       round(sum(com_api * case when gap_preenchido = 3 then 1 end)/(sum(sem_api)+sum(com_api))*100, 2) as M3,
       round(sum(com_api * case when gap_preenchido = 4 then 1 end)/(sum(sem_api)+sum(com_api))*100, 2) as M4,
       round(sum(com_api * case when gap_preenchido = 5 then 1 end)/(sum(sem_api)+sum(com_api))*100, 2) as M5,
       round(sum(com_api * case when gap_preenchido = 6 then 1 end)/(sum(sem_api)+sum(com_api))*100, 2) as M6,
       round(sum(sem_api)/(sum(sem_api)+sum(com_api))*100, 2) as NR
  from (
        select extract(month from dt_criacao) as mes_criacao,
            sum(case when perfil is null or perfil like '%Sem Perfil%' then 1 else 0 end) as sem_api, 
            sum(case when perfil like '%Conservador%' or perfil like '%Moderado%' or perfil like '%Arrojado%' then 1 else 0 end) as com_api,
            trunc(months_between(dt_atualiz, dt_criacao)) as gap_preenchido
        from veritas_gold.vw_cliente
        where tp_pessoa = 'F'
        and dt_criacao >= TO_DATE('07-01-2023', 'MM/DD/YYYY')
        group by extract(month from dt_criacao),
                trunc(months_between(dt_atualiz, dt_criacao))
        order by extract(month from dt_criacao) asc
  )
  group by mes_criacao
  order by mes_criacao
"""
]

print('O código está sendo execultado...')
chrome_options = Options()
# chrome_options.add_argument("--headless")
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_directory,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})
driver = webdriver.Chrome(options=chrome_options)
driver.get("https://g865716a94a2504-veritasprod.adb.sa-vinhedo-1.oraclecloudapps.com/ords/sql-developer?")
username = encontrar_elemento(By.ID, "username", driver)
password = encontrar_elemento(By.ID, "password", driver)
if sistema_operacional == 'Darwin':
    username.send_keys(credenciaisOracle["user"])
    password.send_keys(credenciaisOracle["password"])
elif sistema_operacional == 'Windows':
    username.click()
    username.send_keys(credenciaisOracle["user"])
    password.click()
    password.send_keys(credenciaisOracle["password"])
botao = encontrar_elemento(By.ID, "submitform-button", driver)
botao.click()
elemento_a = encontrar_elemento(By.ID, "WORKSHEET", driver)
elemento_a.click()
botao_fechar = encontrar_elemento(By.CLASS_NAME, "hopscotch-bubble-close", driver)
botao_fechar.click()
editor_sql = encontrar_elemento(By.CLASS_NAME, "view-lines", driver)
if editor_sql:
    for i, query in enumerate(codigo_sql):
        editor_sql.click()
        if sistema_operacional == 'Darwin':
            ActionChains(driver).key_down(Keys.COMMAND).send_keys('a').key_up(Keys.COMMAND).perform()
        elif sistema_operacional == 'Windows':
            ActionChains(driver).key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL).perform()
        ActionChains(driver).send_keys(Keys.DELETE).perform()
        ActionChains(driver).send_keys(query).perform()
        clicar_elemento((By.ID, "code-editor-btn-run-statement"), driver).click()
        try:
            clicar_elemento((By.ID, "download-query-menu-button"), driver).click()
            clicar_elemento((By.ID, "download-menu-csv"), driver).click()
            sleep(1)
            driver.switch_to.window(driver.window_handles[0])
            while any([".crdownload" in filename for filename in os.listdir(download_directory)]):
                sleep(1)
            os.chdir(download_directory)
            cont = 0
            for filename in os.listdir("."):
                if filename.endswith(".csv"):
                    cont += 1
                    novo_nome = f"query_{i+1}_arquivo_{cont}.csv"
                    os.rename(filename, novo_nome)
        except TimeoutException:
            print("Elemento não encontrado")

df1 = pd.read_csv('query_4_arquivo_1.csv')
plt.figure(figsize=(10, 6))
plt.plot(df1['HORA'], df1['DU3'], label='DU3')
plt.plot(df1['HORA'], df1['DU7'], label='DU7')
plt.plot(df1['HORA'], df1['DU30'], label='DU30')
plt.xlabel('Hora')
plt.ylabel('%')
plt.title('ABERTURA DE CONTA - COMPARAÇÃO DIAS UTEIS', color='#14c770', size=17, fontweight='bold')
plt.xticks(range(24))
plt.legend()
plt.savefig('./images/ABERTURA_CONTA_HIST.png')

plt.figure()

df2 = pd.read_csv('query_4_arquivo_2.csv')
plt.figure(figsize=(10, 6))
plt.bar(df2['HORA'], df2['ABERTURAS_CONTA'], color='#14c770', width=0.9)
plt.xlabel('Hora')
plt.ylabel('Aberturas de Conta')
plt.title('ABERTURA DE CONTA HORA A HORA - ONTEM', color='#14c770', size=17, fontweight='bold')
plt.xticks(range(24))
for i, v in enumerate(df2['ABERTURAS_CONTA']):
    plt.text(i, v/2, str(v), ha='center', va='center', color='white')
plt.savefig('./images/ABERTURA_CONTA_HORA.png')

plt.figure()

df3 = pd.read_csv('query_4_arquivo_3.csv')
plt.figure(figsize=(10, 6))
plt.bar(df3['MES'], df3['ABERTURAS_CONTA'], color='#14c770', width=0.9)
plt.xlabel('Mes')
plt.ylabel('Aberturas de Conta')
plt.xticks(range(1, 13))
plt.title('ABERTURA DE CONTA MES A MES', color='#14c770', size=17, fontweight='bold')
for i, v in enumerate(df3['ABERTURAS_CONTA']):
    if not pd.isnull(v): 
        plt.text(i+1, v/2, str(v), ha='center', va='center', color='black')
plt.savefig('./images/ABERTURA_CONTA_MES.png')

df4 = pd.read_csv('query_4_arquivo_4.csv')
df4 = df4.loc[:, ~df4.columns.str.contains('^Unnamed')]
df4['M0'] = df4['M0'].fillna('')
df4['M1'] = df4['M1'].fillna('')
df4['M2'] = df4['M2'].fillna('')
df4['M3'] = df4['M3'].fillna('')
df4['M4'] = df4['M4'].fillna('')
df4['M5'] = df4['M5'].fillna('')
df4['M6'] = df4['M6'].fillna('')
df4['MES'] = df4['MES'].map(meses)
fig, ax = plt.subplots(figsize=(12, 4))
ax.axis('off')
table = ax.table(cellText=df4.values, colLabels=df4.columns, cellLoc='center', loc='center')

for i in range(0, len(df4)+1):
    for j in range(len(df4.columns)):
        cell = table[i, j]
        cell.set_height(0.1)
        if i == 0:
            cell._text.set_color('white')
            cell.set_facecolor('#14c770')
            cell._text.set_fontweight('bold')
        if j != 0 and i >= 1: 
            cell_text = cell.get_text().get_text()
            if cell_text: 
                cell_text = f"{cell_text}%"
                cell.get_text().set_text(cell_text)

ax.set_title('ONBOARDING x SUITABILITY', color='#14c770', size=17, fontweight='bold')

plt.savefig('./images/SUITABILITY.png', bbox_inches='tight', pad_inches=0.5)
