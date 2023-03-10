# -*- coding: utf-8 -*-
"""Carteras_Diciembre.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1mDkWbvDhPUDoJWWqUHcrLQwHTE7piPFs
"""

pip install --upgrade snowflake-sqlalchemy

#%%
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import statsmodels.api as sm
import warnings

from snowflake.connector import connect

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

Snowflake_conn = create_engine(URL( 
                          user='valeria.ramon@mercadofavo.com',
                          authenticator='externalBrowser',
                          account='vta08432.us-east-1',
                          database='FAVODATA',
                          warehouse='FAVODATA',
                          schema='JOURNEY'
                          ))

query_creacion_old = '''

WITH VR_VENTA_DICIEMBRE as (
select 
cast(a.create_date_time_tz as date) as calendar_date,
cast(date_trunc(month, a.create_date_time_tz) as date) as calendar_month,
--case when extract(day from calendar_date)<=15 then 1 else 2 as quincena,
a.store_url,
a.leader_name,
--b.tier_net_lm,
a.dynamo_customer_id,
a.order_number,
sum(net_value) AS total
--sum(total) over (partition by a.store_url,quincena) as venta_quincenal,
from journey.base as a
--left join FAVODATA.SNP_SANDBOX.AN_ENTREP_TIER_TEST as b
--on a.store_url = b.store_url
where create_date_time is not null and order_status not in ('CANCEL' ,'OPEN') and cancel_date_time_tz is null and calendar_month = '2022-12-01'
group by 1,2,3,4,5,6
order by 1 desc),

VR_VENTA_DICIEMBRE_CARTERAS as (
  select a.*, b.mentora, b.orientador, b.flag_emprendedor from 
  VR_VENTA_DICIEMBRE as a
  left join
  FAVODATA.SNP_UNTRUSTED.CARTERAS_DICIEMBRE as b
  on 
  a.store_url = b.emprendedor
  where
  b.flag_retencion = 0
  and b.flag_emprendedor = 'old'
),

VR_CARTERAS_DICIEMBRE_FINAL as (
select 
a.mentora,
a.orientador,
a.store_url,
a.leader_name,
a.dynamo_customer_id,
b.customer_name,
sum(a.total) as total_cliente,
case when total_cliente >= 100 then 1 else 0 end as flag_bono,
case when total_cliente < 100 then round(100 - total_cliente,0) else 0 end as falta
from
VR_VENTA_DICIEMBRE_CARTERAS as a
left join 
journey.customer as b
on a.dynamo_customer_id = b.dynamo_customer_id
--poner nombre cliente
group by 1,2,3,4,5,6
),

 VR_CARTERAS_DICIEMBRE_TOTALES as (
select 
mentora, 
orientador,
store_url, 
count(distinct dynamo_customer_id) as clientes,
sum(flag_bono) as clientes_bono,
case when clientes_bono >=5 then 1 else 0 end flag_graduado
from VR_CARTERAS_DICIEMBRE_FINAL
group by 1,2,3
),

 VR_CARTERAS_DICIEMBRE_TOTALES_2 as (
select 
mentora, 
a.orientador,
truncate((sum(flag_graduado)/b.cartera),4) as porc_graduados,
b.cartera,
sum(flag_graduado) as total_graduados,
case 
when porc_graduados >= 0.25 then 100
when porc_graduados >= 0.20 then 85
when porc_graduados >=0.15 then 70
when porc_graduados >= 0.10 then 60
when porc_graduados >= 0.06 then 50
when porc_graduados >= 0.03 then 40
else 0
end as ganancia_unitaria,
case 
when porc_graduados >= 0.25 then total_graduados*100
when porc_graduados >= 0.20 then total_graduados*85
when porc_graduados >= 0.15 then total_graduados*70
when porc_graduados >= 0.10 then total_graduados*60
when porc_graduados >= 0.06 then total_graduados*50
when porc_graduados >= 0.03 then total_graduados*40
else 0
end as ganancia,
case 
when porc_graduados >= 0.25 then 'max'
when 0.2 <= porc_graduados and porc_graduados < 0.25 then 0.25
when 0.15 <= porc_graduados and porc_graduados < 0.20 then 0.20
when 0.10 <= porc_graduados and porc_graduados < 0.15 then 0.15
when 0.06 <= porc_graduados and porc_graduados < 0.10 then 0.10
when 0.03 <= porc_graduados and porc_graduados < 0.06 then 0.06
else 0.03
end as porc_meta,
case 
when porc_graduados >= 0.25 then 'max'
when 0.2 <= porc_graduados and porc_graduados < 0.25 then ceil(b.cartera*0.25,0)
when 0.15 <= porc_graduados and porc_graduados < 0.20 then ceil(b.cartera*0.20,0)
when 0.10 <= porc_graduados and porc_graduados < 0.15 then ceil(b.cartera*0.15,0)
when 0.06 <= porc_graduados and porc_graduados < 0.10 then ceil(b.cartera*0.10,0)
when 0.03 <= porc_graduados and porc_graduados < 0.06 then ceil(b.cartera*0.06,0)
else ceil(b.cartera*0.03,0)
end as emprendedores_meta
from VR_CARTERAS_DICIEMBRE_TOTALES as a
left join FAVODATA.SNP_UNTRUSTED.CARTERAS_DICIEMBRE_TOTAL as b
on a.orientador = b.orientador
group by 1,2,4
)

{0}

'''

query_creacion_new = ''' 
WITH VR_VENTA_DICIEMBRE_NUEVOS as (select cast(a.create_date_time_tz as date)                    as calendar_date,
                                          cast(date_trunc(month, a.create_date_time_tz) as date) as calendar_month,
--case when extract(day from calendar_date)<=15 then 1 else 2 as quincena,
                                          a.store_url,
                                          a.leader_name,
--b.tier_net_lm,
                                          a.dynamo_customer_id,
                                          a.order_number,
                                          sum(net_value)                                         AS total
--sum(total) over (partition by a.store_url,quincena) as venta_quincenal,
                                   from journey.base as a
--left join FAVODATA.SNP_SANDBOX.AN_ENTREP_TIER_TEST as b
--on a.store_url = b.store_url
                                   where create_date_time is not null
                                     and order_status not in ('CANCEL', 'OPEN')
                                     and cancel_date_time_tz is null
                                     and calendar_month = '2022-12-01'
                                   group by 1, 2, 3, 4, 5, 6
                                   order by 1 desc),

VR_VENTA_DICIEMBRE_CARTERAS_NUEVOS as (select a.*, b.mentora, b.orientador, b.flag_emprendedor
                                       from VR_VENTA_DICIEMBRE_NUEVOS as a
                                                left join
                                            FAVODATA.SNP_UNTRUSTED.CARTERAS_DICIEMBRE as b
                                            on
                                                a.store_url = b.emprendedor
                                       where b.flag_retencion = 0
                                         and b.flag_emprendedor = 'new'
                                       ),

VR_CARTERAS_DICIEMBRE_FINAL_NUEVOS as (
select
a.mentora,
a.orientador,
a.store_url,
a.leader_name,
a.dynamo_customer_id,
b.customer_name,
sum(a.total) as total_cliente,
case when total_cliente >= 100 then 1 else 0 end as flag_bono,
case when total_cliente < 100 then round(100 - total_cliente,0) else 0 end as falta
from
VR_VENTA_DICIEMBRE_CARTERAS_NUEVOS as a
left join
journey.customer as b
on a.dynamo_customer_id = b.dynamo_customer_id
--poner nombre cliente
group by 1,2,3,4,5,6
),

VR_CARTERAS_DICIEMBRE_TOTALES_NUEVOS as (
select
mentora,
orientador,
store_url,
count(distinct dynamo_customer_id) as clientes,
sum(flag_bono) as clientes_bono,
case when clientes_bono >=5 then 1 else 0 end flag_graduado
from VR_CARTERAS_DICIEMBRE_FINAL_NUEVOS
group by 1,2,3),

VR_CARTERAS_DICIEMBRE_TOTALES_2_NUEVOS as (
select
mentora,
a.orientador,
truncate((sum(flag_graduado)/b.cartera_nuevos),4) as porc_graduados,
b.cartera_nuevos,
sum(flag_graduado) as total_graduados,
case
when porc_graduados >= 0.18 then 150
when porc_graduados >= 0.14 then 130
when porc_graduados >=0.11 then 100
when porc_graduados >= 0.07 then 90
when porc_graduados >= 0.04 then 75
when porc_graduados >= 0.02 then 60
else 0
end as ganancia_unitaria,
case
when porc_graduados >= 0.18 then total_graduados*150
when porc_graduados >= 0.14 then total_graduados*130
when porc_graduados >= 0.11 then total_graduados*100
when porc_graduados >= 0.07 then total_graduados*90
when porc_graduados >= 0.04 then total_graduados*75
when porc_graduados >= 0.02 then total_graduados*60
else 0
end as ganancia,
case
when porc_graduados >= 0.18 then 'max'
when 0.14 <= porc_graduados and porc_graduados < 0.18 then 0.18
when 0.11 <= porc_graduados and porc_graduados < 0.14 then 0.14
when 0.07 <= porc_graduados and porc_graduados < 0.11 then 0.11
when 0.04 <= porc_graduados and porc_graduados < 0.07 then 0.07
when 0.02 <= porc_graduados and porc_graduados < 0.04 then 0.04
else 0.02
end as porc_meta,
case
when porc_graduados >= 0.18 then 'max'
when 0.14 <= porc_graduados and porc_graduados < 0.18 then ceil(b.cartera_nuevos*0.18,0)
when 0.11 <= porc_graduados and porc_graduados < 0.14 then ceil(b.cartera_nuevos*0.14,0)
when 0.07 <= porc_graduados and porc_graduados < 0.11 then ceil(b.cartera_nuevos*0.11,0)
when 0.04 <= porc_graduados and porc_graduados < 0.07 then ceil(b.cartera_nuevos*0.07,0)
when 0.02 <= porc_graduados and porc_graduados < 0.04 then ceil(b.cartera_nuevos*0.04,0)
else ceil(b.cartera_nuevos*0.02,0)
end as emprendedores_meta
from VR_CARTERAS_DICIEMBRE_TOTALES_NUEVOS as a
left join FAVODATA.SNP_UNTRUSTED.CARTERAS_DICIEMBRE_TOTAL as b
on a.orientador = b.orientador
group by 1,2,4)

{0}

'''

QUERY_VR_CARTERAS_DICIEMBRE_FINAL = query_creacion_old.format( '''select * from VR_CARTERAS_DICIEMBRE_FINAL;''')

QUERY_VR_CARTERAS_DICIEMBRE_TOTALES_2 = query_creacion_old.format( '''SELECT * FROM VR_CARTERAS_DICIEMBRE_TOTALES_2; ''')

QUERY_VR_CARTERAS_DICIEMBRE_FINAL_NUEVOS = query_creacion_new.format( '''select * from VR_CARTERAS_DICIEMBRE_FINAL_NUEVOS;''')

QUERY_VR_CARTERAS_DICIEMBRE_TOTALES_2_NUEVOS = query_creacion_new.format( '''SELECT * FROM VR_CARTERAS_DICIEMBRE_TOTALES_2_NUEVOS;''')

QUERY_COMISIONES = '''
select
cast(c.create_date_tz as date) as creation_date, 
cast(a.create_date_time_tz as date) as calendar_date,
cast(date_trunc(month, a.create_date_time_tz) as date) as calendar_month,
case when extract(day from calendar_date)<=15 then 1 else 2 end as quincena,
a.store_url,
a.leader_name,
b.tier_net_lm,
sum(net_value) AS total,
sum(total) over (partition by a.store_url,quincena,date_trunc('month',calendar_date)) as venta_quincenal,
case 
when creation_date >= '2022-07-01' then 'De 5% a 10%'
when creation_date < '2022-07-01' then 'De 8% a 15%'
else ''
end as comision
from journey.base as a
left join FAVODATA.SNP_SANDBOX.AN_ENTREP_TIER_TEST as b
on a.store_url = b.store_url and date_trunc('month',calendar_date) = b.calendar_month 
left join journey.entrep as c 
on a.store_url = c.store_url
where create_date_time is not null and order_status not in ('CANCEL' ,'OPEN') and cancel_date_time_tz is null and calendar_month > '2022-11-01'
group by 1,2,3,4,5,6,7
order by 1 desc
'''

### Executes SQL query and data into a Pandas DF
df_Carteras_DIC_Totales_2 = pd.read_sql(con = Snowflake_conn,  sql = QUERY_VR_CARTERAS_DICIEMBRE_TOTALES_2)
df_Carteras_DIC_Totales_Final = pd.read_sql(con = Snowflake_conn,  sql = QUERY_VR_CARTERAS_DICIEMBRE_FINAL)
df_Comisiones = pd.read_sql(con = Snowflake_conn,  sql = QUERY_COMISIONES)
df_carteras_dic_final_nuevos = pd.read_sql(con = Snowflake_conn,  sql = QUERY_VR_CARTERAS_DICIEMBRE_FINAL_NUEVOS)
df_carteras_dic_totales_nuevos = pd.read_sql(con = Snowflake_conn,  sql = QUERY_VR_CARTERAS_DICIEMBRE_TOTALES_2_NUEVOS)

# Librerias de GSheets 
from google.colab import auth
auth.authenticate_user()

import gspread
from google.auth import default

# Damos acceso a GColab para acceder al Gsheets
creds, _ = default()
gc = gspread.authorize(creds)

# carteras diciembre totales
WB_carteras_dic_detalle = gc.open_by_url('https://docs.google.com/spreadsheets/d/1NtlmBt4CiH8mgr_sVPM-bEj8ftxffp7oUSAY-ffZc10/edit?usp=sharing')


# carteras diciembre detalle
WB_carteras_dic_totales  = gc.open_by_url('https://docs.google.com/spreadsheets/d/14nQu3u09QYcOR3ilisa2yphQQ_xPy9wIHBHCD0AewFw/edit?usp=sharing')


# Tracker de comisiones

WB_comisiones  = gc.open_by_url('https://docs.google.com/spreadsheets/d/12I1tS7Qtdq1ZJjfcWiWBJKaOMmS3nocgIsFO4j1DhZ4/edit?usp=sharing')


# Carteras diciembre totales (nuevos)

WB_carteras_dic_totales_nuevos = gc.open_by_url('https://docs.google.com/spreadsheets/d/1sfLAiyVLaPdbZAP21xBMFXyIB47RXqoQoLbEDeomz38/edit?usp=sharing')


# Carteras diciembre detalle (nuevos)

WB_carteras_dic_detalle_nuevos = gc.open_by_url('https://docs.google.com/spreadsheets/d/199obgz8btVjSGlGPK5eCMdCzHbat8uXlfIX5mkTi9mY/edit?usp=sharing')

# preprocesamiento de algunas columnas 
df_Comisiones['creation_date'] = df_Comisiones['creation_date'].astype(str)
df_Comisiones['calendar_date'] = df_Comisiones['calendar_date'].astype(str)
df_Comisiones['calendar_month'] = df_Comisiones['calendar_month'].astype(str)

data_cartera_final = df_Carteras_DIC_Totales_Final.to_numpy().tolist()
data_cartera_total = df_Carteras_DIC_Totales_2.to_numpy().tolist()
data_comisiones = df_Comisiones.to_numpy().tolist()
data_cartera_final_nuevos = df_carteras_dic_final_nuevos.to_numpy().tolist()
data_cartera_total_nuevos =  df_carteras_dic_totales_nuevos.to_numpy().tolist()

headers_cartera_total = [df_Carteras_DIC_Totales_Final.columns.str.upper().to_numpy().tolist()]
headers_cartera_final = [df_Carteras_DIC_Totales_2.columns.str.upper().to_numpy().tolist()]
data_to_write_total = headers_cartera_final + data_cartera_total
# Llamamos a la tabla donde actualizaremos las variables
wsTarget = WB_carteras_dic_totales.worksheet('totales')
wsTarget.update(None,data_to_write_total)

data_to_write_total =  headers_cartera_total + data_cartera_final
# Llamamos a la tabla donde actualizaremos las variables
wsTarget = WB_carteras_dic_detalle.worksheet('carteras diciembre')
wsTarget.update(None,data_to_write_total)

headers_comisiones = [df_Comisiones.columns.str.upper().to_numpy().tolist()]
data_to_write_total =  headers_comisiones + data_comisiones
# Llamamos a la tabla donde actualizaremos las variables
wsTarget = WB_comisiones.worksheet('TRACKER COMISIONES')
wsTarget.update(None,data_to_write_total)

headers_cartera_final_nuevos = [df_carteras_dic_final_nuevos.columns.str.upper().to_numpy().tolist()]
data_to_write_total = headers_cartera_final_nuevos + data_cartera_final_nuevos
# Llamamos a la tabla donde actualizaremos las variables
wsTarget = WB_carteras_dic_detalle_nuevos.worksheet('carteras diciembre nuevos')
wsTarget.update(None,data_to_write_total)

headers_cartera_total_nuevos = [df_carteras_dic_totales_nuevos.columns.str.upper().to_numpy().tolist()]
data_to_write_total = headers_cartera_total_nuevos + data_cartera_total_nuevos
# Llamamos a la tabla donde actualizaremos las variables
wsTarget = WB_carteras_dic_totales_nuevos.worksheet('carteras diciembre nuevos (1)')
wsTarget.update(None,data_to_write_total)