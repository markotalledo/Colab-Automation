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

select * from VR_CARTERAS_DICIEMBRE_FINAL_NUEVOS;
SELECT * FROM VR_CARTERAS_DICIEMBRE_TOTALES_2_NUEVOS;
