# Showcase

Необходимо реализовать месячный транзакционный агрегат и обогатить его epk_id на основе витрины связок.

Описание необходимых таблиц и их объемы + партиционирование представлено ниже:

Детальные транзакции:



Объем терабайт

CREATE EXTERNAL TABLE `txn`(

`evt_id` decimal(18,0), --id события, уникальный ключ

`evt_tim` timestamp, --timestamp события

`client_w4_id` decimal(18,0), --уникальный идентификатор клиента

`mcc_code` decimal(18,0), --mcc код транзакции

`local_amt` decimal(28,10) --сумма транзакции)

PARTITIONED BY (`trx_date` string) --Партиционирование по дате транзакции в формате yyyy-mm-dd

STORED AS PARQUET

LOCATION 'data/custom/rb/card/pa/txn'



Объем 300Гб

CREATE EXTERNAL TABLE `epk_lnk_host_id`(

`epk_id` bigint,

`external_system` string, --Наименование системы источника, тут фльтровать нужно по 'WAY4'

`external_system_client_id` string --id клиента в системе источника,

`row_actual_from` string --дата начла актуальности записи)

PARTITIONED BY ( `row_actual_to` string) --дата окончания актуальности записи, нас интересуют только актуальные id клиента

STORED AS PARQUET

LOCATION 'data/custom/rb/epk/pa/epk_lnk_host_id'



DDL таблицы которую хотим получить:

CREATE TABLE `epk_lnk_host_id`(

`epk_id` bigint,

`sum_txn` string, --Наименование системы источника, тут фльтровать нужно по 'WAY4'

`mcc_code` string --mcc код транзакций)

PARTITIONED BY ( `report_dt` string) --отчетная дата, конец месяца в формате yyyy-mm-dd

STORED AS PARQUET

LOCATION 'data/custom/rb/txn_aggr/pa/txn_aggr
