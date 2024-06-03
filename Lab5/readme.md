# Лабораторная работа №5
Сысоев Г.А.

## Цель Работы

1.  Изучить возможности СУБД Clickhouse для обработки и анализ больших
    данных
2.  Получить навыки применения Clickhouse совместно с языком
    программирования R
3.  Получить навыки анализа метаинфомации о сетевом трафике
4.  Получить навыки применения облачных технологий хранения, подготовки
    и анализа данных: Managed Service for ClickHouse, Rstudio Server.

## Ход работы

## Установка

``` r
install.packages("ClickHouseHTTP")
```

## Подключение ClickHouse

``` r
library(tidyverse)
```

``` r
library(dplyr)
library(ClickHouseHTTP)
library(DBI)
con <- dbConnect(
   ClickHouseHTTP::ClickHouseHTTP(), 
   host="rc1d-sbdcf9jd6eaonra9.mdb.yandexcloud.net",
                      port=8443,
                      user="student24dwh",
                      password = "DiKhuiRIVVKdRt9XON",
                      db = "TMdata",
   https=TRUE, ssl_verifypeer=FALSE)
Clickhousedata <- dbReadTable(con, "data")
dff <- dbGetQuery(con, "SELECT * FROM data")
```

## Задание 1: Надите утечку данных из Вашей сети

### Важнейшие документы с результатами нашей исследовательской деятельности в области создания вакцин скачиваются в виде больших заархивированных дампов. Один из хостов в нашей сети используется для пересылки этой информации – он пересылает гораздо больше информации на внешние ресурсы в Интернете, чем остальные компьютеры нашей сети. Определите его IP-адрес.

``` r
leak <- dff  %>% select(src, dst, bytes) %>% filter(!str_detect(dst, '1[2-4].*')) %>% group_by(src) %>% summarise(bytes_amount = sum(bytes)) %>% arrange(desc(bytes_amount)) %>% collect()
leak %>% head(1)
```

    # A tibble: 1 × 2
      src          bytes_amount
      <chr>               <dbl>
    1 13.37.84.125   5765792351

### Ответ: 13.37.84.125

## Задание 2: Надите утечку данных 2

### Другой атакующий установил автоматическую задачу в системном планировщике cron для экспорта содержимого внутренней wiki системы. Эта система генерирует большое количество трафика в нерабочие часы, больше чем остальные хосты. Определите IP этой системы. Известно, что ее IP адрес отличается от нарушителя из предыдущей задачи.

### Поиск не рабочих часов

``` r
library(lubridate)
df_normaltime_by_traffic_size <- dff %>% select(timestamp, src, dst, bytes) %>% filter(!str_detect(dst, '1[2-4].*')) %>% mutate(timestamp = hour(as_datetime(timestamp/1000))) %>% group_by(timestamp) %>% summarize(traffic_size = sum(bytes)) %>% arrange(desc(traffic_size))
df_normaltime_by_traffic_size %>% collect() %>% print(n = Inf)
```

    # A tibble: 24 × 2
       timestamp traffic_size
           <int>        <dbl>
     1        18  60193966072
     2        23  60192411947
     3        21  60168340116
     4        16  60098320900
     5        20  60080805313
     6        17  60038805616
     7        22  60019583499
     8        19  59993406253
     9         7   2407989038
    10        12   2273682799
    11         3   2272781208
    12         6   2272628627
    13         0   2272231719
    14        13   2269391474
    15         8   2256895552
    16        15   2256892969
    17         9   2255747421
    18         5   2254830735
    19        14   2253404224
    20         2   2250935353
    21         4   2247503973
    22        10   2246424468
    23        11   2245261098
    24         1   2241313453

### Поиск IP

``` r
df_traffic_no_worktime_anomaly <- dff %>% select(timestamp, src, dst, bytes) %>% mutate(timestamp = hour(as_datetime(timestamp/1000))) %>% filter(!str_detect(dst, '1[2-4].*') & timestamp >= 0 & timestamp <= 15)  %>% group_by(src) %>% summarise(bytes_amount = sum(bytes)) %>% arrange(desc(bytes_amount)) %>% collect()
df_traffic_no_worktime_anomaly %>% filter(src != '13.37.84.125') %>% head(1)
```

    # A tibble: 1 × 2
      src         bytes_amount
      <chr>              <int>
    1 12.55.77.96    194447613

### Ответ: 12.55.77.96

## Задание 3: Надите утечку данных 3

### Еще один нарушитель собирает содержимое электронной почты и отправляет в Интернет используя порт, который обычно используется для другого типа трафика. Атакующий пересылает большое количество информации используя этот порт, которое нехарактерно для других хостов, использующих этот номер порта. Определите IP этой системы. Известно, что ее IP адрес отличается от нарушителей из предыдущих задач.

Найдем среднее значение трафика для каждого порта, и суммарный объем
трафика для каждого хоста по портам. Затем соединим две таблицы, и
найдем отношение максимального значения трафика по портам к среднему
значению объема трафику по порту

``` r
average_ports_traffic <- dff |> select(timestamp, src, dst, port, bytes) %>% filter(!str_detect(dst, '1[2-4].')) %>% group_by(src, port) %>% summarise(bytes_ip_port = sum(bytes)) %>% group_by(port) %>% summarise(average_port_traffic = mean(bytes_ip_port)) %>% arrange(desc(average_port_traffic)) |> collect()
```
    `summarise()` has grouped output by 'src'. You can override using the `.groups`
    argument.
``` r
max_ips_ports_traffic <- dff |> select(timestamp, src, dst, port, bytes) %>% filter(!str_detect(dst, '1[2-4].')) %>% group_by(src, port) %>% summarise(bytes_ip_port = sum(bytes)) %>% collect() %>% group_by(port) %>% top_n(1, bytes_ip_port) %>% arrange(desc(bytes_ip_port))
```
    `summarise()` has grouped output by 'src'. You can override using the `.groups`
    argument.
``` r
merged_df <- merge(max_ips_ports_traffic, average_ports_traffic, by = "port")

anomaly_ip_port_traffic <- merged_df %>% mutate(average_anomaly = bytes_ip_port/average_port_traffic) %>% arrange(desc(average_anomaly)) %>% head(1)
anomaly_ip_port_traffic
```
      port         src bytes_ip_port average_port_traffic average_anomaly
    1  124 12.30.96.87        281993             15641.06        18.02902

### Ответ: 12.30.96.87

## Задание 4: Обнаружение канала управления

### Зачастую в корпоротивных сетях находятся ранее зараженные системы, компрометация которых осталась незамеченной. Такие системы генерируют небольшое количество трафика для связи с панелью управления бот-сети, но с одинаковыми параметрами – в данном случае с одинаковым номером порта. Какой номер порта используется бот-панелью для управления ботами?

``` r
df2 <- dbGetQuery(con, "SELECT min(bytes),max(bytes),max(bytes) - min(bytes), avg(bytes), port,count(port) FROM data group by port having avg(bytes) - min(bytes) < 10 and min(bytes) != max(bytes)")
df2 %>% select(port)
```
      port
    1  124

## Задание 5: Обнаружение P2P трафика

### Иногда компрометация сети проявляется в нехарактерном трафике между хостами в локальной сети, который свидетельствует о горизонтальном перемещении (lateral movement). В нашей сети замечена система, которая ретранслирует по локальной сети полученные от панели управления бот-сети команды, создав таким образом внутреннюю пиринговую сеть. Какой уникальный порт используется этой бот сетью для внутреннего общения между собой?

``` r
df2 <- dbGetQuery(con, "SELECT min(bytes),max(bytes),max(bytes) - min(bytes) as anomaly, avg(bytes), port,count(port) FROM data where (src LIKE '12.%' or src LIKE '13.%' or src LIKE '14.%') and (dst LIKE '12.%' or dst LIKE '13.%' or dst LIKE '14.%') group by port order by anomaly asc limit 1")
df2 %>% select(port)
```
      port
    1  115

## Задание 6: Чемпион малвари

### Нашу сеть только что внесли в списки спам-ферм. Один из хостов сети получает множество команд от панели C&C, ретранслируя их внутри сети. В обычных условиях причин для такого активного взаимодействия внутри сети у данного хоста нет. Определите IP такого хоста.

``` r
df <- dbGetQuery(con, "SELECT src, dst FROM data")
max <- df |> filter(str_detect(src, '1[2-4].*')) |> filter(str_detect(dst, '1[2-4].*')) |> group_by(src) |> summarise(count = n()) |> arrange(desc(count)) |> slice(1) |> collect()
answer <- max |> select(src)
answer
```
    # A tibble: 1 × 1
      src        
      <chr>      
    1 13.42.70.40

## Задание 7: Скрытая бот-сеть

### В нашем трафике есть еще одна бот-сеть, которая использует очень большой интервал подключения к панели управления. Хосты этой продвинутой бот-сети не входят в уже обнаруженную нами бот-сеть. Какой порт используется продвинутой бот-сетью для коммуникации?

``` r
df2 <- dbGetQuery(con, "SELECT port, timestamp FROM data where timestamp == (select max(timestamp) from data)")
df2
```

      port    timestamp
    1   83 1.578784e+12

## Задание 8: Внутренний сканнер

### Одна из наших машин сканирует внутреннюю сеть. Что это за система?

``` r
df2 <- dbGetQuery(con, "SELECT src, AVG(timestamp) as time,count(DISTINCT dst) as count FROM data WHERE (src LIKE '12.%' OR src LIKE '13.%' OR src LIKE '14.%') AND (dst  LIKE '12.%' or dst  LIKE '13.%' or dst LIKE '14.%') group by src order by time")
answer <- df2 |> select(src) |> head(1)
answer
```
              src
    1 12.35.59.94

## Вывод

Научились использовать СУБД Clickhouse для обработки и анализ больших данных при помощьи языка программирования R