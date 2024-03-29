Лабораторная работа №3
======================
Сысоев Г.А.

## Цель Работы

1.  Изучить возможности технологии Apache Arrow для обработки и анализ больших данных
2.  Получить навыки применения Arrow совместно с языком программирования R
3.  Получить навыки анализа метаинфомации о сетевом трафике
4.  Получить навыки применения облачных технологий хранения, подготовки и анализа данных: Yandex Object Storage, Rstudio Server.

## Ход работы

## Подключение arrow

```{r}
library('dplyr')
library('tidyverse')
library('arrow')
curl::multi_download(
    "https://storage.yandexcloud.net/arrow-datasets/tm_data.pqt",
     "data/tm_data.pqt",
  resume = TRUE
)
df <- arrow::open_dataset('data/tm_data.pqt')
```

## Задание 1

### Найдите утечку данных из Вашей сети

Важнейшие документы с результатами нашей исследовательской деятельности в
области создания вакцин скачиваются в виде больших заархивированных дампов.
Один из хостов в нашей сети используется для пересылки этой информации – он
пересылает гораздо больше информации на внешние ресурсы в Интернете, чем
остальные компьютеры нашей сети. Определите его IP-адрес.

```{r}
out_traffic <- df  %>% select(src, dst, bytes) %>% filter(!str_detect(dst, '1[2-4].*')) %>% group_by(src) %>% summarise(bytes_amount = sum(bytes)) %>% arrange(desc(bytes_amount)) %>% collect()
out_traffic %>% head(1)
```
### Ответ: 13.37.84.125

## Задание 2

### Найдите утечку данных 2

Другой атакующий установил автоматическую задачу в системном планировщике
cron для экспорта содержимого внутренней wiki системы. Эта система генерирует
большое количество трафика в нерабочие часы, больше чем остальные хосты.
Определите IP этой системы. Известно, что ее IP адрес отличается от нарушителя из
предыдущей задачи.

### Поиск не рабочих часов

```{r}
library(lubridate)
df_normaltime_by_traffic_size <- df %>% select(timestamp, src, dst, bytes) %>% filter(!str_detect(dst, '1[2-4].*')) %>% mutate(timestamp = hour(as_datetime(timestamp/1000))) %>% group_by(timestamp) %>% summarize(traffic_size = sum(bytes)) %>% arrange(desc(traffic_size))
df_normaltime_by_traffic_size %>% collect() %>% print(n = Inf)
```
### Поиск IP

```{R}
df_traffic_no_worktime_anomaly <- df %>% select(timestamp, src, dst, bytes) %>% mutate(timestamp = hour(as_datetime(timestamp/1000))) %>% filter(!str_detect(dst, '1[2-4].*') & timestamp >= 0 & timestamp <= 15)  %>% group_by(src) %>% summarise(bytes_amount = sum(bytes)) %>% arrange(desc(bytes_amount)) %>% collect()
df_traffic_no_worktime_anomaly %>% filter(src != '13.37.84.125') %>% head(1)
```

### Ответ: 12.55.77.96

## Задание 3

### Найдите утечку данных 3
Еще один нарушитель собирает содержимое электронной почты и отправляет в
Интернет используя порт, который обычно используется для другого типа трафика.
Атакующий пересылает большое количество информации используя этот порт,
которое нехарактерно для других хостов, использующих этот номер порта.
Определите IP этой системы. Известно, что ее IP адрес отличается от нарушителей
из предыдущих задач.

Логика поиска злоумышленника в том чтобы найти наиболее отличное значение переданного объёма данных, от средних значений для портов. 
1) Найдём среднее значение трафика для каждого порта назначения во внешней сети(Интернет). Среднее - это объём трафика делённый на колличество хостов, которые отправляли данные на порт. 
2) Найдём суммарный объём трафика для каждого хоста по портам, затем определим максимальные значения отправляемого трафика для каждого порта по сети(в Интернет).
3) Матчим две таблицы: средние значения трафика по портам и максимальные значения объёма трафика отправленные по этим портам с хостов.
4) Находим отношение Максимального значения трафика с хоста по порту к среднему значению объёма трафика по порту. Аномальное значение одно - 12.30.96.87:124 c k = 18.5, т.е. объём переданного трафика с ip 12.30.96.87 по порту 124 на внешний ресурс в 18.5 раз больше среднего трафика идущего из внутренней сети на порт 124.
```{r}

average_ports_traffic <- df |> select(timestamp, src, dst, port, bytes) |> filter(!str_detect(dst, '1[2-4].')) |> group_by(src, port) |> summarise(bytes_ip_port = sum(bytes)) |> group_by(port) |> summarise(average_port_traffic = mean(bytes_ip_port)) |> arrange(desc(average_port_traffic)) |> collect()

max_ips_ports_traffic <- df |> select(timestamp, src, dst, port, bytes) |> filter(!str_detect(dst, '1[2-4].')) |> group_by(src, port) |> summarise(bytes_ip_port = sum(bytes)) |> collect() |> group_by(port) |> top_n(1, bytes_ip_port) |> arrange(desc(bytes_ip_port))

merged_df <- merge(max_ips_ports_traffic, average_ports_traffic, by = "port")

anomaly_ip_port_traffic <- merged_df |> mutate(average_anomaly = bytes_ip_port/average_port_traffic) |> arrange(desc(average_anomaly)) |> head(1)
anomaly_ip_port_traffic
```
### Ответ: 12.30.96.87

## Вывод

Научился пользоваться технологией Apache Arrow для обработки и анализа больших данных
