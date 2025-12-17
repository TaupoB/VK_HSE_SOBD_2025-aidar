В качестве исходных данных используется датасет [**MovieLens**](https://www.kaggle.com/datasets/grouplens/movielens-20m-dataset) (фильмы, рейтинги, теги).

## 1. Структура проекта

Для корректного запуска окружения организована следующая файловая структура:

```text
.
├── config                     # Переменные окружения для Hadoop/Hive
├── docker-compose.yml         # Оркестрация контейнеров
├── lib/                       # JDBC-драйвер (загружается отдельно)
├── data/                      # Исходные CSV-файлы датасета
└── scripts/                   # SQL-сценарии (HQL)
```

## 2. Подготовка

JDBC-драйвер `postgresql-42.7.3.jar` размещен в директории `lib/`.
```bash
mkdir -p lib
curl -o lib/postgresql-42.7.3.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
```
Файлы датасета размещены в папке `data/`

## 3. Развертывание и загрузка данных

### 3.1. Инициализация кластера
После старта требуется ожидание готовности HiveServer2 (1-2 минуты)

```bash
docker compose up -d
```

### 3.2. ETL-процесс (Host -> HDFS)
Данные переносятся с хост-машины в распределенную файловую систему. Создается структура директорий в HDFS, соответствующая таблицам Hive.

```bash
chmod +x load_data.sh
./load_data.sh
```

### 3.3. Создание схемы и витрин

Создание таблиц и витрин
```bash

docker cp ./scripts/. hive-server:/tmp/

docker exec -it hive-server beeline -n hive -u 'jdbc:hive2://hive-server:10000/' -f /tmp/db_creation.hql

docker exec -it hive-server beeline -n hive -u 'jdbc:hive2://hive-server:10000/' -f /tmp/make_views.hql
```

## 4. Описание аналитических витрин

На основе загруженных данных сформировано 5 витрин.

| Витрина | Описание |
| :--- | :--- |
| **`top_rated_movies`** | Cписок картин с рейтингом выше 4.0 и количеством голосов > 100. | 
| **`user_activity_yearly`** | Агрегирует количество оценок по годам.| 
| **`top_movie_tags`** | Для каждого фильма определяется eдинственный наиболее релевантный тег. |
| **`active_users_union`** | Формирует единый список уникальных ID пользователей, проявивших любую активность (оценка ИЛИ тег) |
| **`genre_analysis`** |  Сравнительный анализ (Комедии vs Драмы) по среднему рейтингу и количеству просмотров.|


Пример просмотра содержания витрины

```bash
docker exec -it hive-server beeline -n hive -u 'jdbc:hive2://hive-server:10000/' -e "SELECT * FROM movielens.genre_analysis;"
```