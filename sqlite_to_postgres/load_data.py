import sqlite3
from dataclasses import dataclass, asdict
from typing import Optional, List, Generator

import psycopg
from psycopg import ClientCursor, connection as _connection
from psycopg.rows import dict_row
from tests.check_consistency.check_consistency import test_migration_integrity

# =========================
# Dataclasses
# =========================


@dataclass
class Genre:
    id: str
    name: str
    description: Optional[str]
    created: str
    modified: str


@dataclass
class FilmWork:
    id: str
    title: str
    description: Optional[str]
    creation_date: Optional[str]
    rating: Optional[float]
    type: str
    created: str
    modified: str


@dataclass
class Person:
    id: str
    full_name: str
    created: str
    modified: str


@dataclass
class GenreFilmWork:
    id: str
    genre_id: str
    film_work_id: str
    created: str


@dataclass
class PersonFilmWork:
    id: str
    person_id: str
    film_work_id: str
    role: str
    created: str


# =========================
# SQLite Loader
# =========================


class SQLiteLoader:
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
        self.conn.row_factory = sqlite3.Row

    def load_data(
        self, table: str, columns: list, batch_size: int
    ) -> Generator[List[sqlite3.Row], None, None]:
        cursor = self.conn.cursor()
        # cursor.execute(f"SELECT * FROM {table}")
        query_columns = ", ".join(columns)
        cursor.execute(f"SELECT {query_columns} FROM {table}")

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield rows


# =========================
# Postgres Saver
# =========================


class PostgresSaver:
    def __init__(self, conn: _connection):
        self.conn = conn

    def save_batch(self, table: str, columns: List[str], rows: List[dict]):
        cols = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])

        # По умолчанию конфликтуем по id
        conflict_target = "id"

        # Для таблицы связей конфликтуем по паре полей, на которых висит индекс
        if table == "person_film_work":
            conflict_target = "film_work_id, person_id, role"
        elif table == "genre_film_work":
            conflict_target = "film_work_id, genre_id"

        query = f"""
            INSERT INTO content.{table} ({cols})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_target}) DO NOTHING;
        """

        try:
            with self.conn.cursor() as cur:
                cur.executemany(query, rows)
        except Exception as e:
            print(f"Ошибка записи в {table}: {e}")
            raise


# =========================
# Основная логика
# =========================


def transform_data(
    table: str, rows: List[sqlite3.Row], field_mapping: dict
) -> List[dict]:
    """Преобразование в dataclass и обратно в dict"""
    result = []
    rename_map = {v: k for k, v in field_mapping.items()}

    for row in rows:
        data = {rename_map.get(key, key): value for key, value in dict(row).items()}
        # for sqlite_col, value in dict(row).items():
        #    pg_col = rename_map.get(sqlite_col, sqlite_col)
        #    data[pg_col] = value

        if table == "genre":
            obj = Genre(**data)
        elif table == "film_work":
            obj = FilmWork(**data)
        elif table == "person":
            obj = Person(**data)
        elif table == "genre_film_work":
            obj = GenreFilmWork(**data)
        elif table == "person_film_work":
            obj = PersonFilmWork(**data)
        else:
            continue

        result.append(asdict(obj))

    return result


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""

    loader = SQLiteLoader(connection)
    saver = PostgresSaver(pg_conn)

    batch_size = 100

    TABLES_MAP = {
        "genre": {
            "id": "id",
            "name": "name",
            "description": "description",
            "created": "created_at",  # Postgres: SQLite
            "modified": "updated_at",
        },
        "person": {
            "id": "id",
            "full_name": "full_name",
            "created": "created_at",
            "modified": "updated_at",
        },
        "film_work": {
            "id": "id",
            "title": "title",
            "description": "description",
            "creation_date": "creation_date",
            "rating": "rating",
            "type": "type",
            "created": "created_at",
            "modified": "updated_at",
        },
        "genre_film_work": {
            "id": "id",
            "genre_id": "genre_id",
            "film_work_id": "film_work_id",
            "created": "created_at",
        },
        "person_film_work": {
            "id": "id",
            "person_id": "person_id",
            "film_work_id": "film_work_id",
            "role": "role",
            "created": "created_at",
        },
    }

    for table, field_mapping in TABLES_MAP.items():
        print(f"Загрузка таблицы: {table}")
        # Список колонок для SQL-запроса к SQLite
        sqlite_columns = list(field_mapping.values())
        # Список колонок для INSERT-запроса в Postgres
        pg_columns = list(field_mapping.keys())
        try:
            for batch in loader.load_data(table, sqlite_columns, batch_size):
                transformed = transform_data(table, batch, field_mapping)
                saver.save_batch(table, pg_columns, transformed)
            pg_conn.commit()

        except Exception as e:
            print(f"Ошибка при обработке таблицы {table}: {e}")
            pg_conn.rollback()

    # --- ЗАПУСК ТЕСТА ПОСЛЕ ВСЕХ ТАБЛИЦ ---
    print("\n--- Запуск проверки целостности данных ---")
    try:
        test_migration_integrity(connection, pg_conn, TABLES_MAP)
        print("Миграция завершена успешно и проверена!")
    except AssertionError as e:
        print(f"Тест не пройден: {e}")


# =========================
# Точка входа
# =========================

if __name__ == "__main__":
    dsl = {
        "dbname": "movies_database",
        "user": "app",
        "password": "123qwe",
        "host": "127.0.0.1",
        "port": 5432,
    }

    with sqlite3.connect("db.sqlite") as sqlite_conn, psycopg.connect(
        **dsl, row_factory=dict_row, cursor_factory=ClientCursor
    ) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
        print("\n--- Запуск тестов ---")
