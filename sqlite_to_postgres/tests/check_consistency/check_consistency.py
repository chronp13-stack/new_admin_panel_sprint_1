import sqlite3

# import psycopg
from datetime import datetime
from psycopg.rows import dict_row


def test_migration_integrity(sqlite_conn, pg_conn, tables_map):
    # Настраиваем SQLite на выдачу Row
    sqlite_conn.row_factory = sqlite3.Row
    sqlite_cur = sqlite_conn.cursor()

    # Используем dict_row только для этого курсора, чтобы не ломать saver
    with pg_conn.cursor(row_factory=dict_row) as pg_cur:

        for table_name, mapping in tables_map.items():
            # Печатаем название таблицы в начале проверки
            print(f"  -> Проверка таблицы '{table_name}':", end=" ", flush=True)

            # 1. Сверяем количество
            sqlite_cur.execute(f"SELECT count(*) as cnt FROM {table_name}")
            s_cnt = sqlite_cur.fetchone()["cnt"]

            pg_cur.execute(f"SELECT count(*) as cnt FROM content.{table_name}")
            p_cnt = pg_cur.fetchone()["cnt"]

            assert (
                p_cnt <= s_cnt
            ), f"Ошибка! В Postgres больше строк ({p_cnt}), чем в SQLite ({s_cnt})"

            # 2. Сверяем данные (берем первые 100 записей для скорости или все)
            pg_columns = ", ".join(mapping.keys())
            pg_cur.execute(f"SELECT {pg_columns} FROM content.{table_name}")

            rows_checked = 0
            for pg_row in pg_cur:
                sqlite_id_col = mapping["id"]
                sqlite_cols = ", ".join(mapping.values())

                sqlite_cur.execute(
                    f"SELECT {sqlite_cols} FROM {table_name} WHERE {sqlite_id_col} = ?",
                    (str(pg_row["id"]),),
                )
                sqlite_row = sqlite_cur.fetchone()

                assert sqlite_row is not None, f"ID {pg_row['id']} не найден в SQLite"

                # Глубокая сверка полей
                for pg_f, sq_f in mapping.items():
                    pg_val = pg_row[pg_f]
                    sq_val = sqlite_row[sq_f]

                    # Если это поля даты (created или modified), нормализуем их
                    if pg_f in ("created", "modified"):
                        # Приводим значение из Postgres к строке без часового пояса
                        if isinstance(pg_val, datetime):
                            pg_val = pg_val.replace(tzinfo=None).isoformat(
                                sep=" ", timespec="seconds"
                            )

                        # Убеждаемся, что значение из SQLite тоже в похожем формате
                        # (SQLite иногда хранит миллисекунды, отрезаем их если нужно)
                        sq_val = str(sq_val).split(".")[0]
                        pg_val = str(pg_val).split(".")[0]

                    assert str(pg_val) == str(
                        sq_val
                    ), f"Различие в поле {pg_f}. PG: {pg_val}, SQ: {sq_val}"

                rows_checked += 1

            print(f"OK (проверено {rows_checked} строк) ✅")

    print("\n[!] Проверка всех таблиц завершена успешно.")
    