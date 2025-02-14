from typing import Any, Dict, List, Tuple
import psycopg2

from psycopg2 import sql


class DataBaseConn:

    def __init__(self, host: str, port: int, dbname: str, user: str, password: str):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )

    @staticmethod
    def _create_columns_str(columns: List[str]) -> str:
        s = ""
        for c in range(0, len(columns) - 1):
            s += f"{c},"
        s += str(columns[-1])
        return s

    @staticmethod
    def _create_conditions_str(conditions: Dict[str, Any]) -> Tuple[str, Tuple]:
        if len(conditions) == 0:
            return ("", ())
        else:
            s = "WHERE "
            values = list()
            condition_list = list(conditions.items())

            for i, t in enumerate(condition_list):
                col_name, col_value = t
                values.append(col_value)
                if i != len(condition_list) - 1:
                    s += f"{col_name} = %s and "
                else:
                    s += f"{col_name} = %s"
            return s, tuple(values)

    @staticmethod
    def _create_like_str(like: Dict[str, str]) -> Tuple[str, Any]:
        if len(like) == 0:
            return ("", None)
        else:
            col_name, col_value = next(iter(like.items()))
            s = f"{col_name} LIKE %s"
            return s, col_value

    def select(
        self,
        table: str,
        columns: List[str],
        conditions: Dict[str, Any],
        like_conditions: Dict[str, str],
        fetch: str = "fetchone",
    ):
        with self.conn:
            with self.conn.cursor() as cursor:
                # Insert the task into the table
                columns_str = DataBaseConn._create_columns_str(columns=columns)
                condition_str, vars = DataBaseConn._create_conditions_str(
                    conditions=conditions
                )
                if len(like_conditions) > 0:
                    like_cond_str, like_condition_var = DataBaseConn._create_like_str(
                        like=like_conditions
                    )
                    vars += (like_condition_var,)
                    select_query = sql.SQL(
                        f"SELECT {columns_str} FROM {table} {condition_str} AND {like_cond_str}"
                    )
                else:
                    select_query = sql.SQL(
                        f"SELECT {columns_str} FROM {table} {condition_str}"
                    )

                cursor.execute(query=select_query, vars=vars)
                if fetch == "fetchone":
                    rows = cursor.fetchone()
                elif fetch == "fetchall":
                    rows = cursor.fetchall()
                else:
                    raise ValueError(
                        f"Invalid parameter value: {fetch}. It must be either fetchone or fetchall"
                    )
                return rows

    def insert(self, table: str, values: Dict[str, Any]):
        with self.conn.cursor() as cursor:
            columns = "("
            values_str = "("
            vars = list()
            for i, t in enumerate(values.items()):
                col_name, col_value = t
                vars.append(col_value)
                if i == len(values.keys()) - 1:
                    columns += f"{col_name})"
                    values_str += "%s)"
                else:
                    columns += f"{col_name}, "
                    values_str += "%s, "
            # Insert the task into the table
            insert_query = sql.SQL(f"INSERT INTO {table} {columns} VALUES {values_str}")

            cursor.execute(insert_query, tuple(vars))

            # Commit the transaction
            self.conn.commit()
