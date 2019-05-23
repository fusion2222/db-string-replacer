import time

from sqlalchemy import create_engine, text, table, column, select
from sqlalchemy.orm import sessionmaker

import settings


Session = sessionmaker(bind=settings.engine)


def _create_search_condition(field_names, value_to_replace):
    condition = []

    for field_name in field_names:
        condition.append(
            "{field_name} LIKE '%{value_to_replace}%'".format(
                field_name=field_name, value_to_replace=value_to_replace
            )
        )

    return " OR ".join(condition)


def _replace_values(value_to_replace, new_value, field_names, table_name, total_count, search_condition):
    session = Session()
    # If at least one field contains `value_to_replace`, we want to retrieve it from DB.

    total_updated = 0

    # Primary key does not have to be named `id`. So instead we take the first field name.
    # This can potentially cause havoc. TODO: Make command switch, so user can specify this.

    sql_key_field_name = field_names[0]

    while total_updated < total_count:
        # TODO: Make graphical loadbar
        print('\n{} items out of {} is updated.'.format(total_updated, total_count))

        sql = "SELECT * FROM {table_name} WHERE {condition} ORDER BY 'id' LIMIT {limit} OFFSET {offset}".format(
            table_name=table_name,
            condition=search_condition,
            limit=settings.CHUNK_SIZE,
            offset=total_updated,
        )

        result = settings.engine.execute(text(sql))

        rows = result.fetchall()

        for row in rows:
            update_kwargs = {}

            for field_index, field_name in enumerate(field_names):
                if sql_key_field_name == field_name:
                    row_id = row[0]
                    continue



                try:
                    # In case of datetime objects, or non-strings
                    if value_to_replace in row[field_index]:
                        update_kwargs[field_name] = row[field_index].replace(value_to_replace, new_value)
                except TypeError:
                    continue

            # Update row now..
            condition = '{}={}'.format(sql_key_field_name, row_id)
            update_fields = ', '.join([
                "{}='{}'".format(field_name, field_value) for field_name, field_value in update_kwargs.items()
            ])

            sql = "UPDATE {table_name} SET {update_fields} WHERE {condition}".format(
                table_name=table_name,
                condition=condition,
                update_fields=update_fields
            )

            settings.engine.execute(text(sql))
            total_updated += 1

        session.commit()
        time.sleep(settings.CHUNK_SLEEP_TIME)


def main():
    value_to_replace = None
    new_value = None

    while not value_to_replace or not new_value:
        value_to_replace = input('Enter value you want to be replaced across every table in your DB: ')
        new_value = input('Enter new value you want to place in DB instead of old value: ')

    table_names = settings.engine.table_names()

    for table_name in table_names:

        session = Session()

        sql = text("SELECT * FROM {} LIMIT 0".format(table_name))
        table_cols_result = settings.engine.execute(sql)
        column_names = table_cols_result.keys()
        session.commit()

        search_condition = _create_search_condition(
            field_names=column_names,
            value_to_replace=value_to_replace,
        )

        # TODO: Query above should display matches, not total count of all, even non-matching items.
        count_sql = text("SELECT COUNT(*) FROM {table_name} WHERE {condition}".format(
            table_name=table_name, condition=search_condition)
        )

        table_cols_result = settings.engine.execute(count_sql)
        total_count = table_cols_result.first()[0]
        session.commit()

        _replace_values(
            value_to_replace=value_to_replace,
            new_value=new_value,
            field_names=column_names,
            table_name=table_name,
            total_count=total_count,
            search_condition=search_condition
        )

    return 0


if __name__ == '__main__':
    main()
