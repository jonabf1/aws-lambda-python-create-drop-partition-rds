from behave import given, when, then
import os

from src.handlers.lambda_handler import lambda_handler
from src.utils.partition_name_generator import PartitionNameGenerator
import pymysql
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dateutil.tz import tz


def get_database_connection():
    set_enviroments()
    return pymysql.connect(
        host=os.getenv('database_host'),
        user=os.getenv('database_user'),
        password=os.getenv('database_password'),
        database=os.getenv('database_name'),
        cursorclass=pymysql.cursors.DictCursor
    )


def set_enviroments():
    os.environ['database_host'] = 'localhost'
    os.environ['database_user'] = 'test_user'
    os.environ['database_password'] = 'test_password'
    os.environ['database_name'] = 'database_test_1'
    os.environ['application_name'] = 'application-partition_manager_test'
    os.environ['table_name'] = 'table_test_1'
    os.environ['maxvalue_partition_name'] = 'catch_all'


@given('conto a quantidade de partições existentes')
@when('conto a quantidade de partições existentes')
def step_count_partitions(context):
    with get_database_connection().cursor() as cursor:
        cursor.execute(f"""
            SELECT COUNT(*) as count 
            FROM information_schema.PARTITIONS 
            WHERE TABLE_SCHEMA = '{os.getenv('database_name')}' 
              AND TABLE_NAME = '{os.getenv('table_name')}'
        """)
        context.partition_count = cursor.fetchone()['count']


@then('conto a quantidade de itens existentes na tabela')
@when('conto a quantidade de itens existentes na tabela')
def step_count_items(context):
    with get_database_connection().cursor() as cursor:
        cursor.execute(f"""
            SELECT COUNT(*) as total_items 
            FROM {os.getenv('database_name')}.{os.getenv('table_name')} 
        """)
        context.items_count = cursor.fetchone()['total_items']


@when('verificar se a quantidade de itens na tabela permanece a mesma')
@then('verificar se a quantidade de itens na tabela permanece a mesma')
def step_verify_count_items(context):
    with get_database_connection().cursor() as cursor:
        cursor.execute(f"""
            SELECT COUNT(*) as total_items 
            FROM {os.getenv('database_name')}.{os.getenv('table_name')} 
        """)
        new_total_items = cursor.fetchone()['total_items']
        assert ((context.items_count - getattr(context, 'items_partition_dropped_count', 0)) + getattr(context,
                                                                                                       'new_items_added',
                                                                                                       0)) == new_total_items


@then('valido que a partição maxvalue "{particao}" não contém itens')
@when('valido que a partição maxvalue "{particao}" não contém itens')
def step_validate_maxvalue_empty(context, particao):
    with get_database_connection().cursor() as cursor:
        cursor.execute(f"""
            SELECT COUNT(*) as count 
            FROM {os.getenv('database_name')}.{os.getenv('table_name')} 
            PARTITION ({particao})
        """)
        count_result = cursor.fetchone()
        assert count_result['count'] == 0


@when('crio "{quantidade}" partição futura')
@then('crio "{quantidade}" partição futura')
def step_create_future_partitions(context, quantidade):
    os.environ['enable_drop'] = 'false'
    os.environ['enable_create'] = 'true'
    os.environ['future_partition_months'] = quantidade
    os.environ['drop_month_back'] = '1'
    lambda_handler("event", "context")


@then('valido que as partições existam')
def step_validate_partitions_exist(context):
    future_months = int(os.environ.get('future_partition_months'))
    for month in range(1, future_months + 1):
        partition_date = datetime.now(tz.gettz('America/Sao_Paulo')) + relativedelta(months=month)
        partition_name = PartitionNameGenerator.generate_partition_name(partition_date)
        with get_database_connection().cursor() as cursor:
            cursor.execute(f"""
                SELECT PARTITION_NAME 
                FROM information_schema.PARTITIONS 
                WHERE TABLE_SCHEMA = '{os.getenv('database_name')}' 
                  AND TABLE_NAME = '{os.getenv('table_name')}' 
                  AND PARTITION_NAME = '{partition_name}'
            """)
            assert cursor.fetchone() is not None


@then('valido que a quantidade de partições é maior agora')
def step_validate_partition_count_increased(context):
    with get_database_connection().cursor() as cursor:
        cursor.execute(f"""
            SELECT COUNT(*) as count 
            FROM information_schema.PARTITIONS 
            WHERE TABLE_SCHEMA = '{os.getenv('database_name')}' 
              AND TABLE_NAME = '{os.getenv('table_name')}'
        """)
        new_partition_count = cursor.fetchone()['count']
        assert new_partition_count > context.partition_count


@when('insiro itens com datas "{anos}" anos no futuro')
@then('insiro itens com datas "{anos}" anos no futuro')
def step_insert_many_items(context, anos):
    anos = int(anos)
    connection = get_database_connection()
    with connection.cursor() as cursor:
        counter = 0
        start_date = datetime.now()
        end_date = start_date + relativedelta(years=anos)
        while start_date < end_date:
            counter += 1
            cursor.execute(f"""
                INSERT INTO {os.getenv('database_name')}.{os.getenv('table_name')} (created_at) 
                VALUES ('{start_date.strftime('%Y-%m-%d')}')
            """)
            start_date += timedelta(days=1)  # Incrementa 1 dia
        connection.commit()
        context.new_items_added = counter


@when('insiro itens com datas "{quantidade}" meses no futuro')
@then('insiro itens com datas "{quantidade}" meses no futuro')
def step_insert_many_items(context, quantidade):
    meses = int(quantidade)
    connection = get_database_connection()
    with connection.cursor() as cursor:
        counter = 0
        start_date = datetime.now()
        end_date = start_date + relativedelta(months=meses)
        while start_date < end_date:
            counter += 1
            cursor.execute(f"""
                INSERT INTO {os.getenv('database_name')}.{os.getenv('table_name')} (created_at) 
                VALUES ('{start_date.strftime('%Y-%m-%d')}')
            """)
            start_date += timedelta(days=1)  # Incrementa 1 dia
        connection.commit()
        context.new_items_added = counter


@then('verifico se o campo min e max da chave de particionamento da particao criada está dentro do range aceitavel')
@when('verifico se o campo min e max da chave de particionamento da particao criada está dentro do range aceitavel')
def step_validate_partition_range(context):
    with get_database_connection().cursor() as cursor:
        for month in range(1, int(os.environ.get('future_partition_months')) + 1):
            partition_date = datetime.now() + relativedelta(months=month)
            partition_name = PartitionNameGenerator.generate_partition_name(partition_date)
            cursor.execute(f"""
                SELECT MIN(created_at) as min_date, MAX(created_at) as max_date 
                FROM {os.getenv('database_name')}.{os.getenv('table_name')}
                PARTITION ({partition_name})
            """)
            result = cursor.fetchone()
            min_date = result['min_date']
            max_date = result['max_date']
            assert partition_date.month == min_date.month and partition_date.month == max_date.month


@when('faço drop de uma partição passada usando o valor "{valor}" como indexador passado')
@then('faço drop de uma partição passada usando o valor "{valor}" como indexador passado')
def step_drop_partition(context, valor):
    os.environ['enable_drop'] = 'true'
    os.environ['enable_create'] = 'false'
    os.environ['future_partition_months'] = '1'
    os.environ['drop_month_back'] = valor

    partition_date = datetime.now() - relativedelta(months=int(valor))
    partition_name = PartitionNameGenerator.generate_partition_name(partition_date)

    with get_database_connection().cursor() as cursor:
        cursor.execute(f"""
            SELECT COUNT(*) as total_items 
            FROM {os.getenv('database_name')}.{os.getenv('table_name')} PARTITION ({partition_name})
        """)
        context.items_partition_dropped_count = cursor.fetchone()['total_items']

    lambda_handler("event", "context")


@then('valido que a partição não existe mais')
@when('valido que a partição não existe mais')
def step_validate_partition_does_not_exist(context):
    drop_month_back = int(os.environ.get('drop_month_back'))
    partition_date = datetime.now() - relativedelta(months=drop_month_back)
    partition_name = PartitionNameGenerator.generate_partition_name(partition_date)
    with get_database_connection().cursor() as cursor:
        cursor.execute(f"""
            SELECT PARTITION_NAME 
            FROM information_schema.PARTITIONS 
            WHERE TABLE_SCHEMA = '{os.getenv('database_name')}' 
              AND TABLE_NAME = '{os.getenv('table_name')}' 
              AND PARTITION_NAME = '{partition_name}'
        """)
        assert cursor.fetchone() is None


@then('valido que a partição maxvalue "{particao}" contém itens')
@when('valido que a partição maxvalue "{particao}" contém itens')
def step_validate_maxvalue_contains_items(context, particao):
    with get_database_connection().cursor() as cursor:
        cursor.execute(f"""
            SELECT COUNT(*) as count 
            FROM {os.getenv('database_name')}.{os.getenv('table_name')} 
            PARTITION ({particao})
        """)
        count_result = cursor.fetchone()
        assert count_result['count'] > 0


@when('valido que a(s) partição atual contém itens')
@then('valido que a(s) partição atual contém itens')
def step_validate_partition_contains_itens(context):
    future_months = int(os.environ.get('future_partition_months'))
    for month in range(1, future_months + 1):
        partition_date = datetime.now(tz.gettz('America/Sao_Paulo')) + relativedelta(months=month)
        partition_name = PartitionNameGenerator.generate_partition_name(partition_date)
        with get_database_connection().cursor() as cursor:
            cursor.execute(f"""
                SELECT COUNT(*) as count 
                FROM {os.getenv('database_name')}.{os.getenv('table_name')} 
                PARTITION ({partition_name})
            """)
            count_result = cursor.fetchone()
            assert count_result['count'] > 0


@when('crio "{quantidade}" partição futura e espero que ocorra um erro')
@then('crio "{quantidade}" partição futura e espero que ocorra um erro')
def step_create_future_partitions(context, quantidade):
    os.environ['enable_drop'] = 'false'
    os.environ['enable_create'] = 'true'
    os.environ['future_partition_months'] = quantidade
    os.environ['drop_month_back'] = '1'

    try:
        lambda_handler("event", "context")
    except Exception as e:
        print(e)


@then('valido que a particao existe')
def step_validate_exist_partition(context):
    partition_date = datetime.now() + relativedelta(months=int(os.environ.get('future_partition_months')))
    partition_name = PartitionNameGenerator.generate_partition_name(partition_date)
    with get_database_connection().cursor() as cursor:
        cursor.execute(f"""
            SELECT PARTITION_NAME 
            FROM information_schema.PARTITIONS 
            WHERE TABLE_SCHEMA = '{os.getenv('database_name')}' 
              AND TABLE_NAME = '{os.getenv('table_name')}' 
              AND PARTITION_NAME = '{partition_name}'
        """)
        assert cursor.fetchone() is not None


@then('valido que a quantidade de partições é menor agora')
@when('valido que a quantidade de partições é menor agora')
def step_validate_quantity_is_minor(context):
    with get_database_connection().cursor() as cursor:
        cursor.execute(f"""
            SELECT COUNT(*) as count 
            FROM information_schema.PARTITIONS 
            WHERE TABLE_SCHEMA = '{os.getenv('database_name')}' 
              AND TABLE_NAME = '{os.getenv('table_name')}'
        """)
        new_partition_count = cursor.fetchone()['count']
        assert new_partition_count < context.partition_count


@then('que o numero de partições não tenha alterado')
def step_validate_quantity_is_minor(context):
    with get_database_connection().cursor() as cursor:
        cursor.execute(f"""
            SELECT COUNT(*) as count 
            FROM information_schema.PARTITIONS 
            WHERE TABLE_SCHEMA = '{os.getenv('database_name')}' 
              AND TABLE_NAME = '{os.getenv('table_name')}'
        """)
        new_partition_count = cursor.fetchone()['count']
        assert new_partition_count == context.partition_count