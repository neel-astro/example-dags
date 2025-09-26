from airflow.decorators import dag, task


@task
def generate_data():
    print("XCom.__name__ --->", XCom.__name__)
    return [1, 2, 3]


@task
def filter_item_from_mapped_task(item):
    print("filter_item_from_mapped_task item --->", item)
    if item==1:
        return None
    return item


@task
def print_item(item):
    print("print_item item --->", item)


@dag(
    dag_id='test_bug_dag',
)
def test_bug_dag():
    data = generate_data()
    filtered_data = filter_item_from_mapped_task.expand(item=data)
    print_item.expand(item=filtered_data)

test_bug_dag()
