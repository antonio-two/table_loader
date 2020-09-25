import os
import os.path


def main():
    """
    https://docs.python.org/3/library/os.html
    :return:
    """

    projects = os.path.join(os.getcwd(), "projects")
    tables = dict()

    for root, dirs, files in os.walk(projects):
        if not files:
            continue

        project = os.path.basename(os.path.dirname(root))
        dataset = os.path.basename(root)

        for file in files:
            file_name = file.partition('.')[0]
            table_key = f'{project}.{dataset}.{file_name}'
            file_path = f'{root}/{file}'
            tables.setdefault(table_key, []).append(file_path)

        # expected format {"project.dataset.table":["path/to/schema", "path/to/data"]}
        print(tables)


if __name__ == '__main__':
    main()
