import sqlite3


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except sqlite3.Error as e:
        print(e)

    return conn

    # finally:
    #     if conn:
    #         conn.close()


def create_connection_in_memory():
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(":memory:")
        print(sqlite3.version)
    except sqlite3.Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


def create_table(conn: sqlite3.Connection, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except sqlite3.Error as e:
        print(e)


def add_row (conn, row, table, column_headings):

    #columns = len(row)

    try:
        sql = f''' INSERT INTO {table}({",".join(column_headings)})
                VALUES({",".join("?" for i in column_headings)}) '''
        cur = conn.cursor()
        cur.execute(sql, row)
        conn.commit()
    except Exception as e:
        print(sql)
        raise e
    return cur.lastrowid


def create_project(conn, project):
    """
    Create a new project into the projects table
    :param conn:
    :param project:
    :return: project id
    """

    headings = ("name","begin_date","end_date")

    return add_row(conn, project, "projects", headings)


def create_task(conn, task):
    """
    Create a new task
    :param conn:
    :param task:
    :return:
    """

    headings = ("name","priority","status_id","project_id","begin_date","end_date")

    return add_row(conn, task, "tasks", headings)


if __name__ == '__main__':

    database = r"pythonsqlite.db"

    sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS projects (
                                        id integer PRIMARY KEY,
                                        name text NOT NULL,
                                        begin_date text,
                                        end_date text
                                    ); """

    sql_create_tasks_table = """CREATE TABLE IF NOT EXISTS tasks (
                                id integer PRIMARY KEY,
                                name text NOT NULL,
                                priority integer,
                                status_id integer NOT NULL,
                                project_id integer NOT NULL,
                                begin_date text NOT NULL,
                                end_date text NOT NULL,
                                FOREIGN KEY (project_id) REFERENCES projects (id)
                            );"""
    
    database_connection = None
    try:
        database_connection = create_connection(database)
        
        create_table(database_connection, sql_create_projects_table)
        create_table(database_connection, sql_create_tasks_table)
    
        project = ('Cool App with SQLite & Python', '2015-01-01', '2015-01-30')
        project_id = create_project(database_connection, project)

        task_1 = ('Analyze the requirements of the app', 1, 1, project_id, '2015-01-01', '2015-01-02')
        task_2 = ('Confirm with user about the top requirements', 1, 1, project_id, '2015-01-03', '2015-01-05')

        task_id = create_task(database_connection, task_1)
        task_id = create_task(database_connection, task_2)

    finally:
        if database_connection:
            database_connection.close()


    #create_connection_in_memory()