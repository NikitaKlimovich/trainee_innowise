from task1 import Info,Config,Connection,DatabaseObject,FilePath


def main():
    args = Info()
    info = args.get_info_from_path()
    if info:
        config = Config()
        if config:
            conn = Connection(config)
            conn.get_conn()
            if conn:
                sql_folder = config.get_sql_folder()
                cur = DatabaseObject(conn,sql_folder)
                cur.create()
                cur.insert(info)
                conn.commit_changes()
                cur_with_names = DatabaseObject(conn,sql_folder,True)
                res_info = cur_with_names.get()
                conn.close_conn()
                write_folder = config.get_write_folder()
                output_format = args.get_output_format()
                write_folder_full = '//'.join([write_folder,output_format])
                write_object = FilePath(write_folder_full,output_format)
                write_object.write(res_info)


if __name__ == '__main__':
    main()