import sqlite3
import pandas as pd
import os
import tqdm

class SQLiteDumper:
    def __init__(self, db_path, save_dir):
         # 1. 连接到 SQLite 数据库
        self.conn = sqlite3.connect(db_path)  # 替换为你的数据库文件路径
        self.cursor = self.conn.cursor()
        self.save_dir = save_dir
    
    def close(self):
        self.cursor.close()
        self.conn.close()

    def dump2file(self, id, b3dm_path):
        b3dm_path = os.path.join(self.save_dir, b3dm_path)
        # 2. 查询表中的 ID 和 Data 列
        query = f"SELECT ID, Data FROM dataT WHERE ID={id};"  # 替换为你的表名
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        # 3. 遍历查询结果，写入文件
        for row in rows:
            #file_id = file_path  # ID
            file_data = row[1]  # Data (Blob)

            # 定义文件名（假设文件存储在当前目录下的 output 文件夹
            parent_path = os.path.dirname(b3dm_path)
            if not os.path.isdir(parent_path):
                os.makedirs(parent_path)

            # 将 Blob 数据写入文件
            with open(b3dm_path, 'wb') as file:
                file.write(file_data)
            # print(f"文件 {b3dm_path} 写入成功")
            break
        
        
def dump_model(db_dir, save_dir='output'):
    # 连接到数据库（若不存在则创建）
    conn = sqlite3.connect(db_dir + '/index.db')
    # 2. 定义 SQL 查询
    query = "SELECT * FROM indexT"

    # 3. 将查询结果加载到 Pandas DataFrame
    df = pd.read_sql_query(query, conn)

    # 4. 获取最后一列和倒数第二列的列名
    last_column = df.columns[-1]        # 最后一列
    second_last_column = df.columns[-2] # 倒数第二列
    # 5. 按最后一列升序、倒数第二列降序排序
    df_sorted = df.sort_values(by=[last_column, second_last_column], ascending=[True, False])
    conn.close()
    
    opened_db_dict = {}
    cur_dumper = None
    for index, row in tqdm.tqdm(df_sorted.iterrows(), total=len(df_sorted), desc="模型转换进度"):
        # print(f"索引: {index}, ID: {row['ID']}, Path: {row['Path']}, DataFile: {row['DataFile']}")
        db_path = os.path.join(db_dir, row['DataFile'])
        if os.path.isfile(db_path):
            if not db_path in opened_db_dict.keys():
                opened_db_dict[db_path] = 1
                if not cur_dumper is None:
                    cur_dumper.close()
                cur_dumper = SQLiteDumper(db_path, save_dir)
            # print(f"索引: {index}, ID: {row['ID']}, Path: {row['Path']}, DataFile: {row['DataFile']}")
            cur_dumper.dump2file(row['ID'], row['Path'])
            # break
        else:
            print("Warning: 数据库 ", db_path, " 不存在。")
            
def dump_all_models(model_dir):
    # 列出所有一级目录
    for item in os.listdir(model_dir):
        item_path = os.path.join(model_dir, item)
        if os.path.isdir(item_path):
            print(f"模型目录: {item_path}")
            dump_model(db_dir = os.path.join(item_path, 'sqlite'), save_dir= os.path.join(item_path, 'b3dm'))
    
    
if __name__ == '__main__':
    # 转换单模型
    # dump_model(db_path = 'sqlite/index.db', save_dir='output')
    
    # 转换所有模型
    dump_all_models('/Volumes/Data20T/三维模型/谷歌')