import os

def count_lines_in_folder(folder_path):
    total_lines = 0
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):  # 确保当前路径是文件而不是文件夹
            with open(file_path, 'r') as file:
                lines = len(file.readlines()) - 1
                total_lines += lines
    return total_lines

# 文件夹路径
folder_path = 'C:\\Users\\Austin J Cheng\\PycharmProjects\\pythonProject\\work_daily\\salesforce'


if __name__ == '__main__':
    # 调用函数统计行数
    line_count = count_lines_in_folder(folder_path)

    print("总行数:", line_count)