import pandas as pd


def change(data_frame, wk_id):
    data_frame.loc[df['ID'] == wk_id, 'describe'] = 'Bob is chinese'

# 创建一个示例 DataFrame
data = {'ID': [1, 2, 3, 4],
        'Name': ['Alice', 'Bob', 'Charlie', 'David'],
        'describe': ['Alice is woman', 'Bob is man', 'Charlie is woman', 'David is man']}
df = pd.DataFrame(data)
change(df, 2)

# 打印原始 DataFrame
print("Original DataFrame:")
print(df)
