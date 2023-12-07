import pymysql


def select_data():
    print("开始创建连接")
    connection = pymysql.connect(host="10.157.112.167",
                                 port=3306,
                                 user="oats_talentlink",
                                 password="Fo@tI%Vwc(AO",
                                 db="AEL",
                                 charset="utf8")
    cursor = connection.cursor()
    query = ("select count(*) from AEL.ODS_ADVISORY_TALENT_LINK where start_date>'2023-07-01' and client_code in ('06148335', '06148335', '02018375', '06147796')")
    cursor.execute(query)
    results = cursor.fetchall()
    for row in results:
        print(row)
    cursor.close()
    connection.close()


if __name__ == '__main__':
    select_data()
