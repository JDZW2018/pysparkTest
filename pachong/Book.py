from urllib import request
import re #正则表达式去除网页标签
# from fake_useragent import UserAgent
from bs4 import BeautifulSoup as bs   # 其中bs为BeautifulSoup的别名
import xlwt

bname_list = []#书名列表
author_list = []#作者
grade_list = []#评分
click_list = []#点击量

#正则表达式去除网页标签
def RemoveLabel(html):
    pre=re.compile(('>(.*)<'))
    return''.join(pre.findall(html))


def crawler(index,x):
    # 爬取数据
    url = "http://www.en8848.com.cn/fiction/fiction/gerneral/"+index

    headers = {
        # "USER-Agent":UserAgent(verify_ssl=False).random
    }
    req = request.Request(url, headers=headers)
    resp = request.urlopen(req)
    html_data = resp.read().decode()#整个网页

    soup = bs(html_data, "html.parser")  # 构建一个解析器

    nowplay = soup.find_all("div", class_="yd-book-item yd-book-item-pull-left")

    global bname_list,author_list,grade_list,click_list
    n=0
    for item in nowplay:
        bname_dict = []
        author_dict = []
        grade_dict = []
        click_dict = []
        IsGrade = True#评分和点击量的标签相同，用来做判断
        match="-"#加入日期的标签与评分和点击量相同，但是不需要，所以做筛选
        n = n + 1
        for name_item in item.find_all('h2'):
            if (x == 1):
                if (n >= 31):
                    break
            else:
                if (n >= 14):
                    break
            bname_dict = RemoveLabel(str(name_item))
            bname_list.append(bname_dict)

        for dl_item in item.find_all('div', class_="author-container"):
            if (x == 1):
                if (n >= 31):
                    break
            else:
                if (n >= 14):
                    break
            for author in dl_item.find_all("dd"):
                author_dict = RemoveLabel(str(author))
                author_list.append(author_dict)

        for dd_item in item.find_all('div', class_="price-container"):
            if (x == 1):
                if (n >= 31):
                    break
            else:
                if (n >= 14):
                    break
            for price in dd_item.find_all("dd"):
                if (IsGrade):
                    grade_dict = RemoveLabel(str(price))
                    if(match in grade_dict):
                        pass
                    else:
                        grade_list.append(grade_dict)
                        IsGrade = False
                else:
                    click_dict = RemoveLabel(str(price))
                    if (match in click_dict):
                        pass
                    else:
                        click_list.append(click_dict)
                        IsGrade = True



def excel():
    global  bname_list,author_list,grade_list,click_list
    workbook=xlwt.Workbook(encoding='utf-8')
    worksheet=workbook.add_sheet('book',cell_overwrite_ok=True)
    for i in range(0,4):
        for j in range(0,len(bname_list)+1):
            if(i==0):
                if(j==0):
                    worksheet.write(j, i, '书名')
                else:
                    worksheet.write(j, i,bname_list[j-1])
            elif(i==1):
                if (j == 0):
                    worksheet.write(j, i, '作者')
                else:
                    worksheet.write(j, i, author_list[j - 1])
            elif(i==2):
                if (j == 0):
                    worksheet.write(j, i, '评分')
                else:
                    worksheet.write(j, i, grade_list[j - 1])
            else:
                if (j == 0):
                    worksheet.write(j, i, '点击量')
                else:
                    worksheet.write(j, i, click_list[j - 1])
    workbook.save('book.xls')

crawler("index.html",1)
crawler("index_2.html",2)
excel()

