
import os
import rarfile
from unrar import rarfile


def un_rar(file_name,path):
    file = rarfile.RarFile(file_name)  # 这里写入的是需要解压的文件，别忘了加路径
    file.extractall(path)  # 这里写入的是你想要解压到的文件夹
un_rar("C://Users//Administrator//Desktop//szt_201705081000.rar","C://Users//Administrator//Desktop//123")