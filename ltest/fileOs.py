# -*- coding: UTF-8 -*-

import os
import shutil
for root, dirs, files in os.walk("../log/", topdown=False):
    for name in files:
        dir_name = name[0:-4]
        os.mkdir("../logs/%s"%dir_name)
        src_path = os.path.join(root,name)
        copy_path = "../logs/%s/%s"%(dir_name,name)
        print copy_path + "----------------copy path"
        shutil.copy(src_path,copy_path)
