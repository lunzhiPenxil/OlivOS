# -*- encoding: utf-8 -*-
r'''
_______________________    ________________
__  __ \__  /____  _/_ |  / /_  __ \_  ___/
_  / / /_  /  __  / __ | / /_  / / /____ \
/ /_/ /_  /____/ /  __ |/ / / /_/ /____/ /
\____/ /_____/___/  _____/  \____/ /____/

@File      :   main.py
@Author    :   lunzhiPenxil仑质
@Contact   :   lunzhipenxil@gmail.com
@License   :   AGPL
@Copyright :   (C) 2020-2026, OlivOS-Team
@Desc      :   None
'''

# here put the import lib

import os
import multiprocessing

import OlivOS

if __name__ == '__main__':
    if not os.path.exists('./conf'):
        os.makedirs('./conf')
    OlivOS.bootAPI.Entity(
        basic_conf_path='./conf/basic.json',
        basic_conf=OlivOS.bootDataAPI.default_Conf,
        patch_conf_path='./conf/config.json',
        patch_conf=OlivOS.bootDataAPI.native_patch_Conf,
        extend_queue={
            x: multiprocessing.Queue()
            for x in OlivOS.bootDataAPI.extend_queue
        },
    ).start()
