# -*- encoding: utf-8 -*-
r'''
_______________________    ________________
__  __ \__  /____  _/_ |  / /_  __ \_  ___/
_  / / /_  /  __  / __ | / /_  / / /____ \
/ /_/ /_  /____/ /  __ |/ / / /_/ /____/ /
\____/ /_____/___/  _____/  \____/ /____/

@File      :   OlivOS/onebotV11LinkServerAPI.py
@Author    :   RemiliaCat
@Contact   :   RemiliaNero@gmail.com
@License   :   AGPL
@Copyright :   (C) 2020-2026, OlivOS-Team
@Desc      :   OneBot11 WebSocket-Forward Server Implementation
'''

import asyncio
import threading
import traceback
import websockets
from urllib.parse import urlparse, parse_qs
from dataclasses import dataclass

import OlivOS

modelName = 'onebotV11LinkServer'

gCheckList = [
    'default',
]


@dataclass
class ServerConf:
    url: str
    host: str
    port: int
    token: str
    route: str

    @classmethod
    def init_conf_from_post_info(cls, post_info: OlivOS.API.bot_info_T.post_info_T):
        """从post_info中提取配置信息"""
        # 考虑到OlivOS当前设计并没有区分URL与HOST+PORT的概念
        # 我们暂且废用了post_info的PORT字段，而通过HOST字段解析完整的URL
        tmp_host = post_info.host
        parsed = urlparse(tmp_host)
        scheme = parsed.scheme
        host = parsed.hostname
        port = parsed.port
        route = parsed.path
        url = f"{scheme}://{host}:{port}{route}"
        token = post_info.access_token
        if token is None:
            params = parse_qs(parsed.query)
            token = params.get('access_token', [None])[0]
        return cls(url=url, host=host, port=port, token=token, route=route)


@dataclass
class ExtraConf:
    queue_max_size: int = 512
    queue_timeout: int = 30
    retry_interval: int = 4

    @classmethod
    def init_extra_conf_from_extends(cls, extends: dict):
        return cls(
            queue_max_size=extends.get('queue_max_size', 512),
            queue_timeout=extends.get('queue_timeout', 30),
            retry_interval=extends.get('retry_interval', 4)
        )


class server(OlivOS.API.Proc_templet):
    """OneBot11 正向WebSocket 服务器"""

    def __init__(
        self,
        Proc_name,
        scan_interval=0.001,
        dead_interval=1,
        rx_queue=None,
        tx_queue=None,
        logger_proc=None,
        debug_mode=False,
        bot_info=None
    ):
        OlivOS.API.Proc_templet.__init__(
            self,
            Proc_name=Proc_name,
            Proc_type='onebotV11_link',
            scan_interval=scan_interval,
            dead_interval=dead_interval,
            rx_queue=rx_queue,
            tx_queue=tx_queue,
            logger_proc=logger_proc
        )
        self.bot_info: OlivOS.API.bot_info_T = bot_info
        self.ws_conn: websockets.ClientConnection = None
        self.async_rx_queue: asyncio.Queue = None
        self.conf: ServerConf = ServerConf.init_conf_from_post_info(self.bot_info.post_info)
        self.extra_conf: ExtraConf = ExtraConf.init_extra_conf_from_extends(self.bot_info.extends)

    def start(self) -> threading.Thread:
        proc_this = threading.Thread(
            target=lambda: asyncio.run(self.run()),
            name=self.Proc_name
        )
        proc_this.daemon = self.deamon
        proc_this.start()
        # self.Proc = proc_this
        return proc_this

    def start_unity(self, mode='threading') -> threading.Thread:
        proc_this = self.start()
        return proc_this

    async def run(self):
        self.async_rx_queue = asyncio.Queue(maxsize=self.extra_conf.queue_max_size)
        loop = asyncio.get_event_loop()
        bridge_thread = threading.Thread(
            target=self.__bridge_queue,
            name=f'{self.Proc_name}_bridge',
            args=(loop,),
            daemon=True
        )
        bridge_thread.start()

        while True:
            done = []
            pending = []
            try:
                await self.__link_to_server()
                if self.ws_conn is None:
                    await asyncio.sleep(self.extra_conf.retry_interval)
                    continue
                self.on_open()
                rx_task = asyncio.create_task(self.rx_link())
                tx_task = asyncio.create_task(self.tx_link())
                done, pending = await asyncio.wait(
                    [rx_task, tx_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
            except Exception as e:
                self.on_error(e)
            finally:
                for task in pending:
                    task.cancel()
                if self.ws_conn is not None:
                    await self.ws_conn.close()
                self.ws_conn = None

                self.on_close()
                await asyncio.sleep(self.extra_conf.retry_interval)

    async def rx_link(self):
        while True:
            try:
                raw = await self.ws_conn.recv()
                extra_info = {
                    'id': self.bot_info.id,
                    'token': self.conf.token,
                    'type': 'websocket'
                }
                sdk_event = OlivOS.onebotSDK.event(raw, extra_info)
                if not sdk_event.active:
                    continue
                tx_packet_data = OlivOS.pluginAPI.shallow.rx_packet(sdk_event)
                self.Proc_info.tx_queue.put(tx_packet_data)
            except (websockets.ConnectionClosed, asyncio.CancelledError):
                break
            except Exception as e:
                self.on_error(e)

    async def tx_link(self):
        while True:
            rx_packet_data: OlivOS.API.Control.packet = await self.async_rx_queue.get()
            try:
                data_part = rx_packet_data.key.get('data', {})
                if data_part.get('action') == 'send':
                    payload = data_part.get('data')
                    if payload is not None:
                        await self.ws_conn.send(payload)
            except (websockets.ConnectionClosed, asyncio.CancelledError):
                break
            except Exception as e:
                self.on_error(e)
            finally:
                self.async_rx_queue.task_done()

    async def __link_to_server(self):
        url = self.conf.url
        headers = {
            'Authorization': f'Bearer {self.conf.token}',
            'Content-Type': 'application/json'
        }
        try:
            connection = websockets.connect(url, additional_headers=headers)
            self.ws_conn = await connection.__aenter__()
        except TypeError:
            # 你猜为什么呢？
            connection = websockets.connect(url, extra_headers=headers)
            self.ws_conn = await connection.__aenter__()
        except ConnectionRefusedError:
            pass

    def __bridge_queue(self, loop: asyncio.AbstractEventLoop):
        while True:
            try:
                rx_packet_data = self.Proc_info.rx_queue.get()
                asyncio.run_coroutine_threadsafe(self.async_rx_queue.put(rx_packet_data), loop)
            except EOFError:
                break
            except Exception as e:
                self.on_error(e)

    def on_open(self) -> None:
        """连接建立时的处理"""
        self.log(
            2,
            OlivOS.L10NAPI.getTrans(
                'OlivOS onebotV11 link server [{0}] websocket link start',
                [self.Proc_name],
                modelName
            )
        )

    def on_close(self) -> None:
        """连接关闭时的处理"""
        self.log(
            0,
            OlivOS.L10NAPI.getTrans(
                'OlivOS onebotV11 link server [{0}] websocket link close',
                [self.Proc_name],
                modelName
            )
        )

    def on_lost(self) -> None:
        """连接丢失时的处理"""
        self.log(
            0,
            OlivOS.L10NAPI.getTrans(
                'OlivOS onebotV11 link server [{0}] websocket link lost',
                [self.Proc_name],
                modelName
            )
        )

    def on_error(self, error: Exception) -> None:
        """发生错误时的处理"""
        self.log(
            4,
            OlivOS.L10NAPI.getTrans(
                'OlivOS onebotV11 link server [{0}] websocket link error: \n{1}',
                [self.Proc_name, traceback.format_exc()],
                modelName
            )
        )
