import os
import threading

from yaml import load

from aiohttp import web, ClientSession
import asyncio

class AsyncFS:

    def __init__(self):
        self.cfg = load(open('config.yaml'))        
        self.host = self.cfg['host']
        self.port = self.cfg['port']['a']
        self.dir = self.cfg['dir']['a']
        self.nodes = self.cfg['nodes_for_A']

    def run(self):
        run = web.Application()
        run.add_routes = [
            web.get('/{file}', self.find),
            web.get('/find_silently/{file}', self.find_silently)
        ]
        web.run_app(run, host=str(self.host), port=int(self.port))
        
    def async_find(self, file):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        tasks = asyncio.gather(*[self.ask_nodes(
            "http://{}:{}/find_silently/{}".format(node['host'], node['port'], file))
            for node in self._nodes.values()])
        responses = loop.run_until_complete(tasks)
        loop.close()
        self.find_response = None
        for response in responses:
            if response != 'False':
                thread = threading.Thread(target=self.save_file, args=(file, ))
                thread.start()
                thread.join()
                self.find_response = response
                
    async def find(self, request):
        file = request.match_info.get('file')
        if file in os.listdir(self.dir):
            thread = threading.Thread(target=self.read_file, args=(file, ))
            thread.start()
            thread.join()
            return web.Response(text=self.read_response)
        else:
            thread = threading.Thread(target=self.async_find, args=(file, ))
            thread.start()
            thread.join()
            if self.find_response:
                return web.Response(text=self.find_response)
            raise web.HTTPNotFound()

    async def find_silently(self, request):
        file = request.match_info.get('file')
        if file in os.listdir(self.dir):
            thread = threading.Thread(target=self.read_file, args=(file, ))
            thread.start()
            thread.join()
            return web.Response(text=self.read_response)
        else:
            return web.Response(text='False')

    async def ask_nodes(self, url):
        async with ClientSession() as session:
            return await self.connectto(session, url)

    async def connectto(self, session, url):
        async with session.get(url) as response:
            return await response.text()

    def read_file(self, file):
        with open(os.path.join(self.dir, file), "r") as f:
            self.read_response = f.read()

    def save_file(self, file):
        with open(os.path.join(self.dir, file), "w") as f:
            f.write(file)



a = AsyncFS()
a.run()
