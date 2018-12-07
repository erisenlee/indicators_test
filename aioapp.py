from aiohttp import web
import aiohttp_jinja2
from jinja2 import FileSystemLoader
from datetime import datetime
from app.runner import Dataset

def query(query_dict):
    data = Dataset('dbdevelop',query_dict=query_dict)
    service_data = data.get_service_data()
    return service_data

routes = web.RouteTableDef()

@routes.view('/' ,name='index')
class IndexView(web.View):
    @aiohttp_jinja2.template('index.html')
    async def get(self):
        return {}
    @aiohttp_jinja2.template('index.html')
    async def post(self):
        form = await self.request.post()
        # data = form['city_id']
        query_dict = dict(form)
        result = query(query_dict)
        data = result._asdict()
        return data




# @routes.get('/',name='index')
# @routes.post('/')
# @aiohttp_jinja2.template('index.html')
# async def index(request):
#     if request.method == 'GET':
#         now = datetime.now()
#         return {'content':now}




app = web.Application()
aiohttp_jinja2.setup(app, loader=FileSystemLoader('app/templates'))
app.add_routes(routes)



web.run_app(app)


