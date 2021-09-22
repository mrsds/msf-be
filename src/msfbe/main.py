"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import json
import logging
import sys, os
import traceback
import tornado.web
from tornado.options import define, options, parse_command_line
import ConfigParser
import pkg_resources
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
import msfbe.webmodel as webmodel
from msfbe.webmodel import RequestObject, ProcessingException
import importlib
import signal
import time
from functools import partial
import psutil

class ContentTypes(object):
    CSV = "CSV"
    JSON = "JSON"
    XML = "XML"
    PNG = "PNG"
    NETCDF = "NETCDF"
    ZIP = "ZIP"

class BaseRequestHandler(tornado.web.RequestHandler):
    path = r"/"

    def initialize(self):
        self.logger = logging.getLogger('nexus')

    def get(self):
        self.run()

    def run(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        reqObject = RequestObject(self)
        try:
            result = self.do_get(reqObject)
            self.async_callback(result)
        except ProcessingException as e:
            self.async_onerror_callback(e.reason, e.code)
        except Exception as e:
            self.async_onerror_callback(str(e), 500)

    def async_onerror_callback(self, reason, code=500):
        self.logger.error("Error processing request", exc_info=True)

        self.set_header("Content-Type", "application/json")
        self.set_status(code)

        response = {
            "error": reason,
            "code": code
        }

        self.write(json.dumps(response, indent=5))
        self.finish()

    def async_callback(self, result):
        self.finish()

    ''' Override me for standard handlers! '''
    def do_get(self, reqObject):
        pass



class ModularHandlerWrapper(BaseRequestHandler):
    def initialize(self, clazz=None, webconfig=None):
        BaseRequestHandler.initialize(self)
        self.__clazz = clazz
        self.__webconfig = webconfig

    def do_get(self, request):
        instance = self.__clazz.instance()

        results = instance.handle(request, webconfig=self.__webconfig)

        try:
            self.set_status(results.status_code)
        except AttributeError:
            pass

        if request.get_content_type() == ContentTypes.JSON:
            self.set_header("Content-Type", "application/json")
            try:
                self.write(results.toJson())
            except AttributeError:
                traceback.print_exc(file=sys.stdout)
                self.write(json.dumps(results, indent=4))
        elif request.get_content_type() == ContentTypes.PNG:
            self.set_header("Content-Type", "image/png")
            try:
                self.write(results.toImage())
            except AttributeError:
                traceback.print_exc(file=sys.stdout)
                raise ProcessingException(reason="Unable to convert results to an Image.")
        elif request.get_content_type() == ContentTypes.CSV:
            self.set_header("Content-Type", "text/csv")
            self.set_header("Content-Disposition", "filename=\"%s\"" % request.get_argument('filename', "download.csv"))
            try:
                self.write(results.toCSV())
            except:
                traceback.print_exc(file=sys.stdout)
                raise ProcessingException(reason="Unable to convert results to CSV.")
        elif request.get_content_type() == ContentTypes.NETCDF:
            self.set_header("Content-Type", "application/x-netcdf")
            self.set_header("Content-Disposition", "filename=\"%s\"" % request.get_argument('filename', "download.nc"))
            try:
                self.write(results.toNetCDF())
            except:
                traceback.print_exc(file=sys.stdout)
                raise ProcessingException(reason="Unable to convert results to NetCDF.")
        elif request.get_content_type() == ContentTypes.ZIP:
            self.set_header("Content-Type", "application/zip")
            self.set_header("Content-Disposition", "filename=\"%s\"" % request.get_argument('filename', "download.zip"))
            try:
                self.write(results.toZip())
            except:
                traceback.print_exc(file=sys.stdout)
                raise ProcessingException(reason="Unable to convert results to Zip.")

        return results

    def async_callback(self, result):
        super(ModularHandlerWrapper, self).async_callback(result)
        if hasattr(result, 'cleanup'):
            result.cleanup()


class MsfStaticFileHandler(tornado.web.StaticFileHandler):

    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

"""
class SayHiHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hi")
"""

#https://gist.github.com/wonderbeyond/d38cd85243befe863cdde54b84505784
def sig_handler(server, sig, frame):

    logging.warning('Caught signal: %s', sig)

    logging.info('Stopping child processes')
    current_process = psutil.Process()
    children = current_process.children(recursive=True)
    logging.warning("Stopping %s subprocesses", len(children))
    for child in children:
        logging.warning("Stopping subprocess with PID %s", child.pid)
        os.kill(child.pid, signal.SIGTERM)

    sys.exit(0)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)

    log = logging.getLogger(__name__)

    webconfig = ConfigParser.RawConfigParser()
    webconfig.readfp(pkg_resources.resource_stream(__name__, "config.ini"), filename='config.ini')

    define("debug", default=False, help="run in debug mode")
    define("port", default=webconfig.get("global", "server.socket_port"), help="run on the given port", type=int)
    define("address", default=webconfig.get("global", "server.socket_host"), help="Bind to the given address")
    define("subprocesses", default=webconfig.get("global", "server.num_sub_processes"), help="Number of http server subprocesses", type=int)
    define("enable_gzip", default=webconfig.get("global", "server.enable_gzip") == "true", help="Enable gzip response compression", type=bool)

    define("pgendpoint", default=webconfig.get("database", "db.endpoint"), type=str)
    define("pgport", default=webconfig.get("database", "db.port"), type=int)
    define("pguser", default=webconfig.get("database", "db.username"), type=str)
    define("pgpassword", default=webconfig.get("database", "db.password"), type=str)

    define("s3bucket", default=webconfig.get("s3", "s3.bucket"), type=str)

    parse_command_line()

    webconfig.set("database", "db.endpoint", options.pgendpoint)
    #webconfig.set("database", "db.username", options.pguser)
    #webconfig.set("database", "db.password", options.pgpassword)
    webconfig.set("database", "db.username", os.getenv('PG_USER'))
    webconfig.set("database", "db.password", os.getenv('PG_PWD'))

    webconfig.set("database", "db.port", options.pgport)

    webconfig.set("s3", "s3.bucket", options.s3bucket)

    log.info("""
     ___    _____     ___
    /_ /|  /____/ \  /_ /|       Methane Source Finder
    | | | |  __ \ /| | | |
 ___| | | | |__) |/  | | |__     Jet Propulsion Laboratory
/___| | | |  ___/    | |/__ /|   Pasadena, CA, USA
|_____|/  |_|/       |_____|/

    """)


    log.info("Initializing on host address '%s'" % options.address)
    log.info("Initializing on port '%s'" % options.port)
    log.info("Starting web server in debug mode: %s" % options.debug)

    handlers = []

    moduleDirs = webconfig.get("modules", "module_dirs").split(",")
    for moduleDir in moduleDirs:
        log.info("Loading modules from %s" % moduleDir)
        importlib.import_module(moduleDir)

    staticDir = webconfig.get("static", "static_dir")
    staticEnabled = webconfig.get("static", "static_enabled") == "true"

    for clazzWrapper in webmodel.AVAILABLE_HANDLERS:
        handlers.append(
            (clazzWrapper.path(), ModularHandlerWrapper,
             dict(clazz=clazzWrapper, webconfig=webconfig)))

    if staticEnabled:
        handlers.append(
            (r'/(.*)', MsfStaticFileHandler, {'path': staticDir, "default_filename": "index.html"}))


    app = tornado.web.Application(
        handlers,
        default_host=options.address,
        debug=options.debug,
        compress_response=options.enable_gzip
    )

    server = HTTPServer(app)
    server.bind(options.port, address=options.address)

    signal.signal(signal.SIGTERM, partial(sig_handler, server))
    signal.signal(signal.SIGINT, partial(sig_handler, server))

    server.start(int(options.subprocesses))  # Forks multiple sub-processes


    IOLoop.current().start()

    logging.info("Exit...")