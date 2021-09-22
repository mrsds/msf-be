"""
Copyright (c) 2018 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

from msfbe.webmodel import BaseHandler, service_handler
import boto3

@service_handler
class ImageProxyHandlerImpl(BaseHandler):
    name = "Image Proxy Service"
    path = "/image"
    description = ""
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    def handle(self, computeOptions, **args):
        image = computeOptions.get_argument("item", None)

        if args["webconfig"].get("s3", "s3.profile") != "default":
            boto3.setup_default_session(profile_name=args["webconfig"].get("s3", "s3.profile"))
        s3 = boto3.resource('s3')

        obj = s3.Object(bucket_name=args["webconfig"].get("s3", "s3.bucket"), key=image)
        response = obj.get()
        data = response['Body'].read()

        class SimpleResults:
            def __init__(self, result):
                self.result = result

            def toImage(self):
                return self.result
        return SimpleResults(data)


