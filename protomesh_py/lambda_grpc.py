from aws_lambda_typing import context as context_, events, responses

from typing import Dict
from enum import Enum

from lambda_context import GrpcContext

from google.protobuf.descriptor import ServiceDescriptor, MethodDescriptor
from google.protobuf.message_factory import MessageFactory

import base64

class MethodType(Enum):
    UNARY_UNARY = 1
    UNARY_STREAM = 2
    STREAM_UNARY = 3
    STREAM_STREAM = 4

class AbstractGrpcService:

    def get_service_descriptor(self) -> ServiceDescriptor:
        raise NotImplementedError()

class GrpcMethod:

    __method_desc: MethodDescriptor = None
    __service: AbstractGrpcService = None

    __request_factory = None

    def __init__(self, method_desc: MethodDescriptor, service: AbstractGrpcService):
        
        self.__method_desc = method_desc
        self.__service = service

        message_factory = MessageFactory()

        self.__request_factory = message_factory.GetPrototype(method_desc.input_type)


    def get_method_type(self) -> MethodType:
        if self.__method_desc.client_streaming and self.__method_desc.server_streaming:
            return MethodType.STREAM_STREAM
        elif self.__method_desc.client_streaming:
            return MethodType.STREAM_UNARY
        elif self.__method_desc.server_streaming:
            return MethodType.UNARY_STREAM
        else:
            return MethodType.UNARY_UNARY
    
    def __call__(self, event: events.APIGatewayProxyEventV1, context: context_.Context) -> responses.APIGatewayProxyResponseV1:

        ctx = GrpcContext(event, context)

        method_type = self.get_method_type()

        req_body = event["body"]

        if event["isBase64Encoded"]:
            req_body = base64.b64decode(req_body)

        if method_type == MethodType.UNARY_UNARY or method_type == MethodType.UNARY_STREAM:

            req = self.__request_factory()

            req.ParseFromString(req_body)

            res = responses.APIGatewayProxyResponseV1()

            res["isBase64Encoded"] = False

            try:

                call_res = getattr(self.__service, self.__method_desc.name)(req, ctx)
            
                res["isBase64Encoded"] = True
                res["body"] = base64.b64encode(call_res.SerializeToString())
            
            except Exception as e:
                
                res["body"] = str(e)

            finally:
                ctx.attach_to_response(res)


            return res
   
        raise Exception("Method type not supported")


class GrpcService:

    __methods: Dict[str, GrpcMethod] = {}

    def __init__(self, service: AbstractGrpcService):

        service_desc = service.get_service_descriptor()

        for method_name, method_desc in service_desc.methods_by_name.items():
            self.__methods[method_name] = GrpcMethod(method_desc, service)

    def __getitem__(self, method_name: str) -> GrpcMethod:

        if method_name not in self.methods:
            raise Exception("Method not found")
        
        return self.__methods[method_name]


class GrpcRouter:

    __services: Dict[str, GrpcService] = {}

    def register_service(self, service: AbstractGrpcService) -> GrpcService:

        service_desc = service.get_service_descriptor()

        if service_desc.full_name in self.__services:
            raise Exception("Service already registered")
        
        grpc_service = GrpcService(service)

        self.__services[service_desc.full_name] = grpc_service

        return grpc_service
    
    def __getitem__(self, event: events.APIGatewayProxyEventV1) -> GrpcMethod:
            
        service_name, method_name = event["path"].strip("/").split("/")

        if service_name not in self.__services:
            raise Exception("Service not found")

        return self.__services[service_name][method_name]

       
class GrpcHandler:
    
    __grpc_router: GrpcRouter = None

    def __init__(self, grpc_router: GrpcRouter) -> None:
        self.__grpc_router = grpc_router

    def __call__(self, event: events.APIGatewayProxyEventV1, context: context_.Context) -> responses.APIGatewayProxyResponseV1:
        
        method = self.__grpc_router[event]

        return method(method, context)


# class LinkService(AbstractGrpcService):

#     def get_service_descriptor(self) -> ServiceDescriptor:
#         return LINK_DESCRIPTOR.services_by_name["LinkService"]


# pp = pprint.PrettyPrinter(indent=4)

# router = GrpcRouter()

# router.register_service(LinkService())

# ev = events.APIGatewayProxyEventV1(path="/pomwm.api.v2.datalink.LinkService/ListByIdentity", body="{}")

# y = router[ev]

# x = LINK_DESCRIPTOR.services_by_name["LinkService"].methods_by_name["Delete"]
# # pp.pprint(router.services)
# pp.pprint(y)