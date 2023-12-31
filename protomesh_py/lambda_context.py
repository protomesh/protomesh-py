from grpc import ServicerContext, StatusCode
from typing import Dict, List, Callable
from aws_lambda_typing import context as context_, events, responses
from .lambda_conversion import convert_grpc_to_http_status_code

class GrpcContext(ServicerContext):

    def __init__(self, event: events.APIGatewayProxyEventV1, context: context_.Context):

        self.__client_metadata: Dict[str, List[str]] = {}
        self.__code: StatusCode = StatusCode.OK
        self.__details: str = None
        self.__service_metadata: Dict[str, List[str]] = {}
        self.__context: context_.Context = None
        self.__callback: List[Callable] = []
    
        self.__context = context

        if event["headers"] is not None:
            self.__client_metadata = event["headers"].copy()

        if event["multiValueHeaders"] is not None:
            for key, value in event["multiValueHeaders"].items():
                self.__client_metadata[key] = value

    def invocation_metadata(self) -> Dict[str, List[str]]:
        return self.__client_metadata
    
    def abort(self, code, details):
        self.__code = code
        self.__details = details
    
    def set_code(self, code):
        self.__code = code

    def set_details(self, details):
        self.__details = details

    def code(self):
        return self.__code
    
    def details(self):
        return self.__details
    
    def set_trailing_metadata(self, trailing_metadata):
        self.__service_metadata = trailing_metadata

    def trailing_metadata(self):
        return self.__service_metadata
    
    def attach_to_response(self, res: responses.APIGatewayProxyResponseV1):

        for callback in self.__callback:
            callback()

        res["statusCode"] = convert_grpc_to_http_status_code(self.__code).value

        if self.__details is not None:
            res["body"] = self.__details
            res["isBase64Encoded"] = False

        if len(self.__service_metadata) > 0:
            res["multiValueHeaders"] = {}

        for key, value in self.__service_metadata.items():

            if "multiValueHeaders" not in res:
                res["multiValueHeaders"] = {}

            res["multiValueHeaders"][key] = value

    def peer(self):
        raise NotImplementedError()

    def peer_identities(self):
        raise NotImplementedError()
    
    def peer_identity_key(self):
        raise NotImplementedError()
    
    def auth_context(self):
        raise NotImplementedError()
    
    def time_remaining(self):
        return self.__context.get_remaining_time_in_millis() / 1000.0
    
    def cancel(self):
        raise NotImplementedError()
    
    def add_callback(self, callback: callable):
        self.__callback.append(callback)
        
    def is_active(self):
        return True

    def set_compression(self, compression):
        raise NotImplementedError()
    
    def send_initial_metadata(self, initial_metadata):
        raise NotImplementedError()
    
    def abort_with_status(self, status):
        raise NotImplementedError()
    
    def disable_next_message_compression(self):
        raise NotImplementedError()
